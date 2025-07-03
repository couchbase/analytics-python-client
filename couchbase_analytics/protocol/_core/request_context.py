from __future__ import annotations

import json
import math
import time
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from threading import Event, Lock
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Union
from uuid import uuid4

from httpx import Response as HttpCoreResponse

from couchbase_analytics.common._core import JsonStreamConfig, ParsedResult, ParsedResultType
from couchbase_analytics.common._core.error_context import ErrorContext
from couchbase_analytics.common.errors import AnalyticsError, InvalidCredentialError, TimeoutError
from couchbase_analytics.common.result import BlockingQueryResult
from couchbase_analytics.common.streaming import StreamingState
from couchbase_analytics.protocol._core.json_stream import JsonStream
from couchbase_analytics.protocol._core.net_utils import get_request_ip
from couchbase_analytics.protocol.connection import DEFAULT_TIMEOUTS
from couchbase_analytics.protocol.errors import ErrorMapper

if TYPE_CHECKING:
    from couchbase_analytics.protocol._core.client_adapter import _ClientAdapter
    from couchbase_analytics.protocol._core.request import QueryRequest

# TODO: might not be needed; need to validate httpx iterator behavior
class ThreadSafeBytesIterator:
    def __init__(self, iterator: Iterator[bytes]):
        if not hasattr(iterator, '__next__'):
            raise TypeError("Provided object is not an iterator (missing __next__ method).")
        self._iterator = iterator
        self._lock = Lock()

    def __iter__(self) -> ThreadSafeBytesIterator:
        return self

    def __next__(self) -> bytes:
        with self._lock: # Acquire the lock before accessing the iterator
            try:
                item = next(self._iterator)
                return item
            except StopIteration:
                # Always re-raise StopIteration to signal the end of iteration
                raise

class BackgroundRequest:
    def __init__(self, bg_future: Future[BlockingQueryResult],
                 user_future: Future[BlockingQueryResult],
                 cancel_event: Event) -> None:
        self._background_work_ft = bg_future
        self._user_ft = user_future
        self._cancel_event = cancel_event
        self._background_work_ft.add_done_callback(self._background_work_done)
        self._user_ft.add_done_callback(self._user_done)

    @property
    def user_cancelled(self) -> bool:
        return self._user_ft.cancelled()

    def _background_work_done(self, ft: Future[BlockingQueryResult]) -> None:
        """
        Callback to handle when the background work future is done.
        """
        if self._user_ft.done():
            return
        if self._cancel_event.is_set():
            self._user_ft.cancel()
            return
        try:
            result = ft.result()
            self._user_ft.set_result(result)
        except Exception as ex:
            self._user_ft.set_exception(ex)

    def _user_done(self, ft: Future[BlockingQueryResult]) -> None:
        """
        Callback to handle when the user future is done.
        """
        if self._background_work_ft.done():
            # If the background work future is already done, we don't need to do anything
            return
        if ft.cancelled():
            self._cancel_event.set()
            self._background_work_ft.cancel()
            return



class RequestContext:

    def __init__(self,
                 client_adapter: _ClientAdapter,
                 request: QueryRequest,
                 tp_executor: ThreadPoolExecutor,
                 stream_config: Optional[JsonStreamConfig]=None) -> None:
        self._id = str(uuid4())
        self._client_adapter = client_adapter
        self._request = request
        self._error_ctx = ErrorContext(num_attempts=0,
                                       method=request.method,
                                       statement=request.get_request_statement())
        self._request_state = StreamingState.NotStarted
        self._stream_config = stream_config or JsonStreamConfig()
        self._json_stream: JsonStream
        self._cancel_event = Event()
        self._request_error: Optional[Exception] = None
        self._tp_executor = tp_executor
        self._stage_completed_ft: Optional[Future[Any]] = None
        self._stage_notification_ft: Optional[Future[ParsedResultType]] = None
        self._request_deadline = math.inf
        self._background_request: Optional[BackgroundRequest] = None
        self._shutdown = False

    @property
    def cancel_enabled(self) -> Optional[bool]:
        return self._request.enable_cancel

    @property
    def cancelled(self) -> bool:
        self._check_cancelled_or_timed_out()
        return self._request_state in [StreamingState.Cancelled, StreamingState.SyncCancelledPriorToTimeout]

    @property
    def error_context(self) -> ErrorContext:
        return self._error_ctx

    @property
    def has_stage_completed(self) -> bool:
        return self._stage_completed_ft is not None and self._stage_completed_ft.done()

    @property
    def is_shutdown(self) -> bool:
        return self._shutdown

    @property
    def okay_to_iterate(self) -> bool:
        # NOTE: Called prior to upstream logic attempting to iterate over results from HTTP client
        self._check_cancelled_or_timed_out()
        return StreamingState.okay_to_iterate(self._request_state)

    @property
    def okay_to_stream(self) -> bool:
        # NOTE: Called prior to upstream logic attempting to send request to HTTP client
        self._check_cancelled_or_timed_out()
        return StreamingState.okay_to_stream(self._request_state)

    @property
    def request_error(self) -> Optional[Exception]:
        return self._request_error

    @property
    def request_state(self) -> StreamingState:
        return self._request_state

    @property
    def timed_out(self) -> bool:
        self._check_cancelled_or_timed_out()
        return self._request_state == StreamingState.Timeout

    def _check_cancelled_or_timed_out(self) -> None:
        if self._request_state in [StreamingState.Timeout, StreamingState.Cancelled, StreamingState.Error]:
            return

        if (self._cancel_event.is_set()
            or (self._background_request is not None
                and self._background_request.user_cancelled)):
            self._request_state = StreamingState.Cancelled

        current_time = time.monotonic()
        timed_out = current_time >= self._request_deadline
        # print(f'{current_time=}; req_deadline={self._request_deadline}; {timed_out=}')
        if timed_out:
            if self._request_state == StreamingState.Cancelled:
                self._request_state = StreamingState.SyncCancelledPriorToTimeout
            else:
                self._request_state = StreamingState.Timeout

    def _create_stage_notification_future(self) -> None:
        # TODO:  custom ThreadPoolExecutor, to get a "plain" future
        if self._stage_notification_ft is not None:
            raise RuntimeError('Stage notification future already created for this context.')
        self._stage_notification_ft = Future[ParsedResultType]()

    def _process_error(self,
                       json_data: List[Dict[str, Any]],
                       handle_context_shutdown: Optional[bool]=False) -> None:
        self._request_state = StreamingState.Error
        if not isinstance(json_data, list):
            self._request_error = AnalyticsError(message='Cannot parse error response; expected JSON array',
                                                 context=str(self._request_context.error_context))
        else:
            self._request_error = ErrorMapper.build_error_from_json(json_data, self._error_ctx)
        if handle_context_shutdown is True:
            self.shutdown()
        raise self._request_error

    def _reset_stream(self) -> None:
        if hasattr(self, '_json_stream'):
            del self._json_stream
        self._request_state = StreamingState.ResetAndNotStarted
        self._request.previous_ips = set()
        self._stage_notification_ft = None

    def _start_next_stage(self,
                         fn: Callable[..., Any],
                         *args: object,
                         create_notification: Optional[bool]=False,
                         reset_previous_stage: Optional[bool]=False) -> None:
        if reset_previous_stage is True:
            if self._stage_completed_ft is not None:
                self._stage_completed_ft = None
        elif self._stage_completed_ft is not None and not self._stage_completed_ft.done():
            raise RuntimeError('Future already running in this context.')

        kwargs: Dict[str, Union[RequestContext, Future[ParsedResultType]]] = {'request_context': self}
        if create_notification is True:
            self._create_stage_notification_future()
            if self._stage_notification_ft is None:
                raise RuntimeError('Unable to create stage notification future.')
            kwargs['notify_on_results_or_error'] = self._stage_notification_ft

        self._stage_completed_ft = self._tp_executor.submit(fn, *args, **kwargs)

    def _trace_handler(self, event_name: str, _: str) -> None:
        if event_name == 'connection.connect_tcp.complete':
            pass

    def _wait_for_stage_completed(self) -> None:
        if self._stage_completed_ft is None:
            raise RuntimeError('Stage completed future not created for this context.')
        self._stage_completed_ft.result()

    def cancel_request(self) -> None:
        if self._request_state == StreamingState.Timeout:
            return
        self._request_state = StreamingState.Cancelled

    def deserialize_result(self, result: bytes) -> Any:
        return self._request.deserializer.deserialize(result)

    def finish_processing_stream(self) -> None:
        if not self.has_stage_completed:
            self._wait_for_stage_completed()

        if self.cancelled:
            return

        while not self._json_stream.token_stream_exhausted:
            self._json_stream.continue_parsing()

    def get_result_from_stream(self) -> Optional[ParsedResult]:
        return self._json_stream.get_result(self._stream_config.queue_timeout)

    def initialize(self) -> None:
        # TODO: Add useful logging messages
        if self._request_state == StreamingState.ResetAndNotStarted:
            # print('Skipping initialization as request is a retry')
            return
        self._request_state = StreamingState.Started
        timeouts = self._request.get_request_timeouts() or {}
        current_time = time.monotonic()
        self._request_deadline = current_time + (timeouts.get('read', None) or DEFAULT_TIMEOUTS['query_timeout'])
        # print(f'initialize request ctx: {current_time=}; req_deadline={self._request_deadline}')

    def maybe_continue_to_process_stream(self) -> None:
        if not self.has_stage_completed:
            return

        if self._json_stream.token_stream_exhausted:
            return

        if self.cancelled:
            return

        # NOTE:  _start_next_stage injects the request context into args
        self._start_next_stage(self._json_stream.continue_parsing, reset_previous_stage=True)

    def okay_to_delay_and_retry(self, delay: float) -> bool:
        self._check_cancelled_or_timed_out()
        if self._request_state in [StreamingState.Timeout, StreamingState.Cancelled]:
            return False

        current_time = time.monotonic()
        delay_time = current_time + delay
        will_time_out = self._request_deadline < delay_time
        # print(f'{current_time=}; {delay_time=}; req_deadline={self._request_deadline}; {will_time_out=}')
        if will_time_out:
            self._request_state = StreamingState.Timeout
            return False
        else:
            self._reset_stream()
            return True

    def process_response(self,
                         close_handler: Callable[[], None],
                         raw_response: Optional[ParsedResult]=None,
                         handle_context_shutdown: Optional[bool]=False) -> Any:
        if raw_response is None:
            raw_response = self._json_stream.get_result(self._stream_config.queue_timeout)
            if raw_response is None:
                close_handler()
                raise AnalyticsError(message='Received unexpected empty result from JsonStream.',
                                     context=str(self._error_ctx))

        if raw_response.value is None:
            close_handler()
            raise AnalyticsError(message='Received unexpected empty response value from JsonStream.',
                                 context=str(self._error_ctx))

        # we have all the data, close the core response/stream
        close_handler()
        json_response = json.loads(raw_response.value)
        if 'errors' in json_response:
            self._process_error(json_response['errors'], handle_context_shutdown=handle_context_shutdown)
        return json_response

    def send_request(self, enable_trace_handling: Optional[bool]=False) -> HttpCoreResponse:
        ip = get_request_ip(self._request.url.host, self._request.url.port, self._request.previous_ips)
        if ip is None:
            attempted_ips = ', '.join(self._request.previous_ips or [])
            raise AnalyticsError(message=f'Connect failure.  Unable to connect to any resolved IPs: {attempted_ips}.',
                                 context=str(self._error_ctx))

        if enable_trace_handling is True:
            (self._request.update_url(ip, self._client_adapter.analytics_path)
                          .add_trace_to_extensions(self._trace_handler)
                          .update_previous_ips(ip))
        else:
            self._request.update_url(ip, self._client_adapter.analytics_path).update_previous_ips(ip)
        self._error_ctx.update_request_context(self._request)
        response = self._client_adapter.send_request(self._request)
        self._error_ctx.update_response_context(response)
        # print(f'Response received: {response.status_code} for request {self._id}, body={self._request.body}.')
        if response.status_code == 401:
            raise InvalidCredentialError(context=str(self._error_ctx))

        return response

    def send_request_in_background(self,
                                   fn: Callable[..., BlockingQueryResult],
                                   *args: object,) -> Future[BlockingQueryResult]:

        if self._background_request is not None:
            raise RuntimeError('Background reqeust already created for this context.')
        # TODO:  custom ThreadPoolExecutor, to get a "plain" future
        user_ft = Future[BlockingQueryResult]()
        background_work_ft = self._tp_executor.submit(fn, *args)
        self._background_request = BackgroundRequest(background_work_ft, user_ft, self._cancel_event)
        return user_ft

    def set_state_to_streaming(self) -> None:
        self._request_state = StreamingState.StreamingResults

    def shutdown(self, exc_val: Optional[BaseException]=None) -> None:
        if self.is_shutdown:
            return
        if isinstance(exc_val, CancelledError):
            self._request_state = StreamingState.Cancelled
        elif exc_val is not None:
            self._check_cancelled_or_timed_out()
            if self._request_state not in [StreamingState.Timeout,
                                           StreamingState.Cancelled,
                                           StreamingState.SyncCancelledPriorToTimeout]:
                self._request_state = StreamingState.Error

        if StreamingState.is_okay(self._request_state):
            self._request_state = StreamingState.Completed
        self._shutdown = True

    def start_stream(self, core_response: HttpCoreResponse) -> None:
        if hasattr(self, '_json_stream'):
            # TODO: logging; I don't think this is an error...
            return

        # TODO: need to confirm if the httpx Response iterator is thread-safe
        self._json_stream = JsonStream(core_response.iter_bytes(), stream_config=self._stream_config)
        self._start_next_stage(self._json_stream.start_parsing, create_notification=True)

    def wait_for_stage_notification(self) -> None:
        if self._stage_notification_ft is None:
            raise RuntimeError('Stage notification future not created for this context.')
        deadline = round(self._request_deadline - time.monotonic(), 6) # round to microseconds
        if deadline <= 0:
            raise TimeoutError(message='Request timed out waiting for stage notification', context=str(self._error_ctx))
        result_type = self._stage_notification_ft.result(timeout=deadline)
        if result_type == ParsedResultType.ROW:
            # we move to iterating rows
            self._request_state = StreamingState.StreamingResults
