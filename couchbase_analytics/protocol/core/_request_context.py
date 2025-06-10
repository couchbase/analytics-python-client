from __future__ import annotations

import math
import time

from concurrent.futures import (CancelledError,
                                Future,
                                ThreadPoolExecutor)
from threading import (Event,
                       Lock,
                       get_ident)
from typing import (Any,
                    Callable,
                    Dict,
                    Iterator,
                    List,
                    Optional,
                    TYPE_CHECKING)
from uuid import uuid4

from httpx import Response as HttpCoreResponse

from couchbase_analytics.common.core import ParsedResult
from couchbase_analytics.common.core.net_utils import get_request_ip
from couchbase_analytics.common.deserializer import Deserializer
from couchbase_analytics.common.errors import AnalyticsError, InvalidCredentialError
from couchbase_analytics.common.result import BlockingQueryResult
from couchbase_analytics.common.streaming import StreamingState
from couchbase_analytics.protocol.connection import DEFAULT_TIMEOUTS
from couchbase_analytics.protocol.errors import ErrorMapper

if TYPE_CHECKING:
    from couchbase_analytics.protocol.core.client_adapter import _ClientAdapter
    from couchbase_analytics.protocol.core.request import QueryRequest

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
                 tp_executor: ThreadPoolExecutor) -> None:
        self._id = str(uuid4())
        self._client_adapter = client_adapter
        self._request = request
        self._request_state = StreamingState.NotStarted
        self._cancel_event = Event()
        self._request_error: Optional[Exception] = None
        self._tp_executor = tp_executor
        self._stage_completed_ft: Optional[Future] = None
        self._stage_notification_ft: Optional[Future[ParsedResult]] = None
        self._request_deadline = math.inf
        self._background_request: Optional[BackgroundRequest] = None

    # @property
    # def stage_notification(self) -> Future[ParsedResult]:
    #     if self._stage_notification_ft is None:
    #         raise RuntimeError('Background future not created for this context.')
    #     return self._stage_notification_ft

    @property
    def cancel_enabled(self) -> bool:
        return self._request.enable_cancel

    @property
    def cancel_event(self) -> Event:
        return self._request._cancel_event

    @property
    def deserializer(self) -> Deserializer:
        """
        Returns the deserializer used by this request context.
        """
        return self._request.deserializer

    @property
    def has_stage_completed(self) -> bool:
        return self._stage_completed_ft is not None and self._stage_completed_ft.done()

    @property
    def okay_to_iterate(self) -> bool:
        # Called prior to upstream logic attempting to iterate over results from HTTP client
        self._check_cancelled_or_timed_out()
        return StreamingState.okay_to_iterate(self._request_state)
    
    @property
    def okay_to_stream(self) -> bool:
        # Called prior to upstream logic attempting to send request to HTTP client
        self._check_cancelled_or_timed_out()
        return StreamingState.okay_to_stream(self._request_state)

    @property
    def request_error(self) -> Optional[Exception]:
        return self._request_error
    
    # @property
    # def request_future(self) -> Future[BlockingQueryResult]:
    #     if self._request_future is None:
    #         raise RuntimeError('Request future not created for this context.')
    #     return self._request_future

    @property
    def request_state(self) -> StreamingState:
        return self._request_state
    
    @request_state.setter
    def request_state(self, state: StreamingState) -> None:
        if not isinstance(state, StreamingState):
            raise TypeError('request_state must be an instance of StreamingState')
        self._request_state = state
    
    @property
    def timed_out(self) -> bool:
        self._check_cancelled_or_timed_out()
        return self._request_state == StreamingState.Timeout

    @property
    def cancelled(self) -> bool:
        self._check_cancelled_or_timed_out()
        return self._request_state in [StreamingState.Cancelled, StreamingState.SyncCancelledPriorToTimeout]

    def _check_cancelled_or_timed_out(self) -> None:
        if self._request_state in [StreamingState.Timeout, StreamingState.Cancelled, StreamingState.Error]:
            return
        
        if (self._cancel_event.is_set()
            or (self._background_request is not None
                and self._background_request.user_cancelled)):
            self._request_state = StreamingState.Cancelled

        timed_out = self._request_deadline < time.monotonic()
        if timed_out:
            if self._request_state == StreamingState.Cancelled:
                self._request_state = StreamingState.SyncCancelledPriorToTimeout
            else:
                self._request_state = StreamingState.Timeout

    def _create_stage_notification_future(self) -> None:
        # TODO:  custom ThreadPoolExecutor, to get a "plain" future
        if self._stage_notification_ft is not None:
            raise RuntimeError('Stage notification future already created for this context.')
        self._stage_notification_ft = Future[ParsedResult]()

    def _trace_handler(self, event_name, _) -> None:
        if event_name == 'connection.connect_tcp.complete':
            print('Connection established, updating cancel scope deadline')

    def initialize(self) -> None:
        self._request_state = StreamingState.Started
        timeouts = self._request.get_request_timeouts()
        self._request_deadline = time.monotonic() + timeouts.get('read', DEFAULT_TIMEOUTS['query_timeout'])

    def process_error(self, json_data: List[Dict[str, Any]]) -> None:
        self._request_state = StreamingState.Error
        if not isinstance(json_data, list):
            self._request_error = AnalyticsError('Cannot parse error response; expected JSON array')

        self._request_error = ErrorMapper.build_error_from_json(json_data, status_code=self._request.response_status_code)
        raise self._request_error

    def send_request(self, enable_trace_handling: Optional[bool]=False) -> HttpCoreResponse:
        ip = get_request_ip(self._request.host, self._request.port, self._request.previous_ips)
        if ip is None:
            attempted_ips = ', '.join(self._request.previous_ips or [])
            raise AnalyticsError(f'Connect failure.  Attempted to connect to resolved IPs: {attempted_ips}.')
        
        if enable_trace_handling is True:
            (self._request.update_url(ip, self._client_adapter.analytics_path)
                          .update_extensions({'trace': self._trace_handler})
                          .update_previous_ips(ip))
        else:
            self._request.update_url(ip, self._client_adapter.analytics_path).update_previous_ips(ip)
        response = self._client_adapter.send_request(self._request)
        self._request.set_client_server_addrs(response)
        if response.status_code == 401:
            context = {
                'client_addr': self._request.client_addr,
                'server_addr': self._request.server_addr,
                'http_status': response.status_code,
            }
            raise InvalidCredentialError(context)
        
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

    def start_next_stage(self,
                         fn: Callable[..., Any],
                         *args: object,
                         create_notification: Optional[bool]=False,
                         reset_previous_stage: Optional[bool]=False) -> None:
        if reset_previous_stage is True:
            if self._stage_completed_ft is not None:
                self._stage_completed_ft = None
        elif self._stage_completed_ft is not None and not self._stage_completed_ft.done():
            raise RuntimeError('Future already running in this context.')
        
        kwargs = {'request_context': self}
        if create_notification is True:
            self._create_stage_notification_future()
            kwargs['notify_on_results_or_error'] = self._stage_notification_ft

        self._stage_completed_ft = self._tp_executor.submit(fn, *args, **kwargs)
    
    def wait_for_stage_notification(self) -> ParsedResult:
        # TODO: what if the deadline is already passed?
        deadline = round(self._request_deadline - time.monotonic(), 6) # round to microseconds
        res = self._stage_notification_ft.result(timeout=deadline)
        return res