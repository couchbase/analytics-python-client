from __future__ import annotations

import json
import math
import time
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from threading import Event
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union
from uuid import uuid4

from httpx import Response as HttpCoreResponse

from couchbase_analytics.common._core import JsonStreamConfig, ParsedResult, ParsedResultType
from couchbase_analytics.common._core.error_context import ErrorContext
from couchbase_analytics.common.backoff_calculator import DefaultBackoffCalculator
from couchbase_analytics.common.errors import AnalyticsError, TimeoutError
from couchbase_analytics.common.logging import LogLevel
from couchbase_analytics.common.request import RequestState
from couchbase_analytics.common.result import BlockingQueryResult
from couchbase_analytics.protocol._core.json_stream import JsonStream
from couchbase_analytics.protocol._core.net_utils import get_request_ip
from couchbase_analytics.protocol.connection import DEFAULT_TIMEOUTS
from couchbase_analytics.protocol.errors import ErrorMapper, WrappedError

if TYPE_CHECKING:
    from couchbase_analytics.protocol._core.client_adapter import _ClientAdapter
    from couchbase_analytics.protocol._core.request import QueryRequest


class BackgroundRequest:
    def __init__(
        self, bg_future: Future[BlockingQueryResult], user_future: Future[BlockingQueryResult], cancel_event: Event
    ) -> None:
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
    def __init__(
        self,
        client_adapter: _ClientAdapter,
        request: QueryRequest,
        tp_executor: ThreadPoolExecutor,
        stream_config: Optional[JsonStreamConfig] = None,
    ) -> None:
        self._id = str(uuid4())
        self._client_adapter = client_adapter
        self._request = request
        self._backoff_calc = DefaultBackoffCalculator()
        self._error_ctx = ErrorContext(num_attempts=0, method=request.method, statement=request.get_request_statement())
        self._request_state = RequestState.NotStarted
        self._stream_config = stream_config or JsonStreamConfig()
        self._json_stream: JsonStream
        self._cancel_event = Event()
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
        return self._request_state in [RequestState.Cancelled, RequestState.SyncCancelledPriorToTimeout]

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
        return RequestState.okay_to_iterate(self._request_state)

    @property
    def okay_to_stream(self) -> bool:
        # NOTE: Called prior to upstream logic attempting to send request to HTTP client
        self._check_cancelled_or_timed_out()
        return RequestState.okay_to_stream(self._request_state)

    @property
    def request_state(self) -> RequestState:
        return self._request_state

    @property
    def retry_limit_exceeded(self) -> bool:
        return self.error_context.num_attempts > self._request.max_retries

    @property
    def timed_out(self) -> bool:
        self._check_cancelled_or_timed_out()
        return self._request_state == RequestState.Timeout

    def _check_cancelled_or_timed_out(self) -> None:
        if self._request_state in [RequestState.Timeout, RequestState.Cancelled, RequestState.Error]:
            return

        if self._cancel_event.is_set() or (
            self._background_request is not None and self._background_request.user_cancelled
        ):
            self._request_state = RequestState.Cancelled
            if self._cancel_event.is_set():
                self.log_message('Request has been cancelled', LogLevel.DEBUG)
            elif self._background_request is not None and self._background_request.user_cancelled:
                self.log_message('Request has been cancelled via user background request', LogLevel.DEBUG)
            return

        current_time = time.monotonic()
        timed_out = current_time >= self._request_deadline
        if timed_out:
            message_data = {'current_time': f'{current_time}', 'request_deadline': f'{self._request_deadline}'}
            self.log_message('Request has timed out', LogLevel.DEBUG, message_data=message_data)
            if self._request_state == RequestState.Cancelled:
                self._request_state = RequestState.SyncCancelledPriorToTimeout
            else:
                self._request_state = RequestState.Timeout

    def _create_stage_notification_future(self) -> None:
        # TODO(PYCO-75):  custom ThreadPoolExecutor, to get a "plain" future
        if self._stage_notification_ft is not None:
            raise RuntimeError('Stage notification future already created for this context.')
        self._stage_notification_ft = Future[ParsedResultType]()

    def _process_error(
        self, json_data: Union[str, List[Dict[str, Any]]], handle_context_shutdown: Optional[bool] = False
    ) -> None:
        self._request_state = RequestState.Error
        request_error: Union[AnalyticsError, WrappedError]
        if isinstance(json_data, str):
            request_error = ErrorMapper.build_error_from_http_status_code(json_data, self._error_ctx)
        elif not isinstance(json_data, list):
            request_error = AnalyticsError(
                message='Cannot parse error response; expected JSON array', context=str(self._error_ctx)
            )
        else:
            request_error = ErrorMapper.build_error_from_json(json_data, self._error_ctx)
        if handle_context_shutdown is True:
            self.shutdown()
        raise request_error

    def _reset_stream(self) -> None:
        if hasattr(self, '_json_stream'):
            del self._json_stream
        self._request_state = RequestState.ResetAndNotStarted
        self._stage_notification_ft = None
        self.log_message('Request state has been reset', LogLevel.DEBUG)

    def _start_next_stage(
        self,
        fn: Callable[..., Any],
        *args: object,
        create_notification: Optional[bool] = False,
        reset_previous_stage: Optional[bool] = False,
    ) -> None:
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

    def calculate_backoff(self) -> float:
        return self._backoff_calc.calculate_backoff(self._error_ctx.num_attempts) / 1000

    def cancel_request(self) -> None:
        if self._request_state == RequestState.Timeout:
            return
        self._request_state = RequestState.Cancelled

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
        if self._request_state == RequestState.ResetAndNotStarted:
            self.log_message(
                'Request is a retry, skipping initialization',
                LogLevel.DEBUG,
                message_data={'request_deadline': f'{self._request_deadline}'},
            )
            return
        self._request_state = RequestState.Started
        timeouts = self._request.get_request_timeouts() or {}
        current_time = time.monotonic()
        self._request_deadline = current_time + (timeouts.get('read', None) or DEFAULT_TIMEOUTS['query_timeout'])
        message_data = {'current_time': f'{current_time}', 'request_deadline': f'{self._request_deadline}'}
        self.log_message('Request context initialized', LogLevel.DEBUG, message_data=message_data)

    def log_message(
        self,
        message: str,
        log_level: LogLevel,
        message_data: Optional[Dict[str, str]] = None,
        append_ctx: Optional[bool] = True,
    ) -> None:
        if append_ctx is True:
            message = f'{message}: ctx={self._id}'
        if message_data is not None:
            message_data_str = ', '.join(f'{k}={v}' for k, v in message_data.items())
            message = f'{message}, {message_data_str}'
        self._client_adapter.log_message(message, log_level)

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
        if self._request_state in [RequestState.Timeout, RequestState.Cancelled]:
            return False

        current_time = time.monotonic()
        delay_time = current_time + delay
        will_time_out = self._request_deadline < delay_time
        if will_time_out:
            self._request_state = RequestState.Timeout
            message_data = {
                'current_time': f'{current_time}',
                'delay_time': f'{delay_time}',
                'request_deadline': f'{self._request_deadline}',
            }
            self.log_message('Request will timeout after delay', LogLevel.DEBUG, message_data=message_data)
            return False
        elif self.retry_limit_exceeded:
            self._request_state = RequestState.Error
            message_data = {
                'num_attempts': f'{self.error_context.num_attempts}',
                'max_retries': f'{self._request.max_retries}',
            }
            self.log_message('Request has exceeded max retries', LogLevel.DEBUG, message_data=message_data)
            return False
        else:
            self._reset_stream()
            return True

    def process_response(
        self,
        close_handler: Callable[[], None],
        raw_response: Optional[ParsedResult] = None,
        handle_context_shutdown: Optional[bool] = False,
    ) -> Any:
        if raw_response is None:
            raw_response = self._json_stream.get_result(self._stream_config.queue_timeout)
            if raw_response is None:
                close_handler()
                raise AnalyticsError(
                    message='Received unexpected empty result from JsonStream.', context=str(self._error_ctx)
                )

        if raw_response.value is None:
            close_handler()
            raise AnalyticsError(
                message='Received unexpected empty response value from JsonStream.', context=str(self._error_ctx)
            )

        # we have all the data, close the core response/stream
        close_handler()
        try:
            json_response = json.loads(raw_response.value)
        except json.JSONDecodeError:
            self._process_error(str(raw_response.value), handle_context_shutdown=handle_context_shutdown)
        else:
            if 'errors' in json_response:
                self._process_error(json_response['errors'], handle_context_shutdown=handle_context_shutdown)
            return json_response

    def send_request(self, enable_trace_handling: Optional[bool] = False) -> HttpCoreResponse:
        self._error_ctx.update_num_attempts()
        ip = get_request_ip(self._request.url.host, self._request.url.port, self.log_message)
        if enable_trace_handling is True:
            (
                self._request.update_url(ip, self._client_adapter.analytics_path).add_trace_to_extensions(
                    self._trace_handler
                )
            )
        else:
            self._request.update_url(ip, self._client_adapter.analytics_path)
        self._error_ctx.update_request_context(self._request)
        message_data = {
            'url': f'{self._request.url.get_formatted_url()}',
            'body': f'{self._request.body}',
            'request_deadline': f'{self._request_deadline}',
        }
        self.log_message('HTTP request', LogLevel.DEBUG, message_data=message_data)
        response = self._client_adapter.send_request(self._request)
        self._error_ctx.update_response_context(response)
        message_data = {
            'status_code': f'{response.status_code}',
            'last_dispatched_to': f'{self._error_ctx.last_dispatched_to}',
            'last_dispatched_from': f'{self._error_ctx.last_dispatched_from}',
            'request_deadline': f'{self._request_deadline}',
        }
        self.log_message('HTTP response', LogLevel.DEBUG, message_data=message_data)
        return response

    def send_request_in_background(
        self,
        fn: Callable[..., BlockingQueryResult],
        *args: object,
    ) -> Future[BlockingQueryResult]:
        if self._background_request is not None:
            raise RuntimeError('Background reqeust already created for this context.')
        # TODO(PYCO-75):  custom ThreadPoolExecutor, to get a "plain" future
        user_ft = Future[BlockingQueryResult]()
        background_work_ft = self._tp_executor.submit(fn, *args)
        self._background_request = BackgroundRequest(background_work_ft, user_ft, self._cancel_event)
        return user_ft

    def set_state_to_streaming(self) -> None:
        self._request_state = RequestState.StreamingResults

    def shutdown(self, exc_val: Optional[BaseException] = None) -> None:
        if self.is_shutdown:
            self.log_message('Request context already shutdown', LogLevel.WARNING)
            return
        if isinstance(exc_val, CancelledError):
            self._request_state = RequestState.Cancelled
        elif exc_val is not None:
            self._check_cancelled_or_timed_out()
            if self._request_state not in [
                RequestState.Timeout,
                RequestState.Cancelled,
                RequestState.SyncCancelledPriorToTimeout,
            ]:
                self._request_state = RequestState.Error

        if RequestState.is_okay(self._request_state):
            self._request_state = RequestState.Completed
        self._shutdown = True
        self.log_message('Request context shutdown complete', LogLevel.INFO)

    def start_stream(self, core_response: HttpCoreResponse) -> None:
        if hasattr(self, '_json_stream'):
            self.log_message('JSON stream already exists', LogLevel.WARNING)
            return

        # TODO(PYCO-73): Potentially use new iterator if problems w/ httpx
        self._json_stream = JsonStream(
            core_response.iter_bytes(), stream_config=self._stream_config, logger_handler=self.log_message
        )
        self._start_next_stage(self._json_stream.start_parsing, create_notification=True)

    def wait_for_stage_notification(self) -> None:
        if self._stage_notification_ft is None:
            raise RuntimeError('Stage notification future not created for this context.')
        deadline = round(self._request_deadline - time.monotonic(), 6)  # round to microseconds
        if deadline <= 0:
            raise TimeoutError(message='Request timed out waiting for stage notification', context=str(self._error_ctx))
        result_type = self._stage_notification_ft.result(timeout=deadline)
        if result_type == ParsedResultType.ROW:
            self.log_message('Received row, setting status to streaming', LogLevel.DEBUG)
            # we move to iterating rows
            self._request_state = RequestState.StreamingResults
        else:
            self.log_message(f'Received result type {result_type.name}', LogLevel.DEBUG)
