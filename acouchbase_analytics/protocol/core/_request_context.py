from __future__ import annotations

from asyncio import CancelledError, Task
from types import TracebackType
from typing import (Any,
                    Awaitable,
                    Callable,
                    Coroutine,
                    Dict,
                    List,
                    Optional,
                    Type,
                    Union,
                    TYPE_CHECKING)
from uuid import uuid4

import anyio
from httpx import (Response as HttpCoreResponse,
                   TimeoutException)


from acouchbase_analytics.protocol.core._anyio_utils import (AsyncBackend,
                                                             current_async_library,
                                                             get_time)
from couchbase_analytics.common.core.net_utils import get_request_ip_async
from couchbase_analytics.common.deserializer import Deserializer
from couchbase_analytics.common.errors import AnalyticsError, InvalidCredentialError
from couchbase_analytics.common.streaming import StreamingState
from couchbase_analytics.protocol.connection import DEFAULT_TIMEOUTS
from couchbase_analytics.protocol.errors import ErrorMapper

if TYPE_CHECKING:
    from acouchbase_analytics.protocol.core.client_adapter import _AsyncClientAdapter
    from couchbase_analytics.protocol.core.request import QueryRequest

class AsyncRequestContext:
    # TODO: AsyncExitStack??
    # https://anyio.readthedocs.io/en/stable/cancellation.html

    def __init__(self,
                 client_adapter: _AsyncClientAdapter,
                 request: QueryRequest,
                 backend: Optional[AsyncBackend]=None) -> None:
        self._id = str(uuid4())
        self._client_adapter = client_adapter
        self._request = request
        self._backend = backend or current_async_library()
        # self._response_task: Optional[Task] = None
        self._request_state = StreamingState.NotStarted
        self._stage_completed: Optional[anyio.Event] = None
        self._request_error: Optional[Union[BaseException, Exception]] = None
        connect_timeout = self._client_adapter.connection_details.get_connect_timeout()
        self._connect_deadline = get_time() + connect_timeout
        self._cancel_scope_deadline_updated = False

    @property
    def deserializer(self) -> Deserializer:
        """
        Returns the deserializer used by this request context.
        """
        return self._request.deserializer

    @property
    def has_stage_completed(self) -> bool:
        return self._stage_completed is not None and self._stage_completed.is_set()

    @property
    def okay_to_iterate(self) -> bool:
        self._check_cancelled_or_timed_out()
        return StreamingState.okay_to_iterate(self._request_state)
    
    @property
    def okay_to_stream(self) -> bool:
        self._check_cancelled_or_timed_out()
        return StreamingState.okay_to_stream(self._request_state)

    @property
    def request_error(self) -> Optional[Union[BaseException, Exception]]:
        return self._request_error

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
        return self._request_state in [StreamingState.Cancelled, StreamingState.AsyncCancelledPriorToTimeout]

    def _check_cancelled_or_timed_out(self) -> None:
        if self._request_state in [StreamingState.Timeout, StreamingState.Cancelled, StreamingState.Error]:
            return
        
        if hasattr(self, '_request_deadline') is False:
            return

        current_time = get_time()
        if self._cancel_scope_deadline_updated is False:
            timed_out = current_time >= self._connect_deadline
        else:
            timed_out = current_time >= self._request_deadline

        if timed_out:
            if self._request_state == StreamingState.Cancelled:
                self._request_state = StreamingState.AsyncCancelledPriorToTimeout
            else:
                self._request_state = StreamingState.Timeout

    async def _execute(self, fn: Callable[..., Awaitable[Any]], *args: object) -> None:
        await fn(*args)
        if self._stage_completed is not None:
            self._stage_completed.set()

    def _maybe_set_request_error(self,
                                 exc_type: Optional[Type[BaseException]]=None,
                                 exc_val: Optional[BaseException]=None) -> None:
        self._check_cancelled_or_timed_out()
        # TODO:  Do either of these conditions need to be checked?  Does _check_cancelled_or_timed_out() already handle this
        # if self._taskgroup.cancel_scope.cancelled_caught and get_time() >= self._taskgroup.cancel_scope.deadline:
        # if isinstance(exc_val, CancelledError):
        if exc_val is None:
            return
        if not StreamingState.is_timeout_or_cancelled(self._request_state):
            # This handles httpx timeouts
            if exc_type is not None and issubclass(exc_type, TimeoutException):
                self._request_state = StreamingState.Timeout
            elif issubclass(type(exc_val), TimeoutException):
                self._request_state = StreamingState.Timeout
            else:
                self._request_state = StreamingState.Error
            self._request_error = exc_val
        

    async def _trace_handler(self, event_name: str, _: str) -> None:
        if event_name == 'connection.connect_tcp.complete':
            # after connection is established, we need to update the cancel_scope deadline to match the query_timeout
            self._update_cancel_scope_deadline(self._request_deadline, is_absolute=True)
            self._cancel_scope_deadline_updated = True
        elif self._cancel_scope_deadline_updated is False and event_name.endswith('send_request_headers.started'):
            # if the socket is reused, we won't get the connect_tcp.complete event, so the deadline at the next closest event
            self._update_cancel_scope_deadline(self._request_deadline, is_absolute=True)
            self._cancel_scope_deadline_updated = True

    def _update_cancel_scope_deadline(self, deadline: float, is_absolute: Optional[bool]=False) -> None:
        # TODO:  confirm scenario of get_time() < self._taskgroup.cancel_scope.deadline is handled by anyio
        new_deadline = deadline if is_absolute else get_time() + deadline
        # TODO:  Useful debug log message
        # print(f'Updating cancel scope deadline: {self._taskgroup.cancel_scope.deadline} -> {new_deadline}')
        if get_time() >= new_deadline:
            self._taskgroup.cancel_scope.cancel()
        else:
            self._taskgroup.cancel_scope.deadline = new_deadline

    def cancel_request(self,
                       fn: Optional[Callable[..., Awaitable[Any]]]=None,
                       *args: object) -> None:
        if fn is not None:
            self._taskgroup.start_soon(fn, *args)
        if self._request_state == StreamingState.Timeout:
            return
        self._taskgroup.cancel_scope.cancel()
        self._request_state = StreamingState.Cancelled

    async def initialize(self) -> None:
        await self.__aenter__()
        self._request_state = StreamingState.Started
        # we set the request timeout once the context is initialized in order to create the deadline 
        # closer to when the upstream logic will begin to use the request context
        timeouts = self._request.get_request_timeouts() or {}
        self._request_deadline = get_time() + (timeouts.get('read', None) or DEFAULT_TIMEOUTS['query_timeout'])
        self._update_cancel_scope_deadline(self._connect_deadline, is_absolute=True)

    async def send_request(self, enable_trace_handling: Optional[bool]=False) -> HttpCoreResponse:
        ip = await get_request_ip_async(self._request.host, self._request.port, self._request.previous_ips)
        if ip is None:
            attempted_ips = ', '.join(self._request.previous_ips or [])
            raise AnalyticsError(message=f'Connect failure.  Attempted to connect to resolved IPs: {attempted_ips}.')
        
        if enable_trace_handling is True:
            (self._request.update_url(ip, self._client_adapter.analytics_path)
                          .add_trace_to_extensions(self._trace_handler)
                          .update_previous_ips(ip))
        else:
            self._request.update_url(ip, self._client_adapter.analytics_path).update_previous_ips(ip)
        # TODO:  add logging; provide request details (to/from, deadlines, etc.)
        response = await self._client_adapter.send_request(self._request)
        self._request.set_client_server_addrs(response)
        if response.status_code == 401:
            context = {
                'client_addr': self._request.client_addr,
                'server_addr': self._request.server_addr,
                'http_status': response.status_code,
            }
            raise InvalidCredentialError(str(context))
        return response

    async def shutdown(self,
                       exc_type: Optional[Type[BaseException]]=None,
                       exc_val: Optional[BaseException]=None,
                       exc_tb: Optional[TracebackType]=None) -> None:
        if hasattr(self, '_taskgroup'):
            await self.__aexit__(exc_type, exc_val, exc_tb)
        else:
            self._maybe_set_request_error()

        if StreamingState.is_okay(self._request_state):
            self._request_state = StreamingState.Completed

    def create_response_task(self, fn: Callable[..., Coroutine[Any, Any, Any]], *args: object) -> Task[Any]:
        if self._backend is None or self._backend.backend_lib != 'asyncio':
            raise RuntimeError('Must use the asyncio backend to create a response task.')
        if self._backend.loop is None:
            raise RuntimeError('Async backend loop is not initialized.')
        task_name = f'{self._id}-response-task'
        print(f'Creating response task: {task_name}')
        task: Task[Any] = self._backend.loop.create_task(fn(*args), name=task_name)
        # TODO: I don't think this callback is necessary...need to add more tests to confirm
        def task_done(t: Task[Any]) -> None:
            print(f'Task done callback task=({t.get_name()}); done: {t.done()}, cancelled: {t.cancelled()}')

        task.add_done_callback(task_done)
        self._response_task = task
        return task

    def set_state_to_streaming(self) -> None:
        self._request_state = StreamingState.StreamingResults

    def start_next_stage(self,
                         fn: Callable[..., Awaitable[Any]],
                         *args: object,
                         reset_previous_stage: Optional[bool]=False) -> None:
        if self._stage_completed is not None:
            if reset_previous_stage is True:
                self._stage_completed = None
            else:
                raise RuntimeError('Task already running in this context.')

        self._stage_completed = anyio.Event()
        self._taskgroup.start_soon(self._execute, fn, *args)

    async def wait_for_stage_to_complete(self) -> None:
        if self._stage_completed is None:
            return
        await self._stage_completed.wait()

    async def process_error(self, json_data: List[Dict[str, Any]]) -> None:
        self._request_state = StreamingState.Error
        if not isinstance(json_data, list):
            self._request_error = AnalyticsError('Cannot parse error response; expected JSON array')

        self._request_error = ErrorMapper.build_error_from_json(json_data, status_code=self._request.response_status_code)
        await self.shutdown()
        raise self._request_error

    async def __aenter__(self) -> AsyncRequestContext:
        self._taskgroup = anyio.create_task_group()
        await self._taskgroup.__aenter__()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> Optional[bool]:
        try:
            res = await self._taskgroup.__aexit__(exc_type, exc_val, exc_tb)
            return res
        except BaseException as ex:
            pass # we handle the error when the context is shutdown (which is what calls __aexit__())
        finally:
            self._maybe_set_request_error()
            del self._taskgroup
            # TODO:  should we suppress here (e.g., return True)
            return None