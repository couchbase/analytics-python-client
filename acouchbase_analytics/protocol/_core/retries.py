#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from asyncio import CancelledError
from functools import wraps
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Coroutine,
                    Optional,
                    Union)

from httpx import ConnectError, ConnectTimeout

from acouchbase_analytics.protocol._core.anyio_utils import sleep
from couchbase_analytics.common.errors import (AnalyticsError,
                                               InternalSDKError,
                                               TimeoutError)
from couchbase_analytics.common.streaming import StreamingState
from couchbase_analytics.protocol.errors import WrappedError

if TYPE_CHECKING:
    from acouchbase_analytics.protocol._core.request_context import AsyncRequestContext
    from acouchbase_analytics.protocol.streaming import AsyncHttpStreamingResponse


class AsyncRetryHandler:
    """
        **INTERNAL**
    """

    @staticmethod
    async def handle_httpx_retry(ex: Union[ConnectError, ConnectTimeout],
                                 ctx: AsyncRequestContext
                                 ) -> Optional[Exception]:
        err_str = str(ex)
        if 'SSL:' in err_str:
            message = 'TLS connection error occurred.'
            return AnalyticsError(cause=ex, message=message, context=str(ctx.error_context))
        delay = ctx.calculate_backoff()
        err: Optional[Exception] = None
        if not ctx.okay_to_delay_and_retry(delay):
            if ctx.retry_limit_exceeded:
                err = AnalyticsError(cause=ex, message='Retry limit exceeded.', context=str(ctx.error_context))
            else:
                err = TimeoutError(message='Request timed out during retry delay.', context=str(ctx.error_context))
        if err:
            return err
        await sleep(delay)
        return None

    @staticmethod
    async def handle_retry(ex: WrappedError,
                           ctx: AsyncRequestContext
                           ) -> Optional[Union[BaseException, Exception]]:
        if ex.retriable is True:
            delay = ctx.calculate_backoff()
            err: Optional[Union[BaseException, Exception]] = None
            if not ctx.okay_to_delay_and_retry(delay):
                if ctx.retry_limit_exceeded:
                    if ex.is_cause_query_err:
                        ex.maybe_set_cause_context(ctx.error_context)
                        err = ex.unwrap()
                    else:
                        err = AnalyticsError(cause=ex.unwrap(),
                                             message='Retry limit exceeded.',
                                             context=str(ctx.error_context))
                else:
                    err = TimeoutError(message='Request timed out during retry delay.',
                                       context=str(ctx.error_context))

            if err:
                return err
            await sleep(delay)
            return None
        ex.maybe_set_cause_context(ctx.error_context)
        return ex.unwrap()


    @staticmethod
    def with_retries(fn: Callable[[AsyncHttpStreamingResponse], Coroutine[Any, Any, None]]  # noqa: C901
                    ) -> Callable[[AsyncHttpStreamingResponse], Coroutine[Any, Any, None]]:
        @wraps(fn)
        async def wrapped_fn(self: AsyncHttpStreamingResponse) -> None:  # noqa: C901
            while True:
                try:
                    await fn(self)
                    break
                except WrappedError as ex:
                    err = await AsyncRetryHandler.handle_retry(ex, self._request_context)
                    if err is None:
                        continue
                    await self._request_context.shutdown(type(ex), ex, ex.__traceback__)
                    raise err from None
                except (ConnectError, ConnectTimeout) as ex:
                    err = await AsyncRetryHandler.handle_httpx_retry(ex, self._request_context)
                    if err is None:
                        continue
                    await self._request_context.shutdown(type(ex), ex, ex.__traceback__)
                    raise err from None
                except AnalyticsError:
                    # if an AnalyticsError is raised, we have already shut down the request context
                    raise
                except RuntimeError as ex:
                    await self._request_context.shutdown(type(ex), ex, ex.__traceback__)
                    raise ex
                except BaseException as ex:
                    await self._request_context.shutdown(type(ex), ex, ex.__traceback__)
                    if self._request_context.timed_out:
                        raise TimeoutError(message='Request timed out.',
                                           context=str(self._request_context.error_context)) from None
                    if self._request_context.cancelled:
                        raise CancelledError('Request was cancelled.') from None
                    if self._request_context.request_error is not None:
                        raise self._request_context.request_error from None
                    raise InternalSDKError(cause=ex,
                                           message=str(ex),
                                           context=str(self._request_context.error_context)) from None
                finally:
                    if not StreamingState.is_okay(self._request_context.request_state):
                        await self.close()

        return wrapped_fn
