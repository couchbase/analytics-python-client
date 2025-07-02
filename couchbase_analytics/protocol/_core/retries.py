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

from concurrent.futures import CancelledError
from functools import wraps
from random import uniform
from time import sleep
from typing import TYPE_CHECKING, Callable

from httpx import ConnectError, ConnectTimeout

from couchbase_analytics.common.errors import AnalyticsError, InternalSDKError, TimeoutError
from couchbase_analytics.common.streaming import StreamingState
from couchbase_analytics.protocol.errors import WrappedError

if TYPE_CHECKING:
    from couchbase_analytics.protocol.streaming import HttpStreamingResponse

class RetryHandler:
    """
        **INTERNAL**
    """

    @staticmethod
    def with_retries(fn: Callable[[HttpStreamingResponse], None]) -> Callable[[HttpStreamingResponse], None]:  # noqa: C901
        @wraps(fn)
        def wrapped_fn(self: HttpStreamingResponse) -> None:  # noqa: C901
            while True:
                try:
                    fn(self)
                    break
                except WrappedError as ex:
                    if ex.retriable is True:
                        delay = calc_backoff(self._request_context.error_context.num_attempts)
                        if not self._request_context.okay_to_delay_and_retry(delay):
                            self._request_context.shutdown(ex)
                            raise TimeoutError(message='Request timed out during retry delay.',
                                               context=str(self._request_context.error_context)) from None
                        sleep(delay)
                        continue
                    self._request_context.shutdown(ex)
                    ex.maybe_set_cause_context(self._request_context.error_context)
                    raise ex.unwrap() from None
                except AnalyticsError:
                    # if an AnalyticsError is raised, we have already shut down the request context
                    raise
                except RuntimeError as ex:
                    self._request_context.shutdown(ex)
                    raise ex
                except ConnectError as ex:
                    self._request_context.shutdown(ex)
                    raise AnalyticsError(cause=ex,
                                         message='Unable to establish connection for request.',
                                         context=str(self._request_context.error_context)) from None
                except ConnectTimeout as ex:
                    self._request_context.shutdown(ex)
                    raise TimeoutError(cause=ex,
                                       message='Request timed out trying to establish connection.',
                                       context=str(self._request_context.error_context)) from None
                except BaseException as ex:
                    self._request_context.shutdown(ex)
                    if self._request_context.request_error is not None:
                        raise self._request_context.request_error from None
                    if self._request_context.timed_out:
                        raise TimeoutError(message='Request timeout.',
                                           context=str(self._request_context.error_context)) from None
                    if self._request_context.cancelled:
                        raise CancelledError('Request was cancelled.') from None
                    raise InternalSDKError(cause=ex,
                                           message=str(ex),
                                           context=str(self._request_context.error_context)) from None
                finally:
                    if not StreamingState.is_okay(self._request_context.request_state):
                        self.close()

        return wrapped_fn
    
def calc_backoff(retry_count: int) -> float:
    min_ms = 100
    max_ms = 60000
    delay_ms = min_ms * pow(2, retry_count)
    capped_ms = min(max_ms, delay_ms)
    return uniform(0, capped_ms / 1000.0)


