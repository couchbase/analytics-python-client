#  Copyright 2016-2025. Couchbase, Inc.
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

from asyncio import AbstractEventLoop
from typing import Optional

import anyio


def get_time() -> float:
    """
    Get the current time in seconds since the epoch.
    """
    return anyio.current_time()


async def sleep(delay: float) -> None:
    await anyio.sleep(delay)


class AsyncBackend:
    def __init__(self, backend_lib: str) -> None:
        """
        Initialize the async backend.
        """
        self._backend_lib = backend_lib

    @property
    def backend_lib(self) -> str:
        """
        Get the name of the async backend library
        """
        return self._backend_lib

    @property
    def loop(self) -> Optional[AbstractEventLoop]:
        """
        Get the event loop for the async backend, if it exists
        """
        if not hasattr(self, '_loop'):
            if self._backend_lib == 'asyncio':
                import asyncio

                self._loop = asyncio.get_event_loop()
            else:
                raise RuntimeError('Unsupported async backend library.')
        return self._loop


def current_async_library() -> Optional[AsyncBackend]:
    try:
        import sniffio
    except ImportError:
        async_lib = 'asyncio'

    try:
        async_lib = sniffio.current_async_library()
    except sniffio.AsyncLibraryNotFoundError:
        async_lib = 'asyncio'

    if async_lib not in ('asyncio', 'trio'):
        raise RuntimeError('Running under an unsupported async environment.')

    # TODO(PYCO-71): Add trio support
    if async_lib == 'trio':
        raise RuntimeError('trio currently not supported')

    return AsyncBackend(async_lib)
