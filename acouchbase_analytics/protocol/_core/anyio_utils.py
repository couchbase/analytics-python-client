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

    # TODO: This helps make tests work.
    #       Should we work through the scenario when sniffio cannot find the async library?
    try:
        async_lib = sniffio.current_async_library()
    except sniffio.AsyncLibraryNotFoundError:
        async_lib = 'asyncio'

    if async_lib not in ('asyncio', 'trio'):
        raise RuntimeError('Running under an unsupported async environment.')

    # TODO: confirm trio support
    if async_lib == 'trio':
        raise RuntimeError('trio currently not supported')

    return AsyncBackend(async_lib)
