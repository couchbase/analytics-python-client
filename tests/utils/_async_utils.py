
from typing import (Any,
                    List)

import anyio

class TaskGroupResultCollector:

    def __init__(self):
        self._results = []

    @property
    def results(self) -> List[Any]:
        return self._results

    async def _execute(self, fn, *args):
        result = await fn(*args)
        self._results.append(result)

    def start_soon(self, fn, *args):
        self._taskgroup.start_soon(self._execute, fn, *args)
    
    async def __aenter__(self):
        self._taskgroup = anyio.create_task_group()
        await self._taskgroup.__aenter__()
        return self

    async def __aexit__(self, *tb):
        try:
            res = await self._taskgroup.__aexit__(*tb)
            return res
        finally:
            del self._taskgroup