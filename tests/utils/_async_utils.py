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

from types import TracebackType
from typing import Any, Callable, List, Optional, Type

import anyio


class TaskGroupResultCollector:

    def __init__(self) -> None:
        self._results: List[Any] = []

    @property
    def results(self) -> List[Any]:
        return self._results

    async def _execute(self, fn: Callable[..., Any], *args: object) -> None:
        result = await fn(*args)
        self._results.append(result)

    def start_soon(self, fn: Callable[..., Any], *args: object) -> None:
        self._taskgroup.start_soon(self._execute, fn, *args)

    async def __aenter__(self) -> TaskGroupResultCollector:
        self._taskgroup = anyio.create_task_group()
        await self._taskgroup.__aenter__()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> Any:
        try:
            res = await self._taskgroup.__aexit__(exc_type= exc_type, exc_val=exc_val, exc_tb=exc_tb)
            return res
        finally:
            del self._taskgroup
