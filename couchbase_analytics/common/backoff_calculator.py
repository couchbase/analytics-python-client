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

from abc import ABC, abstractmethod
from random import uniform
from typing import Optional


class BackoffCalculator(ABC):
    @abstractmethod
    def calculate_backoff(self, retry_count: int) -> float:
        raise NotImplementedError


class DefaultBackoffCalculator(BackoffCalculator):
    MIN = 100
    MAX = 60 * 1000
    EXPONENT_BASE = 1.5

    def __init__(
        self, min: Optional[int] = None, max: Optional[int] = None, exponent_base: Optional[int] = None
    ) -> None:
        self._min = min or self.MIN
        self._max = max or self.MAX
        self._exp = exponent_base or self.EXPONENT_BASE

    def calculate_backoff(self, retry_count: int) -> float:
        delay_ms = self._min * self._exp ** (retry_count - 1)
        capped_ms = min(self._max, delay_ms)
        return uniform(0, capped_ms)  # nosec B311
