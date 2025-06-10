#!/usr/bin/env python

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

from __future__ import print_function

import random
from typing import Optional, Union

import anyio

class AsyncBytesIterator:

    def __init__(self,
                 data: Union[bytes, str],
                 chunk_size: Optional[int] = 100,
                 simulate_delay: Optional[bool] = False,
                 simulate_delay_range: Optional[tuple] = (0.01, 0.1)):
        self._data = data if isinstance(data, bytes) else bytes(data, 'utf-8')
        self._chunk_size = chunk_size
        self._simulate_delay = simulate_delay
        self._simulate_delay_range = simulate_delay_range
        self._start = 0
        self._stop = self._chunk_size

    def __aiter__(self):
        return self
    
    async def __anext__(self):
        if self._simulate_delay:
            delay = random.uniform(*self._simulate_delay_range)
            await anyio.sleep(delay)
        if not self._data:
            raise StopAsyncIteration
        while True:
            if len(self._data) == 0:
                raise StopAsyncIteration

            if self._start >= len(self._data):
                raise StopAsyncIteration

            if self._stop >= len(self._data):
                self._stop = len(self._data)

            chunk = self._data[self._start:self._stop]
            self._start = self._stop
            self._stop += self._chunk_size
            return chunk

class BytesIterator:

    def __init__(self, data: Union[bytes, str], chunk_size: Optional[int] = 100):
        self._data = data if isinstance(data, bytes) else bytes(data, 'utf-8')
        self._chunk_size = chunk_size
        self._start = 0
        self._stop = self._chunk_size

    def __iter__(self):
        return self
    
    def __next__(self):
        if not self._data:
            raise StopIteration
        while True:
            if len(self._data) == 0:
                raise StopIteration

            if self._start >= len(self._data):
                raise StopIteration

            if self._stop >= len(self._data):
                self._stop = len(self._data)

            chunk = self._data[self._start:self._stop]
            self._start = self._stop
            self._stop += self._chunk_size
            return chunk