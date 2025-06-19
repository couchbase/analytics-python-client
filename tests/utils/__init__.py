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

from __future__ import annotations

import os
import pathlib
import random

from collections.abc import AsyncIterator as PyAsyncIterator
from collections.abc import Iterator
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)
from urllib.parse import quote

import anyio

class AsyncBytesIterator(PyAsyncIterator[bytes]):

    def __init__(self,
                 data: Union[bytes, str],
                 chunk_size: Optional[int] = 100,
                 simulate_delay: Optional[bool] = False,
                 simulate_delay_range: Optional[Tuple[float, float]] = (0.01, 0.1)) -> None:
        self._data = data if isinstance(data, bytes) else bytes(data, 'utf-8')
        self._chunk_size = chunk_size or 100
        self._simulate_delay = simulate_delay or False
        self._simulate_delay_range = simulate_delay_range or (0.01, 0.1)
        self._start = 0
        self._stop = self._chunk_size

    def __aiter__(self) -> AsyncBytesIterator:
        return self
    
    async def __anext__(self) -> bytes:
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

class BytesIterator(Iterator[bytes]):

    def __init__(self, data: Union[bytes, str], chunk_size: Optional[int] = 100) -> None:
        self._data = data if isinstance(data, bytes) else bytes(data, 'utf-8')
        self._chunk_size = chunk_size or 100
        self._start = 0
        self._stop = self._chunk_size

    def __iter__(self) -> BytesIterator:
        return self
    
    def __next__(self) -> bytes:
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
        

def get_test_cert_path() -> str:
    return os.path.join(pathlib.Path(__file__).parent, 'certs', 'dinocluster.pem')

def get_test_cert_list() -> List[str]:
    cert_file = pathlib.Path(get_test_cert_path())
    cert_file1 = pathlib.Path(os.path.join(pathlib.Path(__file__).parent, 'certs', 'dinoca.pem'))
    return [cert_file.read_text(), cert_file1.read_text()]

def get_test_cert_str() -> str:
    cert_file = pathlib.Path(get_test_cert_path())
    return cert_file.read_text()

def to_query_str(params: Dict[str, Any]) -> str:
    encoded_params = []
    for k, v in params.items():
        if v in [True, False]:
            encoded_params.append(f'{quote(k)}={quote(str(v).lower())}')
        else:
            encoded_params.append(f'{quote(k)}={quote(str(v))}')

    return '&'.join(encoded_params)