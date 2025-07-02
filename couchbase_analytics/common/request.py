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

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class RequestURL:
    scheme: str
    host: str
    port: int
    path: Optional[str] = None

    def get_formatted_url(self) -> str:
        """Get the formatted URL for this request."""
        if self.path is None:
            return f'{self.scheme}://{self.host}:{self.port}'
        return f'{self.scheme}://{self.host}:{self.port}{self.path}'

    def __repr__(self) -> str:
        details: Dict[str, str] = {
            'scheme': self.scheme,
            'host': self.host,
            'port': str(self.port),
            'path': self.path if self.path else ''
        }
        return f'{type(self).__name__}({details})'
    
    def __str__(self) -> str:
        return self.__repr__()