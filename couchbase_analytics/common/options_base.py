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

import sys
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Literal, Optional, TypedDict, Union

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias, Unpack
else:
    if sys.version_info < (3, 11):
        from typing import TypeAlias

        from typing_extensions import Unpack
    else:
        from typing import TypeAlias, Unpack

from couchbase_analytics.common import JSONType
from couchbase_analytics.common._core import JsonStreamConfig
from couchbase_analytics.common.deserializer import Deserializer
from couchbase_analytics.common.enums import QueryScanConsistency

"""
    Python Analytics SDK Cluster Options Classes
"""


class ClusterOptionsKwargs(TypedDict, total=False):
    deserializer: Optional[Deserializer]
    security_options: Optional[SecurityOptionsBase]
    timeout_options: Optional[TimeoutOptionsBase]


ClusterOptionsValidKeys: TypeAlias = Literal[
    'deserializer',
    'security_options',
    'timeout_options',
]


class ClusterOptionsBase(Dict[str, Any]):
    """
        **INTERNAL**
    """

    VALID_OPTION_KEYS: List[ClusterOptionsValidKeys] = [
        'deserializer',
        'security_options',
        'timeout_options',
    ]

    def __init__(self, **kwargs: Unpack[ClusterOptionsKwargs]) -> None:
        filtered_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**filtered_kwargs)


class SecurityOptionsKwargs(TypedDict, total=False):
    trust_only_capella: Optional[bool]
    trust_only_pem_file: Optional[str]
    trust_only_pem_str: Optional[str]
    trust_only_certificates: Optional[List[str]]
    disable_server_certificate_verification: Optional[bool]


SecurityOptionsValidKeys: TypeAlias = Literal[
    'trust_only_capella',
    'trust_only_pem_file',
    'trust_only_pem_str',
    'trust_only_certificates',
    'disable_server_certificate_verification',
]


class SecurityOptionsBase(Dict[str, object]):
    """
        **INTERNAL**
    """

    VALID_OPTION_KEYS: List[SecurityOptionsValidKeys] = [
        'trust_only_capella',
        'trust_only_pem_file',
        'trust_only_pem_str',
        'trust_only_certificates',
        'disable_server_certificate_verification',
    ]

    def __init__(self, **kwargs: Unpack[SecurityOptionsKwargs]) -> None:
        filtered_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**filtered_kwargs)


class TimeoutOptionsKwargs(TypedDict, total=False):
    connect_timeout: Optional[timedelta]
    query_timeout: Optional[timedelta]


TimeoutOptionsValidKeys: TypeAlias = Literal[
    'connect_timeout',
    'query_timeout',
]


class TimeoutOptionsBase(Dict[str, object]):
    """
        **INTERNAL**
    """

    VALID_OPTION_KEYS: List[TimeoutOptionsValidKeys] = [
        'connect_timeout',
        'query_timeout',
    ]

    def __init__(self, **kwargs: Unpack[TimeoutOptionsKwargs]) -> None:
        filtered_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**filtered_kwargs)


class QueryOptionsKwargs(TypedDict, total=False):
    client_context_id: Optional[str]
    deserializer: Optional[Deserializer]
    lazy_execute: Optional[bool]
    named_parameters: Optional[Dict[str, JSONType]]
    positional_parameters: Optional[Iterable[JSONType]]
    query_context: Optional[str]
    raw: Optional[Dict[str, Any]]
    readonly: Optional[bool]
    scan_consistency: Optional[Union[QueryScanConsistency, str]]
    stream_config: Optional[JsonStreamConfig]
    timeout: Optional[timedelta]


QueryOptionsValidKeys: TypeAlias = Literal[
    'client_context_id',
    'deserializer',
    'lazy_execute',
    'named_parameters',
    'positional_parameters',
    'query_context',
    'raw',
    'readonly',
    'scan_consistency',
    'stream_config',
    'timeout',
]


class QueryOptionsBase(Dict[str, object]):

    VALID_OPTION_KEYS: List[QueryOptionsValidKeys] = [
        'client_context_id',
        'deserializer',
        'lazy_execute',
        'named_parameters',
        'positional_parameters',
        'query_context',
        'raw',
        'readonly',
        'scan_consistency',
        'stream_config',
        'timeout',
    ]

    def __init__(self, **kwargs: Unpack[QueryOptionsKwargs]) -> None:
        filtered_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**filtered_kwargs)
