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

import json
from typing import Any, Callable, List, Optional, TypedDict

from couchbase_analytics.common._core.duration_str_utils import parse_duration_str
from couchbase_analytics.common.logging import LogLevel


class QueryMetricsCore(TypedDict, total=False):
    """
    **INTERNAL**
    """

    elapsed_time: float
    execution_time: float
    compile_time: float
    queue_wait_time: float
    result_count: int
    result_size: int
    processed_objects: int
    buffer_cache_hit_ratio: str
    buffer_cache_page_read_count: int


class QueryWarningCore(TypedDict, total=False):
    """
    **INTERNAL**
    """

    code: int
    message: str


class QueryMetadataCore(TypedDict, total=False):
    """
    **INTERNAL**
    """

    request_id: str
    client_context_id: str
    warnings: List[QueryWarningCore]
    metrics: QueryMetricsCore
    status: Optional[str]


def _parse_duration_metric(metrics: Any, field: str, log_fn: Optional[Callable[[str, LogLevel], None]] = None) -> float:
    raw = metrics.get(field, '0')
    try:
        return parse_duration_str(raw, in_millis=True)
    except ValueError:
        if log_fn is not None:
            log_fn(
                f'Could not parse metrics field "{field}"; received value="{raw}". Defaulting to 0.',
                LogLevel.WARNING,
            )
        return 0.0


def build_query_metadata(
    json_data: Optional[Any] = None,
    raw_metadata: Optional[bytes] = None,
    request_id: Optional[str] = None,
    log_fn: Optional[Callable[[str, LogLevel], None]] = None,
) -> QueryMetadataCore:
    """
    Builds the query metadata from the raw bytes.

    Args:
        metadata (bytes): The raw metadata bytes.

    Returns:
        QueryMetadataCore: The parsed query metadata.
    """
    if json_data is None and raw_metadata is None:
        raise ValueError('No metadata provided.')

    if json_data is None and raw_metadata is not None:
        json_data = json.loads(raw_metadata.decode('utf-8'))

    if json_data is None or not isinstance(json_data, dict):
        raise ValueError('Invalid query metadata format. Expected a JSON object.')

    warnings: List[QueryWarningCore] = []
    for warning in json_data.get('warnings', []):
        warnings.append({'code': warning.get('code', 0), 'message': warning.get('msg', '')})

    metadata: QueryMetadataCore = {
        'request_id': json_data.get('requestID', request_id or ''),
        'client_context_id': json_data.get('clientContextID', ''),
        'warnings': warnings,
    }

    if 'status' in json_data:
        metadata['status'] = json_data.get('status', '')

    if 'metrics' not in json_data:
        metadata['metrics'] = {}
        return metadata

    metrics: QueryMetricsCore = {
        'elapsed_time': _parse_duration_metric(json_data['metrics'], 'elapsedTime', log_fn),
        'execution_time': _parse_duration_metric(json_data['metrics'], 'executionTime', log_fn),
        'compile_time': _parse_duration_metric(json_data['metrics'], 'compileTime', log_fn),
        'queue_wait_time': _parse_duration_metric(json_data['metrics'], 'queueWaitTime', log_fn),
        'result_count': json_data['metrics'].get('resultCount', 0),
        'result_size': json_data['metrics'].get('resultSize', 0),
        'processed_objects': json_data['metrics'].get('processedObjects', 0),
        'buffer_cache_hit_ratio': json_data['metrics'].get('bufferCacheHitRatio', ''),
        'buffer_cache_page_read_count': json_data['metrics'].get('bufferCachePageReadCount', 0),
    }

    metadata['metrics'] = metrics
    return metadata
