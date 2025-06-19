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

import json
from time import time
from typing import Dict, TYPE_CHECKING

import pytest

from couchbase_analytics.common.core import (JsonParsingError,
                                             JsonStreamConfig,
                                             ParsedResult,
                                             ParsedResultType)

from couchbase_analytics.common.core.async_json_stream import AsyncJsonStream
from couchbase_analytics.common.errors import AnalyticsError
from tests.environments.simple_environment import JsonDataType
from tests.utils import AsyncBytesIterator
from tests.utils._async_utils import TaskGroupResultCollector

if TYPE_CHECKING:
    from tests.environments.simple_environment import AsyncSimpleEnvironment


class JsonParsingTestSuite:
    TEST_MANIFEST = [
        'test_analytics_error',
        'test_analytics_error_mid_stream',
        'test_analytics_many_rows',
        'test_analytics_multiple_errors',
        'test_analytics_parses_async',
        'test_analytics_simple_result',

        'test_array',
        'test_array_empty',
        'test_array_mixed_types',
        'test_array_of_objects',

        'test_invalid_empty',
        'test_invalid_garbage_between_objects',
        'test_invalid_leading_garbage',
        'test_invalid_trailing_garbage',
        'test_invalid_whitespace_only',

        'test_object',
        'test_object_complex_nested_structure',
        'test_object_empty',
        'test_object_simple_nested',
        'test_object_with_empty_key_and_value',
        'test_object_with_unicode',

        'test_value_bool',
        'test_value_null',
    ]

    @pytest.mark.parametrize('buffered_result', [True, False])
    async def test_analytics_error(self,
                                   async_test_env: AsyncSimpleEnvironment,
                                   buffered_result: bool) -> None:
        json_object, bytes_data = async_test_env.get_json_data(JsonDataType.FAILED_REQUEST)
        if buffered_result:
            parser = AsyncJsonStream(AsyncBytesIterator(bytes_data),
                                     stream_config=JsonStreamConfig(buffer_entire_result=True))
        else:
            parser = AsyncJsonStream(AsyncBytesIterator(bytes_data))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.ERROR
        assert isinstance(result.value, bytes)
        assert json.loads(result.value.decode('utf-8')) == json_object
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    async def test_analytics_error_mid_stream(self, async_test_env: AsyncSimpleEnvironment) -> None:
        json_object, bytes_data = async_test_env.get_json_data(JsonDataType.FAILED_REQUEST_MID_STREAM)
        parser = AsyncJsonStream(AsyncBytesIterator(bytes_data))
        await parser.start_parsing()
        row_idx = 0
        while True:
            result = await parser.get_result()
            if result is None and not parser.token_stream_exhausted:
                await parser.continue_parsing()
                continue
            assert isinstance(result, ParsedResult)
            assert result.result_type in [ParsedResultType.ROW, ParsedResultType.ERROR]
            assert isinstance(result.value, bytes)
            if result.result_type == ParsedResultType.ROW:
                assert json.loads(result.value.decode('utf-8')) == json_object['results'][row_idx]
                row_idx += 1
            else:
                final_result = result.value.decode('utf-8')
                break

        # if we are not buffering the entire result, the final result will exclude the results key
        json_object.pop('results')
        assert json.loads(final_result) == json_object
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    async def test_analytics_many_rows(self, async_test_env: AsyncSimpleEnvironment) -> None:
        json_object, bytes_data = async_test_env.get_json_data(JsonDataType.MULTIPLE_RESULTS)
        parser = AsyncJsonStream(AsyncBytesIterator(bytes_data))
        await parser.start_parsing()
        row_idx = 0
        while row_idx < 36:
            result = await parser.get_result()
            if result is None and not parser.token_stream_exhausted:
                await parser.continue_parsing()
                continue
            assert isinstance(result, ParsedResult)
            assert result.result_type == ParsedResultType.ROW
            assert isinstance(result.value, bytes)
            assert json.loads(result.value.decode('utf-8')) == json_object['results'][row_idx]
            row_idx += 1

        final_result = await parser.get_result()
        assert isinstance(final_result, ParsedResult)
        assert final_result.result_type == ParsedResultType.END
        assert isinstance(final_result.value, bytes)
        # if we are not buffering the entire result, the final result will exclude the results key
        json_object.pop('results')
        assert json.loads(final_result.value.decode('utf-8')) == json_object
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.parametrize('buffered_result', [True, False])
    async def test_analytics_multiple_errors(self,
                                             async_test_env: AsyncSimpleEnvironment,
                                             buffered_result: bool) -> None:
        json_object, bytes_data = async_test_env.get_json_data(JsonDataType.FAILED_REQUEST_MULTI_ERRORS)
        if buffered_result:
            parser = AsyncJsonStream(AsyncBytesIterator(bytes_data),
                                     stream_config=JsonStreamConfig(buffer_entire_result=True))
        else:
            parser = AsyncJsonStream(AsyncBytesIterator(bytes_data))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.ERROR
        assert isinstance(result.value, bytes)
        assert json.loads(result.value.decode('utf-8')) == json_object
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    async def test_analytics_parses_async(self, async_test_env: AsyncSimpleEnvironment) -> None:
        json_object, bytes_data = async_test_env.get_json_data(JsonDataType.MULTIPLE_RESULTS)
        async def _run_async(idx: int) -> Dict[float, int]:
            parser = AsyncJsonStream(AsyncBytesIterator(bytes_data,
                                                              simulate_delay=True,
                                                              simulate_delay_range=(0.01, 0.1)))
            await parser.start_parsing()
            row_idx = 0
            while row_idx < 36:
                result = await parser.get_result()
                if result is None and not parser.token_stream_exhausted:
                    await parser.continue_parsing()
                    continue
                assert isinstance(result, ParsedResult)
                assert result.result_type == ParsedResultType.ROW
                assert isinstance(result.value, bytes)
                assert json.loads(result.value.decode('utf-8')) == json_object['results'][row_idx]
                row_idx += 1

            return {time(): idx}
        
        async with TaskGroupResultCollector() as tg:
            for idx in range(10):
                tg.start_soon(_run_async, idx)
        ordered_results = dict(sorted({k: v for r in tg.results for k, v in r.items()}.items()))
        assert list(ordered_results.values()) != list(i for i in range(10))

    @pytest.mark.parametrize('buffered_result', [True, False])
    async def test_analytics_simple_result(self,
                                           async_test_env: AsyncSimpleEnvironment,
                                           buffered_result: bool) -> None:
        json_object, bytes_data = async_test_env.get_json_data(JsonDataType.SIMPLE_REQUEST)
        if buffered_result:
            parser = AsyncJsonStream(AsyncBytesIterator(bytes_data),
                                     stream_config=JsonStreamConfig(buffer_entire_result=True))
        else:
            parser = AsyncJsonStream(AsyncBytesIterator(bytes_data))
        await parser.start_parsing()
        # check for individual rows when not buffering the result
        if not buffered_result:
            result = await parser.get_result()
            assert isinstance(result, ParsedResult)
            assert result.result_type == ParsedResultType.ROW
            assert isinstance(result.value, bytes)
            assert json.loads(result.value.decode('utf-8')) == json_object['results'][0]

        final_result = await parser.get_result()
        assert isinstance(final_result, ParsedResult)
        assert final_result.result_type == ParsedResultType.END
        assert isinstance(final_result.value, bytes)
        # we don't store the 'results' if buffering is not enabled
        if not buffered_result:
            json_object.pop('results')
        assert json.loads(final_result.value.decode('utf-8')) == json_object
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_array(self) -> None:
        data = '[1,2,"three"]'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_array_empty(self) -> None:
        data = '[]'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_array_mixed_types(self) -> None:
        data = '[123,"text",true,null,{"key":"value"}]'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_array_of_objects(self) -> None:
        data = '[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_invalid_empty(self) -> None:
        try:
            data = ''
            parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                     stream_config=JsonStreamConfig(buffer_entire_result=True))
            await parser.start_parsing()
        except JsonParsingError as err:
            assert isinstance(err, JsonParsingError)
            assert err.cause is not None
            assert 'parse error' in str(err.cause)

    @pytest.mark.anyio
    async def test_invalid_garbage_between_objects(self) -> None:
        try:
            data = '[{"id":1,"name":"Alice"},garbage,{"id":2,"name":"Bob"}]'
            parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                     stream_config=JsonStreamConfig(buffer_entire_result=True))
            await parser.start_parsing()
        except JsonParsingError as err:
            assert isinstance(err, JsonParsingError)
            assert err.cause is not None
            assert 'lexical error' in str(err.cause)

    @pytest.mark.anyio
    async def test_invalid_leading_garbage(self) -> None:
        try:
            data = 'garbage{"key":"value"}'
            parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                     stream_config=JsonStreamConfig(buffer_entire_result=True))
            await parser.start_parsing()
        except JsonParsingError as err:
            assert isinstance(err, JsonParsingError)
            assert err.cause is not None
            assert 'lexical error' in str(err.cause)

    @pytest.mark.anyio
    async def test_invalid_trailing_garbage(self) -> None:
        try:
            data = '{"key":"value"}garbage'
            parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                     stream_config=JsonStreamConfig(buffer_entire_result=True))
            await parser.start_parsing()
        except JsonParsingError as err:
            assert isinstance(err, JsonParsingError)
            assert err.cause is not None
            assert 'parse error' in str(err.cause)

    @pytest.mark.anyio
    async def test_invalid_whitespace_only(self) -> None:
        try:
            data = '   \n\t  '
            parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                     stream_config=JsonStreamConfig(buffer_entire_result=True))
            await parser.start_parsing()
        except JsonParsingError as err:
            assert isinstance(err, JsonParsingError)
            assert err.cause is not None
            assert 'parse error' in str(err.cause)

    @pytest.mark.anyio
    async def test_value_bool(self) -> None:
        data = 'true'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_value_null(self) -> None:
        data = 'null'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_object(self) -> None:
        data = '{"name":"John","age":30,"city":"New York"}'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_object_complex_nested_structure(self) -> None:
        data_list = ['{"users":[{"id":1,"name":"Alice","roles":["admin","editor"]},'
                     '{"id":2,"name":"Bob","roles":["viewer"]}],',
                     '"meta":{"count":2,"status":"success"}}']
        data = ''.join(data_list)
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_object_empty(self) -> None:
        data = '{}'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_object_simple_nested(self) -> None:
        data = '{"outer":{"inner":{"key":"value"}}}'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_object_with_empty_key_and_value(self) -> None:
        data = '{"":""}'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

    @pytest.mark.anyio
    async def test_object_with_unicode(self) -> None:
        data = '{"name":"你好","city":"Denver"}'
        parser = AsyncJsonStream(AsyncBytesIterator(bytes(data, 'utf-8')),
                                 stream_config=JsonStreamConfig(buffer_entire_result=True))
        await parser.start_parsing()
        result = await parser.get_result()
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        with pytest.raises(AnalyticsError):
            await parser.get_result()

class JsonParsingTests(JsonParsingTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(JsonParsingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(JsonParsingTests) if valid_test_method(meth)]
        test_list = set(JsonParsingTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='async_test_env')
    def acouchbase_test_environment(self, simple_async_test_env: AsyncSimpleEnvironment) -> AsyncSimpleEnvironment:
        return simple_async_test_env