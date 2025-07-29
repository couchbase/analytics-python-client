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
from typing import TYPE_CHECKING

import pytest

from couchbase_analytics.common._core import JsonStreamConfig, ParsedResult, ParsedResultType
from couchbase_analytics.protocol._core.json_stream import JsonStream
from tests.environments.simple_environment import JsonDataType
from tests.utils import BytesIterator

if TYPE_CHECKING:
    from tests.environments.simple_environment import SimpleEnvironment


class JsonParsingTestSuite:
    TEST_MANIFEST = [
        'test_analytics_error',
        'test_analytics_error_mid_stream',
        'test_analytics_many_rows',
        'test_analytics_many_rows_raw',
        'test_analytics_multiple_errors',
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
    def test_analytics_error(self, test_env: SimpleEnvironment, buffered_result: bool) -> None:
        json_object, bytes_data = test_env.get_json_data(JsonDataType.FAILED_REQUEST)
        if buffered_result:
            parser = JsonStream(BytesIterator(bytes_data), stream_config=JsonStreamConfig(buffer_entire_result=True))
        else:
            parser = JsonStream(BytesIterator(bytes_data))
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.ERROR
        assert isinstance(result.value, bytes)
        assert json.loads(result.value.decode('utf-8')) == json_object
        assert parser.get_result(0.01) is None

    def test_analytics_error_mid_stream(self, test_env: SimpleEnvironment) -> None:
        json_object, bytes_data = test_env.get_json_data(JsonDataType.FAILED_REQUEST_MID_STREAM)
        parser = JsonStream(BytesIterator(bytes_data))
        parser.start_parsing()
        row_idx = 0
        while True:
            result = parser.get_result(0.01)
            if result is None and not parser.token_stream_exhausted:
                parser.continue_parsing()
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
        assert parser.get_result(0.01) is None

    def test_analytics_many_rows(self, test_env: SimpleEnvironment) -> None:
        json_object, bytes_data = test_env.get_json_data(JsonDataType.MULTIPLE_RESULTS)
        parser = JsonStream(BytesIterator(bytes_data))
        parser.start_parsing()
        row_idx = 0
        while row_idx < 36:
            result = parser.get_result(0.01)
            if result is None and not parser.token_stream_exhausted:
                parser.continue_parsing()
                continue
            assert isinstance(result, ParsedResult)
            assert result.result_type == ParsedResultType.ROW
            assert isinstance(result.value, bytes)
            assert json.loads(result.value.decode('utf-8')) == json_object['results'][row_idx]
            row_idx += 1

        final_result = parser.get_result(0.01)
        assert isinstance(final_result, ParsedResult)
        assert final_result.result_type == ParsedResultType.END
        assert isinstance(final_result.value, bytes)
        # if we are not buffering the entire result, the final result will exclude the results key
        json_object.pop('results')
        assert json.loads(final_result.value.decode('utf-8')) == json_object
        assert parser.get_result(0.01) is None

    @pytest.mark.parametrize('buffered_result', [True, False])
    def test_analytics_many_rows_raw(self, test_env: SimpleEnvironment, buffered_result: bool) -> None:
        json_object, bytes_data = test_env.get_json_data(JsonDataType.MULTIPLE_RESULTS_RAW)
        if buffered_result:
            parser = JsonStream(BytesIterator(bytes_data), stream_config=JsonStreamConfig(buffer_entire_result=True))
        else:
            parser = JsonStream(BytesIterator(bytes_data))

        parser.start_parsing()
        if not buffered_result:
            row_idx = 0
            while row_idx < 10:
                result = parser.get_result(0.01)
                if result is None and not parser.token_stream_exhausted:
                    parser.continue_parsing()
                    continue
                assert isinstance(result, ParsedResult)
                assert result.result_type == ParsedResultType.ROW
                assert isinstance(result.value, bytes)
                assert json.loads(result.value.decode('utf-8')) == json_object['results'][row_idx]
                row_idx += 1

        final_result = parser.get_result(0.01)
        assert isinstance(final_result, ParsedResult)
        assert final_result.result_type == ParsedResultType.END
        assert isinstance(final_result.value, bytes)
        if not buffered_result:
            # if we are not buffering the entire result, the final result will exclude the results key
            json_object.pop('results')
        assert json.loads(final_result.value.decode('utf-8')) == json_object
        assert parser.get_result(0.01) is None

    @pytest.mark.parametrize('buffered_result', [True, False])
    def test_analytics_multiple_errors(self, test_env: SimpleEnvironment, buffered_result: bool) -> None:
        json_object, bytes_data = test_env.get_json_data(JsonDataType.FAILED_REQUEST_MULTI_ERRORS)
        if buffered_result:
            parser = JsonStream(BytesIterator(bytes_data), stream_config=JsonStreamConfig(buffer_entire_result=True))
        else:
            parser = JsonStream(BytesIterator(bytes_data))
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.ERROR
        assert isinstance(result.value, bytes)
        assert json.loads(result.value.decode('utf-8')) == json_object
        assert parser.get_result(0.01) is None

    @pytest.mark.parametrize('buffered_result', [True, False])
    def test_analytics_simple_result(self, test_env: SimpleEnvironment, buffered_result: bool) -> None:
        json_object, bytes_data = test_env.get_json_data(JsonDataType.SIMPLE_REQUEST)
        if buffered_result:
            parser = JsonStream(BytesIterator(bytes_data), stream_config=JsonStreamConfig(buffer_entire_result=True))
        else:
            parser = JsonStream(BytesIterator(bytes_data))
        parser.start_parsing()
        # check for individual rows when not buffering the result
        if not buffered_result:
            result = parser.get_result(0.01)
            assert isinstance(result, ParsedResult)
            assert result.result_type == ParsedResultType.ROW
            assert isinstance(result.value, bytes)
            assert json.loads(result.value.decode('utf-8')) == json_object['results'][0]

        final_result = parser.get_result(0.01)
        assert isinstance(final_result, ParsedResult)
        assert final_result.result_type == ParsedResultType.END
        assert isinstance(final_result.value, bytes)
        # we don't store the 'results' if buffering is not enabled
        if not buffered_result:
            json_object.pop('results')
        assert json.loads(final_result.value.decode('utf-8')) == json_object
        assert parser.get_result(0.01) is None

    def test_array(self) -> None:
        data = '[1,2,"three"]'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_array_empty(self) -> None:
        data = '[]'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_array_mixed_types(self) -> None:
        data = '[123,"text",true,null,{"key":"value"}]'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_array_of_objects(self) -> None:
        data = '[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_invalid_empty(self) -> None:
        data = ''
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        res = parser.get_result(0.01)
        assert isinstance(res, ParsedResult)
        assert res.result_type == ParsedResultType.ERROR
        assert res.value is not None
        decoded_value = res.value.decode('utf-8')
        assert ('parse error' in decoded_value or 'Incomplete JSON content' in decoded_value) is True

    def test_invalid_garbage_between_objects(self) -> None:
        data = '[{"id":1,"name":"Alice"},garbage,{"id":2,"name":"Bob"}]'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        res = parser.get_result(0.01)
        assert isinstance(res, ParsedResult)
        assert res.result_type == ParsedResultType.ERROR
        assert res.value is not None
        decoded_value = res.value.decode('utf-8')
        assert ('lexical error' in decoded_value or 'Unexpected symbol' in decoded_value) is True

    def test_invalid_leading_garbage(self) -> None:
        data = 'garbage{"key":"value"}'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        res = parser.get_result(0.01)
        assert isinstance(res, ParsedResult)
        assert res.result_type == ParsedResultType.ERROR
        assert res.value is not None
        decoded_value = res.value.decode('utf-8')
        assert ('lexical error' in decoded_value or 'Unexpected symbol' in decoded_value) is True

    def test_invalid_trailing_garbage(self) -> None:
        data = '{"key":"value"}garbage'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        res = parser.get_result(0.01)
        assert isinstance(res, ParsedResult)
        assert res.result_type == ParsedResultType.ERROR
        assert res.value is not None
        decoded_value = res.value.decode('utf-8')
        assert ('parse error' in decoded_value or 'Additional data found' in decoded_value) is True

    def test_invalid_whitespace_only(self) -> None:
        data = '   \n\t  '
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        res = parser.get_result(0.01)
        assert isinstance(res, ParsedResult)
        assert res.result_type == ParsedResultType.ERROR
        assert res.value is not None
        decoded_value = res.value.decode('utf-8')
        assert ('parse error' in decoded_value or 'Incomplete JSON content' in decoded_value) is True

    def test_object(self) -> None:
        data = '{"name":"John","age":30,"city":"New York"}'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_object_complex_nested_structure(self) -> None:
        data_list = [
            '{"users":[{"id":1,"name":"Alice","roles":["admin","editor"]},{"id":2,"name":"Bob","roles":["viewer"]}],',
            '"meta":{"count":2,"status":"success"}}',
        ]
        data = ''.join(data_list)
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_object_empty(self) -> None:
        data = '{}'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_object_simple_nested(self) -> None:
        data = '{"outer":{"inner":{"key":"value"}}}'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_object_with_empty_key_and_value(self) -> None:
        data = '{"":""}'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_object_with_unicode(self) -> None:
        data = '{"name":"你好","city":"Denver"}'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_value_bool(self) -> None:
        data = 'true'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None

    def test_value_null(self) -> None:
        data = 'null'
        parser = JsonStream(
            BytesIterator(bytes(data, 'utf-8')), stream_config=JsonStreamConfig(buffer_entire_result=True)
        )
        parser.start_parsing()
        result = parser.get_result(0.01)
        assert isinstance(result, ParsedResult)
        assert result.result_type == ParsedResultType.END
        assert isinstance(result.value, bytes)
        assert result.value.decode('utf-8') == data
        assert parser.get_result(0.01) is None


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

    @pytest.fixture(scope='class', name='test_env')
    def couchbase_test_environment(self, simple_test_env: SimpleEnvironment) -> SimpleEnvironment:
        return simple_test_env
