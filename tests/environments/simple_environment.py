import json
from enum import Enum
from typing import Any, Tuple

import pytest

class JsonDataType(Enum):
    SIMPLE_REQUEST = 'simple_request'
    MULTIPLE_RESULTS = 'multiple_results'
    FAILED_REQUEST = 'failed_request'
    FAILED_REQUEST_MULTI_ERRORS = 'failed_request_multi_errors'
    FAILED_REQUEST_MID_STREAM = 'failed_request_mid_stream'

JSON_DATA = {
    'simple_request':"""
{
  "requestID": "98f69cf0-6d00-4a61-b8b6-e3b29fb6061b",
  "signature": {
    "*": "*"
  },
  "results": [
    {
      "greeting": "Hello, data!"
    }
  ],
  "plans": {},
  "status": "success",
  "metrics": {
    "elapsedTime": "60.13982ms",
    "executionTime": "56.765081ms",
    "compileTime": "3.244949ms",
    "queueWaitTime": "0ns",
    "resultCount": 1,
    "resultSize": 27,
    "processedObjects": 0
  }
}""".strip(),
    'multiple_results':"""
{
  "requestID": "94c7f89f-92b6-4aba-a90d-be715ca47309",
  "signature": {
    "*": "*"
  },
  "results": [
    {"id": 1, "name": "John Doe", "age": 30, "city": "New York"},
    {"id": 2, "name": "Jane Smith", "age": 25, "city": "Los Angeles"},
    {"id": 3, "name": "Sam Brown", "age": 22, "city": "Chicago"},
    {"id": 4, "name": "Lisa White", "age": 28, "city": "Houston"},
    {"id": 5, "name": "Tom Green", "age": 35, "city": "Phoenix"},
    {"id": 6, "name": "Anna Blue", "age": 27, "city": "Philadelphia"},
    {"id": 7, "name": "Mike Black", "age": 32, "city": "San Antonio"},
    {"id": 8, "name": "Sara Yellow", "age": 29, "city": "San Diego"},
    {"id": 9, "name": "Chris Red", "age": 31, "city": "Dallas"},
    {"id": 10, "name": "Kate Purple", "age": 26, "city": "San Jose"},
    {"id": 11, "name": "Paul Orange", "age": 33, "city": "Austin"},
    {"id": 12, "name": "Nina Pink", "age": 24, "city": "Jacksonville"},
    {"id": 13, "name": "Leo Grey", "age": 36, "city": "Fort Worth"},
    {"id": 14, "name": "Eva Cyan", "age": 23, "city": "Columbus"},
    {"id": 15, "name": "Zoe Brown", "age": 34, "city": "Charlotte"},
    {"id": 16, "name": "Liam Gold", "age": 21, "city": "San Francisco"},
    {"id": 17, "name": "Mia Silver", "age": 30, "city": "Indianapolis"},
    {"id": 18, "name": "Noah Bronze", "age": 25, "city": "Seattle"},
    {"id": 19, "name": "Olivia Copper", "age": 22, "city": "Denver"},
    {"id": 20, "name": "Ethan Steel", "age": 28, "city": "Washington"},
    {"id": 21, "name": "Sophia Iron", "age": 35, "city": "Boston"},
    {"id": 22, "name": "James Wood", "age": 27, "city": "El Paso"},
    {"id": 23, "name": "Ava Stone", "age": 32, "city": "Detroit"},
    {"id": 24, "name": "Lucas Clay", "age": 29, "city": "Nashville"},
    {"id": 25, "name": "Charlotte Brick", "age": 31, "city": "Baltimore"},
    {"id": 26, "name": "Benjamin Marble", "age": 26, "city": "Milwaukee"},
    {"id": 27, "name": "Amelia Slate", "age": 33, "city": "Albuquerque"},
    {"id": 28, "name": "Oliver Quartz", "age": 24, "city": "Tucson"},
    {"id": 29, "name": "Isabella Granite", "age": 36, "city": "Fresno"},
    {"id": 30, "name": "Elijah Onyx", "age": 23, "city": "Sacramento"},
    {"id": 31, "name": "Mason Jade", "age": 34, "city": "Long Beach"},
    {"id": 32, "name": "Charlotte Ruby", "age": 21, "city": "Kansas City"},
    {"id": 33, "name": "Aiden Sapphire", "age": 30, "city": "Mesa"},
    {"id": 34, "name": "Harper Emerald", "age": 25, "city": "Virginia Beach"},
    {"id": 35, "name": "Ella Amethyst", "age": 22, "city": "Atlanta"},
    {"id": 36, "name": "Liam Diamond", "age": 28, "city": "Colorado Springs"}
  ],
  "plans": {},
  "status": "success",
  "metrics": {
    "elapsedTime": "14.927542ms",
    "executionTime": "12.875792ms",
    "compileTime": "4.178042ms",
    "queueWaitTime": "0ns",
    "resultCount": 2,
    "resultSize": 300,
    "processedObjects": 2,
    "bufferCacheHitRatio": "100.00%"
  }
}""".strip(),
    'failed_request':"""
{
  "requestID": "c5f50c58-c044-481f-a26a-357a29f7446e",
  "errors": [
    {
      "code": 24000,
      "msg": "Syntax error: TokenMgrError: Lexical error at line 1, column 14.  Encountered: <EOF> after : \\"'m not N1QL;\\""
    }
  ],
  "status": "fatal",
  "metrics": {
    "elapsedTime": "3.146092ms",
    "executionTime": "1.907313ms",
    "compileTime": "0ns",
    "queueWaitTime": "0ns",
    "resultCount": 0,
    "resultSize": 0,
    "processedObjects": 0,
    "bufferCacheHitRatio": "0.00%",
    "bufferCachePageReadCount": 0,
    "errorCount": 1
  }
}""".strip(),
    'failed_request_multi_errors':"""
{
  "requestID": "c5f50c58-c044-481f-a26a-357a29f7446e",
  "errors": [
    {
      "code": 24000,
      "msg": "Syntax error: TokenMgrError: Lexical error at line 1, column 14.  Encountered: <EOF> after : \\"'m not N1QL;\\""
    },
    {
      "code": 20001,
      "msg": "Insufficient permissions or the requested object does not exist"
    }
  ],
  "status": "fatal",
  "metrics": {
    "elapsedTime": "3.146092ms",
    "executionTime": "1.907313ms",
    "compileTime": "0ns",
    "queueWaitTime": "0ns",
    "resultCount": 0,
    "resultSize": 0,
    "processedObjects": 0,
    "bufferCacheHitRatio": "0.00%",
    "bufferCachePageReadCount": 0,
    "errorCount": 2
  }
}""".strip(),
    'failed_request_mid_stream':"""
{
  "requestID": "c5f50c58-c044-481f-a26a-357a29f7446e",
  "results": [
    {"id": 1, "name": "John Doe", "age": 30, "city": "New York"},
    {"id": 2, "name": "Jane Smith", "age": 25, "city": "Los Angeles"}
  ],
  "errors": [
    {
      "code": 24000,
      "msg": "Syntax error: TokenMgrError: Lexical error at line 1, column 14.  Encountered: <EOF> after : \\"'m not N1QL;\\""
    }
  ],
  "status": "fatal",
  "metrics": {
    "elapsedTime": "3.146092ms",
    "executionTime": "1.907313ms",
    "compileTime": "0ns",
    "queueWaitTime": "0ns",
    "resultCount": 2,
    "resultSize": 2,
    "processedObjects": 2,
    "bufferCacheHitRatio": "0.00%",
    "bufferCachePageReadCount": 0,
    "errorCount": 2
  }
}""".strip()
}

class BaseSimpleEnvironment:
    def __init__(self, backend: str) -> None:
        self._backend = backend

    def get_json_data(self, json_type: JsonDataType) -> Tuple[Any, bytes]:
        """
        Retrieve JSON data by key.
        """
        key = json_type.value if isinstance(json_type, JsonDataType) else json_type
        if key not in JSON_DATA:
            raise KeyError(f"Key '{key}' not found in JSON data.")
        data = JSON_DATA[key]
        return json.loads(data), bytes(data, 'utf-8')

class AsyncSimpleEnvironment(BaseSimpleEnvironment):
    def __init__(self, backend: str) -> None:
        super().__init__(backend)

class SimpleEnvironment(BaseSimpleEnvironment):
    def __init__(self, backend: str) -> None:
        super().__init__(backend)

@pytest.fixture(scope='class', name='simple_async_test_env')
def simple_async_test_environment(anyio_backend: str) -> AsyncSimpleEnvironment:
    return AsyncSimpleEnvironment(anyio_backend)

@pytest.fixture(scope='class', name='simple_test_env')
def simple_test_environment(anyio_backend: str) -> SimpleEnvironment:
    return SimpleEnvironment(anyio_backend)