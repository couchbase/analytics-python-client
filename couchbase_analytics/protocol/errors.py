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
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Union)

if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias
else:
    from typing import TypeAlias

from couchbase_analytics.common.errors import (AnalyticsError,
                                              InternalSDKError,
                                              InvalidCredentialError,
                                              QueryError)

AnalyticsClientError: TypeAlias = Union[AnalyticsError,
                                        InternalSDKError,
                                        QueryError,
                                        RuntimeError,
                                        ValueError]

# class CoreErrorMap(Enum):
#     AnalyticsError = 1
#     InvalidCredentialError = 2
#     TimeoutError = 3
#     QueryError = 4


# class ClientErrorMap(Enum):
#     ValueError = 1
#     RuntimeError = 2
#     QueryOperationCanceledError = 3
#     InternalSDKError = 4


# PYCBAC_CORE_ERROR_MAP: Dict[int, type[AnalyticsError]] = {
#     e.value: getattr(sys.modules['couchbase_analytics.common.errors'], e.name) for e in CoreErrorMap
# }

# PYCBAC_CLIENT_ERROR_MAP: Dict[int, type[AnalyticsClientError]] = {
#     1: ValueError,
#     2: RuntimeError,
#     3: QueryOperationCanceledError,
#     4: InternalSDKError
# }


class ErrorMapper:

    @staticmethod  # noqa: C901
    def build_error_from_json(json_data: List[Dict[str, Any]],
                              status_code: Optional[int]=None) -> AnalyticsClientError:
        context = {'errors': json_data,
                   'http_status': status_code}
        # TODO: error handling needs to be more robust
        if status_code is None:
            status_code = json_data[0].get('status', 500)
            return AnalyticsError(message='Unknown error occurred.')
        elif status_code == 401:
            return InvalidCredentialError(str(context), message='Invalid credentials provided.')
        else:
            first_error = json_data[0]
            code = first_error.get('code', 0)
            server_message = first_error.get('msg', 'Unknown error occurred.')
            return QueryError(code, server_message, str(context))


