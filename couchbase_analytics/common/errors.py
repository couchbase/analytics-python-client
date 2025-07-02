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

from typing import Dict, Optional, Union

"""

Error Classes

"""


class AnalyticsError(Exception):
    """
    Generic base error.  Analytics specific errors inherit from this base error.
    """

    def __init__(self,
                 cause: Optional[Union[BaseException, Exception]] = None,
                 message: Optional[str] = None,
                 context: Optional[str] = None) -> None:
        self._cause = cause
        self._message = message
        self._context = context
        super().__init__(message)

    def _err_details(self) -> Dict[str, str]:
        details: Dict[str, str] = {}
        if self._context is not None:
            details['context'] = self._context
        if self._cause is not None:
            details['cause'] = self._cause.__repr__()
        if self._message is not None and not self._message.isspace():
            details['message'] = self._message
        return details

    def __repr__(self) -> str:
        details = self._err_details()
        if details:
            return f'{type(self).__name__}({details})'
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return self.__repr__()


class InvalidCredentialError(AnalyticsError):
    """
    Indicates that an error occurred authenticating the user to the cluster.
    """

    def __init__(self,
                 cause: Optional[Union[BaseException, Exception]] = None,
                 context: Optional[str] = None,
                 message: Optional[str] = None) -> None:
        super().__init__(cause=cause, context=context, message=message)

    def __repr__(self) -> str:
        details = self._err_details()
        if details:
            return f'{type(self).__name__}({details})'
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return self.__repr__()


class QueryError(AnalyticsError):
    """
    Indicates that an query request received an error from the Analytics server.
    """

    def __init__(self,
                 code: int,
                 server_message: str,
                 context: str,
                 message: Optional[str] = None) -> None:
        super().__init__(message=message, context=context)
        self._code = code
        self._server_message = server_message

    @property
    def code(self) -> int:
        """
        Returns:
            Error code from Analytics server
        """
        return self._code

    @property
    def server_message(self) -> str:
        """
        Returns:
            Error message from Analytics server
        """
        return self._server_message

    def __repr__(self) -> str:
        details: Dict[str, str] = {
            'code': str(self._code),
            'server_message': self._server_message,
            'context': self._context or ''
        }
        return f"{type(self).__name__}({details})"

    def __str__(self) -> str:
        return self.__repr__()


class TimeoutError(AnalyticsError):
    """
    Indicates that a request was unable to complete prior to reaching the deadline specified for the reqest.
    """

    def __init__(self,
                 cause: Optional[Union[BaseException, Exception]] = None,
                 context: Optional[str] = None,
                 message: Optional[str] = None) -> None:
        super().__init__(cause=cause, context=context, message=message)

    def __repr__(self) -> str:
        details = self._err_details()
        if details:
            return f'{type(self).__name__}({details})'
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return self.__repr__()


class FeatureUnavailableError(Exception):
    """
    Raised when feature that is not available with the current server version is used.
    """

    def __repr__(self) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    def __str__(self) -> str:
        return self.__repr__()


class InternalSDKError(Exception):
    """
    This means the SDK has done something wrong. Get support.
    (this doesn't mean *you* didn't do anything wrong, it does mean you should not be seeing this message)
    """

    def __init__(self,
                 cause: Optional[Union[BaseException, Exception]] = None,
                 context: Optional[str] = None,
                 message: Optional[str] = None) -> None:
        self._cause = cause
        self._message = message
        self._context = context
        super().__init__(message)

    def __repr__(self) -> str:
        details: Dict[str, str] = {}
        if self._context is not None:
            details['context'] = self._context
        if self._cause is not None:
            details['cause'] = self._cause.__repr__()
        if self._message is not None and not self._message.isspace():
            details['message'] = self._message
        if details:
            return f'{type(self).__name__}({details})'
        return f'{type(self).__name__}()'

    def __str__(self) -> str:
        return self.__repr__()
