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

import socket

from typing import Optional, TYPE_CHECKING
from uuid import uuid4

from httpx import BasicAuth, AsyncClient, Response

from couchbase_analytics.common.credential import Credential
from couchbase_analytics.common.deserializer import Deserializer
from couchbase_analytics.protocol.connection import _ConnectionDetails
from couchbase_analytics.protocol.options import OptionsBuilder

if TYPE_CHECKING:
    from couchbase_analytics.protocol.core.request import QueryRequest


class _AsyncClientAdapter:
    """
        **INTERNAL**
    """

    _ANALYTICS_PATH = '/api/v1/request'

    def __init__(self,
                 http_endpoint: str,
                 credential: Credential,
                 options: Optional[object] = None,
                 **kwargs: object) -> None:
        self._client_id = str(uuid4())
        self._opts_builder = OptionsBuilder()
        self._conn_details = _ConnectionDetails.create(self._opts_builder,
                                                       http_endpoint,
                                                       credential,
                                                       options,
                                                       **kwargs)
        # TODO:  do we want to support custom HTTP transports for the async client?
        self._http_transport_cls = None

    @property
    def analytics_path(self) -> str:
        """
            **INTERNAL**
        """
        return self._ANALYTICS_PATH 
    
    @property
    def client(self) -> AsyncClient:
        """
            **INTERNAL**
        """
        return self._client
    
    @property
    def client_id(self) -> str:
        """
            **INTERNAL**
        """
        return self._client_id

    @property
    def connection_details(self) -> _ConnectionDetails:
        """
            **INTERNAL**
        """
        return self._conn_details

    @property
    def default_deserializer(self) -> Deserializer:
        """
            **INTERNAL**
        """
        return self._conn_details.default_deserializer
    
    @property
    def has_client(self) -> bool:
        """
            **INTERNAL**
        """
        return hasattr(self, '_client')

    @property
    def options_builder(self) -> OptionsBuilder:
        """
            **INTERNAL**
        """
        return self._opts_builder

    async def close_client(self) -> None:
        """
            **INTERNAL**
        """
        if hasattr(self, '_client'):
            await self._client.aclose()

    async def create_client(self) -> None:
        """
            **INTERNAL**
        """
        if not hasattr(self, '_client'):
            if self._conn_details.is_secure():
                if self._conn_details.ssl_context is None:
                    raise ValueError('SSL context is required for secure connections.')
                transport = None
                if self._http_transport_cls is not None:
                    transport = self._http_transport_cls(verify=self._conn_details.ssl_context)
                self._client = AsyncClient(verify=self._conn_details.ssl_context,
                                           auth=BasicAuth(*self._conn_details.credential),
                                           transport=transport)
            else:
                transport = None
                if self._http_transport_cls is not None:
                    transport = self._http_transport_cls()
                self._client = AsyncClient(auth=BasicAuth(*self._conn_details.credential),
                                           transport=transport)
            # TODO: log message


    async def send_request(self, request: QueryRequest) -> Response:
        """
            **INTERNAL**
        """
        if not hasattr(self, '_client'):
            raise RuntimeError('Client not created yet')
        
        if request.url is None:
            raise ValueError('Request URL cannot be None')

        req = self._client.build_request(request.method,
                                         request.url,
                                         json=request.body,
                                         extensions=request.extensions)
        try:
            return await self._client.send(req, stream=True)
        except socket.gaierror as err:
            raise RuntimeError(f'Unable to connect to {self._conn_details.get_scheme_host_and_port()}') from err

    def reset_client(self) -> None:
        """
            **INTERNAL**
        """
        if hasattr(self, '_client'):
            del self._client

