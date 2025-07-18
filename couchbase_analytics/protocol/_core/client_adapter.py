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

import logging
from typing import TYPE_CHECKING, Optional, cast
from uuid import uuid4

from httpx import URL, BasicAuth, Client, Response

from couchbase_analytics.common.credential import Credential
from couchbase_analytics.common.deserializer import Deserializer
from couchbase_analytics.common.logging import LogLevel, log_message
from couchbase_analytics.protocol.connection import _ConnectionDetails
from couchbase_analytics.protocol.options import OptionsBuilder

# from couchbase_analytics.protocol.core._http_transport import AnalyticsHTTPTransport

if TYPE_CHECKING:
    from couchbase_analytics.protocol._core.request import QueryRequest


class _ClientAdapter:
    """
    **INTERNAL**
    """

    ANALYTICS_PATH = '/api/v1/request'
    LOGGER_NAME = 'couchbase_analytics'

    def __init__(
        self, http_endpoint: str, credential: Credential, options: Optional[object] = None, **kwargs: object
    ) -> None:
        self._client_id = str(uuid4())
        self._prefix = ''
        self._cluster_id = cast(str, kwargs.pop('cluster_id', ''))
        self._opts_builder = OptionsBuilder()
        # TODO:  We should limit the allowed transports to the ones we support
        #        Question is how do we want to limit the transports?  Should users even need to override?
        # self._http_transport_cls = kwargs.pop('http_transport_cls', AnalyticsHTTPTransport)
        self._http_transport_cls = None
        kwargs['logger_name'] = self.logger_name
        self._conn_details = _ConnectionDetails.create(self._opts_builder, http_endpoint, credential, options, **kwargs)

    @property
    def analytics_path(self) -> str:
        """
        **INTERNAL**
        """
        return self.ANALYTICS_PATH

    @property
    def client(self) -> Client:
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
    def log_prefix(self) -> str:
        """
        **INTERNAL**
        """
        if self._prefix:
            return self._prefix
        self._prefix = f'[{self._cluster_id}'
        if self.has_client:
            self._prefix += f'/{self._client_id}'
            if self.connection_details.is_secure():
                self._prefix += '/https]'
            else:
                self._prefix += '/http]'

        return self._prefix

    @property
    def logger_name(self) -> str:
        """
        **INTERNAL**
        """
        return self.LOGGER_NAME

    @property
    def options_builder(self) -> OptionsBuilder:
        """
        **INTERNAL**
        """
        return self._opts_builder

    def close_client(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_client'):
            self._client.close()
            self.log_message('Cluster HTTP client closed', LogLevel.INFO)

    def create_client(self) -> None:
        """
        **INTERNAL**
        """
        if not hasattr(self, '_client'):
            auth = BasicAuth(*self._conn_details.credential)
            if self._conn_details.is_secure():
                if self._conn_details.ssl_context is None:
                    raise ValueError('SSL context is required for secure connections.')
                transport = None
                if self._http_transport_cls is not None:
                    transport = self._http_transport_cls(verify=self._conn_details.ssl_context)
                self._client = Client(verify=self._conn_details.ssl_context, auth=auth, transport=transport)
            else:
                transport = None
                if self._http_transport_cls is not None:
                    transport = self._http_transport_cls()
                self._client = Client(auth=auth, transport=transport)

            self.log_message(
                (f'Cluster HTTP client created: connection_details={self._conn_details.get_init_details()}'),
                LogLevel.INFO,
            )
        else:
            self.log_message('Cluster HTTP client already exists, skipping creation.', LogLevel.INFO)

    def log_message(self, message: str, log_level: LogLevel) -> None:
        log_message(logger, f'{self.log_prefix} {message}', log_level)

    def send_request(self, request: QueryRequest) -> Response:
        """
        **INTERNAL**
        """
        if not hasattr(self, '_client'):
            raise RuntimeError('Client not created yet')

        url = URL(scheme=request.url.scheme, host=request.url.ip, port=request.url.port, path=request.url.path)
        req = self._client.build_request(request.method, url, json=request.body, extensions=request.extensions)
        return self._client.send(req, stream=True)

    def reset_client(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_client'):
            del self._client


logger = logging.getLogger(_ClientAdapter.LOGGER_NAME)
