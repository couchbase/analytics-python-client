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

import logging
from typing import Optional, cast
from uuid import uuid4

from httpx import URL, Client, Response

from couchbase_analytics.common.credential import Credential, _CredentialHolder
from couchbase_analytics.common.deserializer import Deserializer
from couchbase_analytics.common.logging import LogLevel, log_message
from couchbase_analytics.protocol._core.auth import DynamicCredentialAuth
from couchbase_analytics.protocol._core.request import HttpRequest
from couchbase_analytics.protocol.connection import _ConnectionDetails
from couchbase_analytics.protocol.options import OptionsBuilder


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
        # PYCO-67:  Do we want to allow supporting custom HTTP transports?
        self._http_transport_cls = None
        kwargs['logger_name'] = self.logger_name
        self._conn_details = _ConnectionDetails.create(self._opts_builder, http_endpoint, credential, options, **kwargs)
        self._credential_holder = _CredentialHolder(credential)

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
    def credential_holder(self) -> _CredentialHolder:
        """
        **INTERNAL**
        """
        return self._credential_holder

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

    def _build_client(self) -> Client:
        auth = DynamicCredentialAuth(self._credential_holder)
        if self._conn_details.is_secure():
            if self._conn_details.ssl_context is None:
                raise ValueError('SSL context is required for secure connections.')
            transport = None
            if self._http_transport_cls is not None:
                transport = self._http_transport_cls(verify=self._conn_details.ssl_context)
            return Client(verify=self._conn_details.ssl_context, auth=auth, transport=transport)
        transport = None
        if self._http_transport_cls is not None:
            transport = self._http_transport_cls()
        return Client(auth=auth, transport=transport)

    def create_client(self) -> None:
        """
        **INTERNAL**
        """
        if not hasattr(self, '_client'):
            self._client = self._build_client()
            self.log_message(
                (f'Cluster HTTP client created: connection_details={self._conn_details.get_init_details()}'),
                LogLevel.INFO,
            )
        else:
            self.log_message('Cluster HTTP client already exists, skipping creation.', LogLevel.INFO)

    def log_message(self, message: str, log_level: LogLevel) -> None:
        log_message(logger, f'{self.log_prefix} {message}', log_level)

    def send_request(self, request: HttpRequest, stream: Optional[bool] = True) -> Response:
        """
        **INTERNAL**
        """
        if not hasattr(self, '_client'):
            raise RuntimeError('Client not created yet')

        url = URL(scheme=request.url.scheme, host=request.url.ip, port=request.url.port, path=request.url.path)
        req = self._client.build_request(
            request.method,
            url,
            data=request.data,
            json=request.body,
            headers=request.headers,
            extensions=request.extensions,
        )

        if stream is None:
            stream = True
        return self._client.send(req, stream=stream)

    def reset_client(self) -> None:
        """
        **INTERNAL**
        """
        if hasattr(self, '_client'):
            del self._client

    def update_credential(self, new_credential: Credential) -> None:
        if new_credential._kind == 'cert':
            # httpx pins the SSL context to the Client at construction, and
            # the cert chain is part of that context.  So a cert rotation
            # needs a fresh Client.  Build it before closing the old one,
            # otherwise a concurrent send_request can see self._client gone.
            self._credential_holder.credential._check_replaceable_with(new_credential)
            old_client = getattr(self, '_client', None)
            old_ssl_context = self._conn_details.ssl_context
            old_sni_hostname = self._conn_details.sni_hostname
            try:
                self._conn_details.validate_security_options(new_credential)
                # If the cluster hasn't issued a request yet there's no Client
                # to swap; we still refreshed the SSL context above.
                new_client = self._build_client() if old_client is not None else None
            except Exception:
                self._conn_details.ssl_context = old_ssl_context
                self._conn_details.sni_hostname = old_sni_hostname
                raise
            if new_client is not None:
                self._client = new_client
            self._credential_holder.replace(new_credential)
            if old_client is not None:
                old_client.close()
        else:
            self._credential_holder.replace(new_credential)
        self.log_message('Cluster HTTP credential updated', LogLevel.INFO)


logger = logging.getLogger(_ClientAdapter.LOGGER_NAME)
