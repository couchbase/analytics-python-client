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

from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Dict, Optional, TypedDict, Union, cast
from uuid import uuid4

from couchbase_analytics.common.deserializer import Deserializer
from couchbase_analytics.common.options import QueryOptions
from couchbase_analytics.common.request import RequestURL
from couchbase_analytics.protocol.options import QueryOptionsTransformedKwargs
from couchbase_analytics.query import QueryScanConsistency

if TYPE_CHECKING:
    from acouchbase_analytics.protocol._core.client_adapter import _AsyncClientAdapter as AsyncClientAdapter
    from couchbase_analytics.protocol._core.client_adapter import _ClientAdapter as BlockingClientAdapter


class RequestTimeoutExtensions(TypedDict, total=False):
    pool: Optional[float]  # Timeout for acquiring a connection from the pool
    connect: Optional[float]  # Timeout for establishing a socket connection
    read: Optional[float]  # Timeout for reading data from the socket connection
    write: Optional[float]  # Timeout for writing data to the socket connection


class RequestExtensions(TypedDict, total=False):
    timeout: RequestTimeoutExtensions
    sni_hostname: Optional[str]
    trace: Optional[Callable[[str, str], Union[None, Coroutine[Any, Any, None]]]]


@dataclass
class QueryRequest:
    url: RequestURL
    deserializer: Deserializer
    body: Dict[str, Union[str, object]]
    extensions: RequestExtensions
    max_retries: int
    method: str = 'POST'

    options: Optional[QueryOptionsTransformedKwargs] = None
    enable_cancel: Optional[bool] = None

    def add_trace_to_extensions(
        self, handler: Callable[[str, str], Union[None, Coroutine[Any, Any, None]]]
    ) -> QueryRequest:
        """
        **INTERNAL**
        """
        if self.extensions is None:
            self.extensions = {}
        self.extensions['trace'] = handler
        return self

    def get_request_statement(self) -> Optional[str]:
        """
        **INTERNAL**
        """
        if 'statement' in self.body:
            return cast(str, self.body['statement'])
        return None

    def get_request_timeouts(self) -> Optional[RequestTimeoutExtensions]:
        """
        **INTERNAL**
        """
        if self.extensions is None or 'timeout' not in self.extensions:
            return {}
        return self.extensions['timeout']

    def update_url(self, ip: str, path: str) -> QueryRequest:
        """
        **INTERNAL**
        """
        self.url.ip = ip
        self.url.path = path
        return self


class _RequestBuilder:
    def __init__(
        self,
        client: Union[AsyncClientAdapter, BlockingClientAdapter],
        database_name: Optional[str] = None,
        scope_name: Optional[str] = None,
    ) -> None:
        self._conn_details = client.connection_details
        self._opts_builder = client.options_builder
        self._database_name = database_name
        self._scope_name = scope_name

        connect_timeout = self._conn_details.get_connect_timeout()
        self._default_query_timeout = self._conn_details.get_query_timeout()
        self._extensions: RequestExtensions = {
            'timeout': {'pool': connect_timeout, 'connect': connect_timeout, 'read': self._default_query_timeout}
        }
        if self._conn_details.is_secure() and self._conn_details.sni_hostname is not None:
            self._extensions['sni_hostname'] = self._conn_details.sni_hostname

    def build_base_query_request(  # noqa: C901
        self,
        statement: str,
        *args: object,
        is_async: Optional[bool] = False,
        **kwargs: object,
    ) -> QueryRequest:  # noqa: C901
        enable_cancel: Optional[bool] = None
        cancel_kwarg_token = kwargs.pop('enable_cancel', None)
        if isinstance(cancel_kwarg_token, bool):
            enable_cancel = cancel_kwarg_token

        # default if no options provided
        opts = QueryOptions()
        args_list = list(args)
        parsed_args_list = []
        for arg in args_list:
            if isinstance(arg, QueryOptions):
                # we have options passed in
                opts = arg
            elif enable_cancel is None and isinstance(arg, bool):
                enable_cancel = arg
            else:
                parsed_args_list.append(arg)

        # need to pop out named params prior to sending options to the builder
        named_param_keys = list(filter(lambda k: k not in QueryOptions.VALID_OPTION_KEYS, kwargs.keys()))
        named_params = {}
        for key in named_param_keys:
            named_params[key] = kwargs.pop(key)

        q_opts = self._opts_builder.build_options(QueryOptions, QueryOptionsTransformedKwargs, kwargs, opts)
        # positional params and named params passed in outside of QueryOptions serve as overrides
        if parsed_args_list and len(parsed_args_list) > 0:
            q_opts['positional_parameters'] = parsed_args_list
        if named_params and len(named_params) > 0:
            q_opts['named_parameters'] = named_params
        # handle deserializer and max_retries
        deserializer = q_opts.pop('deserializer', None) or self._conn_details.default_deserializer
        max_retries = q_opts.pop('max_retries', None) or self._conn_details.get_max_retries()

        body: Dict[str, Union[str, object]] = {
            'statement': statement,
            'client_context_id': q_opts.get('client_context_id', None) or str(uuid4()),
        }

        if self._database_name is not None and self._scope_name is not None:
            body['query_context'] = f'default:`{self._database_name}`.`{self._scope_name}`'

        # handle timeouts
        timeout = q_opts.get('timeout', None) or self._default_query_timeout
        extensions = deepcopy(self._extensions)
        if timeout is not None and timeout != self._default_query_timeout:
            extensions['timeout']['read'] = timeout
        # we add 5 seconds to the server timeout to ensure we always trigger a client side timeout
        timeout_ms = (timeout + 5) * 1e3  # convert to milliseconds
        body['timeout'] = f'{timeout_ms}ms'

        for opt_key, opt_val in q_opts.items():
            if opt_key == 'deserializer':
                continue
            elif opt_key == 'raw':
                for k, v in opt_val.items():  # type: ignore[attr-defined]
                    body[k] = v
            elif opt_key == 'positional_parameters':
                body['args'] = list(opt_val)  # type: ignore[call-overload]
            elif opt_key == 'named_parameters':
                for k, v in opt_val.items():  # type: ignore[attr-defined]
                    key = f'${k}' if not k.startswith('$') else k
                    body[key] = v
            elif opt_key == 'readonly':
                body['readonly'] = opt_val
            elif opt_key == 'scan_consistency':
                if isinstance(opt_val, QueryScanConsistency):
                    body['scan_consistency'] = opt_val.value
                else:
                    body['scan_consistency'] = opt_val

        return QueryRequest(
            self._conn_details.url,
            deserializer,
            body,
            extensions=extensions,
            max_retries=max_retries,
            options=q_opts,
            enable_cancel=enable_cancel,
        )
