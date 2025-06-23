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

from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Coroutine,
                    Dict,
                    Optional,
                    Set,
                    Tuple,
                    TypedDict,
                    Union)
from uuid import uuid4

from couchbase_analytics.common.deserializer import Deserializer
from couchbase_analytics.common.options import QueryOptions
from couchbase_analytics.protocol.options import QueryOptionsTransformedKwargs
from couchbase_analytics.query import QueryScanConsistency

if TYPE_CHECKING:
    from httpx import Response as HttpCoreResponse

    from acouchbase_analytics.protocol.core.client_adapter import _AsyncClientAdapter as AsyncClientAdapter
    from couchbase_analytics.protocol.core.client_adapter import _ClientAdapter as BlockingClientAdapter

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
    scheme: str
    host: str
    port: int
    deserializer: Deserializer
    body: Dict[str, Union[str, object]]
    extensions: RequestExtensions
    method: str = 'POST'
    url: Optional[str] = None
    
    options: Optional[QueryOptionsTransformedKwargs] = None
    client_addr: Optional[Tuple[str, int]] = None
    server_addr: Optional[Tuple[str, int]] = None
    previous_ips: Optional[Set[str]] = None
    response_status_code: Optional[int] = None
    enable_cancel: Optional[bool] = None

    def get_request_timeouts(self) -> Optional[RequestTimeoutExtensions]:
        """
        **INTERNAL**
        Get the request timeouts from the extensions.
        Returns:
            Dict[str, int]: The request timeouts.
        """
        if self.extensions is None or 'timeout' not in self.extensions:
            return {}
        return self.extensions['timeout']

    def set_client_server_addrs(self, response: HttpCoreResponse) -> None:
        network_stream = response.extensions.get('network_stream', None)
        # TODO: what if network_stream is None?
        if network_stream is not None:
            self.client_addr = network_stream.get_extra_info('client_addr')
            self.server_addr = network_stream.get_extra_info('server_addr')
        
        self.response_status_code = response.status_code

    def add_trace_to_extensions(self, handler: Callable[[str, str], Union[None, Coroutine[Any, Any, None]]]) -> QueryRequest:
        """
        **INTERNAL**
        Update the extensions of the request.
        Args:
            new_extensions (Dict[str, str]): The new extension(s) to add.
        """
        if self.extensions is None:
            self.extensions = {}
        self.extensions['trace'] = handler
        return self

    def update_previous_ips(self, ip: str) -> QueryRequest:
        """
        **INTERNAL**
        Update the previous IPs of the request.
        Args:
            ip (str): The new IP to add to the previous IPs.
        """
        if self.previous_ips is None:
            self.previous_ips = set()
        self.previous_ips.add(ip)
        return self

    def update_url(self, ip: str, path: str) -> QueryRequest:
        """
        **INTERNAL**
        Update the URL of the request.
        Args:
            new_url (str): The new URL to set.
        """
        self.url = f'{self.scheme}://{ip}:{self.port}{path}'
        return self


class _RequestBuilder:

    def __init__(self,
                 client: Union[AsyncClientAdapter, BlockingClientAdapter],
                 database_name: Optional[str]=None,
                 scope_name: Optional[str]=None
                 ) -> None:
        self._conn_details = client.connection_details
        self._opts_builder = client.options_builder
        self._database_name = database_name
        self._scope_name = scope_name
        
        connect_timeout = self._conn_details.get_connect_timeout()
        self._default_query_timeout = self._conn_details.get_query_timeout()
        self._extensions: RequestExtensions = {
            'timeout': {
                'pool': connect_timeout,
                'connect': connect_timeout,
                'read': self._default_query_timeout
            }
        }
        # TODO:  warning if we have a secure connection, but the sni_hostname is not set?
        if self._conn_details.is_secure() and self._conn_details.sni_hostname is not None:
            self._extensions['sni_hostname'] = self._conn_details.sni_hostname


    def build_base_query_request(self,  # noqa: C901
                                 statement: str,
                                 *args: object,
                                 is_async: Optional[bool] = False,
                                 **kwargs: object) -> QueryRequest:  # noqa: C901
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

        q_opts = self._opts_builder.build_options(QueryOptions,
                                                  QueryOptionsTransformedKwargs,
                                                  kwargs,
                                                  opts)
        # positional params and named params passed in outside of QueryOptions serve as overrides
        if parsed_args_list and len(parsed_args_list) > 0:
            q_opts['positional_parameters'] = parsed_args_list
        if named_params and len(named_params) > 0:
            q_opts['named_parameters'] = named_params
        # add the default serializer if one does not exist
        deserializer = q_opts.pop('deserializer', None) or self._conn_details.default_deserializer

        body: Dict[str, Union[str, object]] = {
            'statement': statement,
            'client_context_id': q_opts.get('client_context_id', None) or str(uuid4())
        }
        
        if self._database_name is not None and self._scope_name is not None:
            body['query_context'] = f'default:`{self._database_name}`.`{self._scope_name}`'

        # handle timeouts
        timeout = q_opts.get('timeout', None) or self._default_query_timeout
        extensions = deepcopy(self._extensions)
        if (timeout is not None
            and timeout != self._default_query_timeout):
            extensions['timeout']['read'] = timeout
        # in the async world we have our own cancel scope that handles the connect timeout
        if is_async:
            del extensions['timeout']['pool']
            del extensions['timeout']['connect']
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
                body['args'] = [arg for arg in opt_val]  # type: ignore[attr-defined]
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

        scheme, host, port = self._conn_details.get_scheme_host_and_port()
        return QueryRequest(scheme,
                            host,
                            port,
                            deserializer,
                            body,
                            extensions=extensions,
                            options=q_opts,
                            enable_cancel=enable_cancel)
