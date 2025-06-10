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

import ssl

from dataclasses import dataclass
from typing import (TYPE_CHECKING,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    TypedDict)
from urllib.parse import parse_qs, urlparse

from couchbase_analytics.common.credential import Credential
from couchbase_analytics.common.deserializer import DefaultJsonDeserializer, Deserializer
from couchbase_analytics.common.options import ClusterOptions
from couchbase_analytics.protocol import PYCBAC_VERSION
from couchbase_analytics.protocol.options import (ClusterOptionsTransformedKwargs,
                                                  QueryStrVal,
                                                  SecurityOptionsTransformedKwargs,
                                                  TimeoutOptionsTransformedKwargs)

from httpcore import (Origin, URL)

if TYPE_CHECKING:
    from couchbase_analytics.protocol.options import OptionsBuilder


class StreamingTimeouts(TypedDict, total=False):
    query_timeout: Optional[int]



class DefaultTimeouts(TypedDict):
    connect_timeout: int
    dispatch_timeout: int
    query_timeout: int

DEFAULT_TIMEOUTS: DefaultTimeouts = {
    'connect_timeout': 10,
    'dispatch_timeout': 30,
    'query_timeout': 60 * 10,
}


def parse_http_endpoint(http_endpoint: str) -> Tuple[URL, Dict[str, QueryStrVal]]:
    """ **INTERNAL**

    Parse the provided HTTP endpoint

    The provided connection string will be parsed to split the connection string
    and the the query options.  Query options will be split into legacy options
    and 'current' options.

    Args:
        http_endpoint (str): The HTTP endpoint to use for requests.

    Returns:
        Tuple[str, Dict[str, Any], Dict[str, Any]]: The parsed HTTP URL and options dict.
    """
    parsed_endpoint = urlparse(http_endpoint)
    if parsed_endpoint.scheme is None or parsed_endpoint.scheme not in ['http', 'https']:
        raise ValueError(f"The endpoint scheme must be 'http[s]'.  Found: {parsed_endpoint.scheme}.")

    port = parsed_endpoint.port
    if parsed_endpoint.port is None:
        port = 80 if parsed_endpoint.scheme == 'http' else 443
        

    url = URL(scheme=parsed_endpoint.scheme,
              host=parsed_endpoint.hostname,
              port=port,
              target=parsed_endpoint.path or '/')

    return url, parse_query_string_options(parsed_endpoint.query)


def parse_query_string_options(query_str: str) -> Dict[str, QueryStrVal]:
    """Parse the query string options

    Query options will be split into legacy options and 'current' options. The values for the
    'current' options are cast to integers or booleans where applicable

    Args:
        query_str (str): The query string.

    Returns:
        Tuple[Dict[str, QueryStrVal], Dict[str, QueryStrVal]]: The parsed current options and legacy options.
    """
    options = parse_qs(query_str)

    query_str_opts: Dict[str, QueryStrVal] = {}
    for k, v in options.items():
        query_str_opts[k] = parse_query_string_value(v)

    return query_str_opts


def parse_query_string_value(value: List[str]) -> QueryStrVal:
    """Parse a query string value

    The provided value is a list of at least one element. Returns either a list of strings or a single element
    which might be cast to an integer or a boolean if that's appropriate.

    Args:
        value (List[str]): The query string value.

    Returns:
        Union[List[str], str, bool, int]: The parsed current options and legacy options.
    """

    if len(value) > 1:
        return value
    v = value[0]
    if v.isnumeric():
        return int(v)
    elif v.lower() in ['true', 'false']:
        return v.lower() == 'true'
    return v


def parse_query_str_options(query_str_opts: Dict[str, QueryStrVal]) -> Dict[str, QueryStrVal]:
    final_opts: Dict[str, QueryStrVal] = {}
    for k, v in query_str_opts.items():
        tokens = k.split('.')
        if len(tokens) == 2:
            if tokens[0] in ['timeout', 'security']:
                final_opts[tokens[1]] = v
            else:
                # TODO:  exceptions -- this means the user passed in an invalid option
                pass 
        else:
            final_opts[k] = v

    return final_opts


@dataclass
class _ConnectionDetails:
    """
    **INTERNAL**
    """
    url: URL
    cluster_options: ClusterOptionsTransformedKwargs
    credential: Tuple[bytes, bytes]
    default_deserializer: Deserializer
    ssl_context: Optional[ssl.SSLContext] = None
    sni_hostname: Optional[str] = None
    
    def get_connect_timeout(self) -> int:
        timeout_opts: Optional[TimeoutOptionsTransformedKwargs] = self.cluster_options.get('timeout_options')
        if timeout_opts is not None:
            connect_timeout = timeout_opts.get('connect_timeout', None)
            if connect_timeout is not None:
                return connect_timeout
        return DEFAULT_TIMEOUTS['connect_timeout']
    
    def get_query_timeout(self) -> int:
        timeout_opts: Optional[TimeoutOptionsTransformedKwargs] = self.cluster_options.get('timeout_options')
        if timeout_opts is not None:
            query_timeout = timeout_opts.get('query_timeout', None)
            if query_timeout is not None:
                return query_timeout
        return DEFAULT_TIMEOUTS['query_timeout']

    def get_scheme_host_and_port(self) -> Tuple[str, str, int]:
        return self.url.scheme.decode(), self.url.host.decode(), self.url.port

    def is_secure(self) -> bool:
        return self.url.scheme.decode() == 'https'

    def validate_security_options(self) -> None:
        security_opts: Optional[SecurityOptionsTransformedKwargs] = self.cluster_options.get('security_options')
        # TODO: security settings
        if security_opts is not None:
            # separate between value options and boolean option (trust_only_capella)
            solo_security_opts = ['trust_only_pem_file',
                                'trust_only_pem_str',
                                'trust_only_certificates']
            trust_capella = security_opts.get('trust_only_capella', None)
            security_opt_count = sum(map(lambda k: 1 if security_opts.get(k, None) is not None else 0, solo_security_opts))
            if security_opt_count > 1 or (security_opt_count == 1 and trust_capella is True):
                raise ValueError(('Can only set one of the following options: '
                                f'[{", ".join(["trust_only_capella"] + solo_security_opts)}]'))
        
        if self.is_secure():
            self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            self.ssl_context.set_default_verify_paths()
            # ssl_context.load_verify_locations(cafile='.vscode/tls/cluster_ca.pem')
            self.ssl_context.load_verify_locations(cafile='.vscode/tls/capella.pem')
            self.ssl_context.load_verify_locations(cafile='.vscode/tls/dinocluster.pem')
            self.ssl_context.load_verify_locations(cafile='.vscode/tls/dinoca.pem')
            self.sni_hostname = self.url.host.decode()

    @classmethod
    def create(cls,
               opts_builder: OptionsBuilder,
               http_endpoint: str,
               credential: Credential,
               options: Optional[object] = None,
               **kwargs: object) -> _ConnectionDetails:
        url, query_str_opts = parse_http_endpoint(http_endpoint)

        cluster_opts = opts_builder.build_cluster_options(ClusterOptions,
                                                          ClusterOptionsTransformedKwargs,
                                                          kwargs,
                                                          options,
                                                          query_str_opts=parse_query_str_options(query_str_opts))

        default_deserializer = cluster_opts.pop('deserializer', None)
        if default_deserializer is None:
            default_deserializer = DefaultJsonDeserializer()

        conn_dtls = cls(url,
                        cluster_opts,
                        credential.astuple(),
                        default_deserializer)
        conn_dtls.validate_security_options()
        return conn_dtls
