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
                    TypedDict,
                    cast)
from urllib.parse import parse_qs, urlparse

from couchbase_analytics.common._core._certificates import _Certificates
from couchbase_analytics.common._core.duration_str_utils import parse_duration_str
from couchbase_analytics.common._core.utils import is_null_or_empty
from couchbase_analytics.common.credential import Credential
from couchbase_analytics.common.deserializer import DefaultJsonDeserializer, Deserializer
from couchbase_analytics.common.options import (ClusterOptions,
                                                SecurityOptions,
                                                TimeoutOptions)
from couchbase_analytics.common.request import RequestURL
from couchbase_analytics.protocol.options import (ClusterOptionsTransformedKwargs,
                                                  QueryStrVal,
                                                  SecurityOptionsTransformedKwargs,
                                                  TimeoutOptionsTransformedKwargs)

if TYPE_CHECKING:
    from couchbase_analytics.protocol.options import OptionsBuilder


class StreamingTimeouts(TypedDict, total=False):
    query_timeout: Optional[float]


class DefaultTimeouts(TypedDict):
    connect_timeout: float
    query_timeout: float


DEFAULT_TIMEOUTS: DefaultTimeouts = {
    'connect_timeout': 10,
    'query_timeout': 60 * 10,
}

DEFAULT_MAX_RETRIES: int = 7

def parse_http_endpoint(http_endpoint: str) -> Tuple[RequestURL, Dict[str, List[str]]]:
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

    host = parsed_endpoint.hostname
    if host is None:
        host = ''

    if len(host.split(',')) > 1:
        raise ValueError('The endpoint must not contain multiple hosts.')

    port = parsed_endpoint.port
    if parsed_endpoint.port is None:
        port = 80 if parsed_endpoint.scheme == 'http' else 443

    if port is None:
        raise ValueError('The URL must have a port specified.')

    if not is_null_or_empty(parsed_endpoint.path):
        raise ValueError('The SDK does not currently support HTTP endpoint paths.')

    url = RequestURL(scheme=parsed_endpoint.scheme,
                     host=host,
                     port=port)

    return url, parse_qs(parsed_endpoint.query)


def parse_query_string_value(value: List[str], enforce_str: Optional[bool]=False) -> QueryStrVal:
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
    if v.isnumeric() and not enforce_str:
        return int(v)
    elif v.lower() in ['true', 'false']:
        return v.lower() == 'true'
    return v


def parse_query_str_options(query_str_opts: Dict[str, List[str]]) -> Dict[str, QueryStrVal]:
    final_opts: Dict[str, QueryStrVal] = {}
    for k, v in query_str_opts.items():
        tokens = k.split('.')
        if len(tokens) == 2:
            if tokens[0] == 'security':
                final_opts[tokens[1]] = parse_query_string_value(v)
            elif tokens[0] == 'timeout':
                val = parse_query_string_value(v, enforce_str=True)
                final_opts[tokens[1]] = parse_duration_str(cast(str, val))
            else:
                print('Warning: Unrecognized query string option:', k)
                # TODO:  exceptions -- this means the user passed in an invalid option
                pass
        else:
            if k in SecurityOptions.VALID_OPTION_KEYS:
                msg = f'Invalid query string option: {k}.'
                if k not in ['trust_only_pem_str', 'trust_only_certificates']:
                    msg += f'  Use "security.{k}" instead.'
                raise ValueError(msg)
            elif k in TimeoutOptions.VALID_OPTION_KEYS:
                raise ValueError(f'Invalid query string option: {k}.  Use "timeout.{k}" instead.')
            final_opts[k] = parse_query_string_value(v)

    return final_opts


@dataclass
class _ConnectionDetails:
    """
    **INTERNAL**
    """
    url: RequestURL
    cluster_options: ClusterOptionsTransformedKwargs
    credential: Tuple[bytes, bytes]
    default_deserializer: Deserializer
    ssl_context: Optional[ssl.SSLContext] = None
    sni_hostname: Optional[str] = None

    def get_connect_timeout(self) -> float:
        timeout_opts: Optional[TimeoutOptionsTransformedKwargs] = self.cluster_options.get('timeout_options')
        if timeout_opts is not None:
            connect_timeout = timeout_opts.get('connect_timeout', None)
            if connect_timeout is not None:
                return connect_timeout
        return DEFAULT_TIMEOUTS['connect_timeout']

    def get_max_retries(self) -> int:
        return self.cluster_options.get('max_retries', None) or DEFAULT_MAX_RETRIES

    def get_query_timeout(self) -> float:
        timeout_opts: Optional[TimeoutOptionsTransformedKwargs] = self.cluster_options.get('timeout_options')
        if timeout_opts is not None:
            query_timeout = timeout_opts.get('query_timeout', None)
            if query_timeout is not None:
                return query_timeout
        return DEFAULT_TIMEOUTS['query_timeout']

    def is_secure(self) -> bool:
        return self.url.scheme == 'https'

    def validate_security_options(self) -> None:
        security_opts: Optional[SecurityOptionsTransformedKwargs] = self.cluster_options.get('security_options')
        # TODO: security settings
        if security_opts is not None:
            # separate between value options and boolean option (trust_only_capella)
            solo_security_opts = ['trust_only_pem_file', 'trust_only_pem_str', 'trust_only_certificates']
            trust_capella = security_opts.get('trust_only_capella', None)
            security_opt_count = sum((1 if security_opts.get(opt, None) is not None else 0 for opt in solo_security_opts))  # noqa: E501
            if security_opt_count > 1 or (security_opt_count == 1 and trust_capella is True):
                raise ValueError(('Can only set one of the following options: '
                                f'[{", ".join(["trust_only_capella"] + solo_security_opts)}]'))

        if not self.is_secure():
            return

        self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.sni_hostname = self.url.host

        if security_opts is None:
            self.ssl_context.set_default_verify_paths()
            capalla_certs = _Certificates.get_capella_certificates()
            self.ssl_context.load_verify_locations(cadata='\n'.join(capalla_certs))
        elif security_opts.get('trust_only_capella', False):
            capalla_certs = _Certificates.get_capella_certificates()
            self.ssl_context.load_verify_locations(cadata='\n'.join(capalla_certs))
        elif (certpath := security_opts.get('trust_only_pem_file', None)) is not None:
            self.ssl_context.load_verify_locations(cafile=certpath)
            security_opts['trust_only_capella'] = False
        elif (certstr := security_opts.get('trust_only_pem_str', None)) is not None:
            self.ssl_context.load_verify_locations(cadata=certstr)
            security_opts['trust_only_capella'] = False
        elif (certificates := security_opts.get('trust_only_certificates', None)) is not None:
            self.ssl_context.load_verify_locations(cadata='\n'.join(certificates))
            security_opts['trust_only_capella'] = False

        if security_opts is not None and security_opts.get('disable_server_certificate_verification', False):
            # TODO: log warning
            print('Warning: Server certificate verification is disabled. This is not recommended for production use.')
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE
        else:
            self.ssl_context.check_hostname = True
            self.ssl_context.verify_mode = ssl.CERT_REQUIRED


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
