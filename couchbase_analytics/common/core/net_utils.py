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

from ipaddress import ip_address
from random import choice
from typing import (Any,
                    Dict,
                    List,
                    Optional)
from urllib.parse import quote

import anyio

def get_request_ip(host: str,
                   port: int,
                   previous_ips: Optional[List[str]]=None) -> Optional[str]:
    # Lets not call getaddrinfo, if the host is already an IP address
    try:
        ip = ip_address(host)
    except ValueError:
        ip = None

    # if we have localhost, httpx does not seem to be able to resolve IPv6 localhost (::1) properly
    # TODO: IPv6 support for localhost??
    if host == 'localhost':
        ip = '127.0.0.1'

    if previous_ips is None:
        previous_ips = []
    if not ip:
        # TODO: getaddrinfo() will raise an exception if name resolution fails
        result = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM, family=socket.AF_UNSPEC)
        # TODO: Handle IPv4 vs IPv6; with or without port?
        # ips = [f'{addr[4][0]}:{addr[4][1]}' for addr in result]
        try:
            ip = choice([addr[4][0] for addr in result if addr[4][0] not in previous_ips])
        except IndexError:
            ip = None
    else:
        ip_str = str(ip) if not isinstance(ip, str) else ip
        ip = None if ip_str in previous_ips else ip_str

    return ip


async def get_request_ip_async(host: str,
                               port: int,
                               previous_ips: Optional[List[str]]=None) -> Optional[str]:
    # Lets not call getaddrinfo, if the host is already an IP address
    try:
        ip = ip_address(host)
    except ValueError:
        ip = None

    # if we have localhost, httpx does not seem to be able to resolve IPv6 localhost (::1) properly
    # TODO: IPv6 support for localhost??
    if host == 'localhost':
        ip = '127.0.0.1'

    if previous_ips is None:
        previous_ips = []

    if not ip:
        # TODO: getaddrinfo() will raise an exception if name resolution fails
        result = await anyio.getaddrinfo(host, port, type=socket.SOCK_STREAM, family=socket.AF_UNSPEC)
        # TODO: Handle IPv4 vs IPv6; with or without port?
        # ips = [f'{addr[4][0]}:{addr[4][1]}' for addr in result]
        try:
            ip = choice([addr[4][0] for addr in result if addr[4][0] not in previous_ips])
        except IndexError:
            ip = None
    else:
        ip_str = str(ip) if not isinstance(ip, str) else ip
        ip = None if ip_str in previous_ips else ip_str

    return ip


# TODO: unused??
def to_query_str(params: Dict[str, Any]) -> str:
    encoded_params = []
    for k, v in params.items():
        if v in [True, False]:
            encoded_params.append(f'{quote(k)}={quote(str(v).lower())}')
        else:
            encoded_params.append(f'{quote(k)}={quote(str(v))}')

    return '&'.join(encoded_params)