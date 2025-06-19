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

from typing import Dict, Optional
from urllib.parse import urlparse

import pytest

from couchbase_analytics.cluster import Cluster
from couchbase_analytics.credential import Credential
from couchbase_analytics.protocol.core.client_adapter import _ClientAdapter
from couchbase_analytics.protocol.core.request import _RequestBuilder
from tests.utils import get_test_cert_path, to_query_str

TEST_CERT_PATH = get_test_cert_path()


class ConnectionTestSuite:
    TEST_MANIFEST = [
        'test_connstr_options_fail',
        'test_connstr_options_timeout',
        'test_connstr_options_timeout_fail',
        'test_connstr_options_timeout_invalid_duration',
        'test_connstr_options_security',
        'test_connstr_options_security_fail',
        'test_invalid_connection_strings',
        'test_valid_connection_strings',
    ]

    @pytest.mark.parametrize('connstr_opt',
                             ['invalid_op=10',
                              'connect_timeout=2500ms',
                              'dispatch_timeout=2500ms',
                              'query_timeout=2500ms',
                              'socket_connect_timeout=2500ms',
                              'trust_only_pem_file=/path/to/file',
                              'disable_server_certificate_verification=True'
                              ])
    def test_connstr_options_fail(self,
                                          connstr_opt: str) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'https://localhost?{connstr_opt}'
        with pytest.raises(ValueError):
            _ClientAdapter(connstr, cred)

    @pytest.mark.parametrize('duration, expected_seconds',
                             [('1h', '3600'),
                              ('+1h', '3600'),
                              ('+1h', '3600'),
                              ('1h10m', '4200'),
                              ('1.h10m', '4200'),
                              ('.1h10m', '960'),
                              ('0001h00010m', '4200'),
                              ('2m3s4ms', '123.004'),
                              (('100ns', '1e-7')),
                              (('100us', '1e-4')),
                              (('100μs', '1e-4')),
                              (('1000000ns', '.001')),
                              (('1000us', '.001')),
                              (('1000μs', '.001')),
                              ('4ms3s2m', '123.004'),
                              ('4ms3s2m5s', '128.004'),
                              ('2m3.125s', '123.125'),
                              ])
    def test_connstr_options_timeout(self,
                                     duration: str,
                                     expected_seconds: str) -> None:
        opt_keys = ['timeout.connect_timeout',
                    'timeout.query_timeout']
        opts = {k: duration for k in opt_keys}
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'https://localhost?{to_query_str(opts)}'
        client = _ClientAdapter(connstr, cred)
        req_builder = _RequestBuilder(client)
        req = req_builder.build_base_query_request('SELECT 1=1')
        expected = float(expected_seconds)
        returned_timeout_opts = req.get_request_timeouts()
        assert isinstance(returned_timeout_opts, dict)
        for k in opts.keys():
            opt_key = k.split('.')[1]
            if opt_key.startswith('connect'):
                pool_timeout = returned_timeout_opts.get('pool')
                assert pool_timeout is not None
                assert abs(pool_timeout - expected) < 1e-9
                connect_timeout = returned_timeout_opts.get('connect')
                assert connect_timeout is not None
                assert abs(connect_timeout - expected) < 1e-9
            else:
                read_timeout = returned_timeout_opts.get('read')
                assert read_timeout is not None
                assert abs(read_timeout - expected) < 1e-9

    @pytest.mark.parametrize('invalid_opt_name',
                             ['connect_timeout',
                              'dispatch_timeout',
                              'query_timeout',
                              'resolve_timeout',
                              'socket_connect_timeout'])
    def test_connstr_options_timeout_fail(self, invalid_opt_name: str) -> None:
        opts = {invalid_opt_name: '2500s'}
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'https://localhost?{to_query_str(opts)}'
        with pytest.raises(ValueError):
            _ClientAdapter(connstr, cred)

    @pytest.mark.parametrize('bad_duration',
                             ['123',
                              '00',
                              ' 1h',
                              '1h ',
                              '1h 2m'
                              '+-3h',
                              '-+3h',
                              '-',
                              '-.',
                              '.',
                              '.h',
                              '2.3.4h',
                              '3x',
                              '3',
                              '3h4x',
                              '1H',
                              '1h-2m',
                              '-1h',
                              '-1m',
                              '-1s'
                              ])
    def test_connstr_options_timeout_invalid_duration(self,
                                                      bad_duration: str) -> None:
        opt_keys = ['timeout.connect_timeout', 'timeout.query_timeout']
        for key in opt_keys:
            opts = {key: bad_duration}
            cred = Credential.from_username_and_password('Administrator', 'password')
            connstr = f'https://localhost?{to_query_str(opts)}'
            with pytest.raises(ValueError):
                _ClientAdapter(connstr, cred)

    @pytest.mark.parametrize('connstr_opts, expected_opts',
                             [({'security.trust_only_pem_file': TEST_CERT_PATH},
                               {'trust_only_pem_file': TEST_CERT_PATH,
                                'trust_only_capella': False}),
                              ({'security.disable_server_certificate_verification': 'true'},
                               {'disable_server_certificate_verification': True}),
                              ])
    def test_connstr_options_security(self,
                                      connstr_opts: Dict[str, object],
                                      expected_opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'https://localhost?{to_query_str(connstr_opts)}'
        client = _ClientAdapter(connstr, cred)
        sec_opts = client.connection_details.cluster_options.get('security_options', {})
        assert sec_opts == expected_opts

    @pytest.mark.parametrize('invalid_opt_name',
                             ['trust_only_capella',
                              'trust_only_pem_file',
                              'trust_only_pem_str',
                              'trust_only_certificates',
                              'disable_server_certificate_verification'])
    def test_connstr_options_security_fail(self, invalid_opt_name: str) -> None:
        opts = {invalid_opt_name: 'True'}
        cred = Credential.from_username_and_password('Administrator', 'password')
        connstr = f'https://localhost?{to_query_str(opts)}'
        with pytest.raises(ValueError):
            _ClientAdapter(connstr, cred)

    @pytest.mark.parametrize('connstr', ['10.0.0.1:8091',
                                         'http://10.0.0.1:11222,10.0.0.2,10.0.0.3:11207',
                                         'http://10.0.0.1;10.0.0.2:11210;10.0.0.3',
                                         'http://[::ffff:192.168.0.1]:11207,[::ffff:192.168.0.2]:11207',
                                         'https://10.0.0.1:11222,10.0.0.2,10.0.0.3:11207',
                                         'https://10.0.0.1;10.0.0.2:11210;10.0.0.3',
                                         'https://[::ffff:192.168.0.1]:11207,[::ffff:192.168.0.2]:11207',
                                         'couchbase://10.0.0.1',
                                         'couchbases://10.0.0.1'])
    def test_invalid_connection_strings(self, connstr: str) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        with pytest.raises(ValueError):
            Cluster.create_instance(connstr, cred)

    @pytest.mark.parametrize('connstr', ['http://10.0.0.1',
                                         'http://10.0.0.1:11222',
                                         'http://[3ffe:2a00:100:7031::1]',
                                         'http://[::ffff:192.168.0.1]:11207',
                                         'http://test.local:11210',
                                         'http://fqdn',
                                         'https://10.0.0.1',
                                         'https://10.0.0.1:11222',
                                         'https://[3ffe:2a00:100:7031::1]',
                                         'https://[::ffff:192.168.0.1]:11207',
                                         'https://test.local:11210',
                                         'https://fqdn'
                                         ])
    def test_valid_connection_strings(self, connstr: str) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _ClientAdapter(connstr, cred)
        # options should be empty
        assert {} == client.connection_details.cluster_options
        parsed_connstr = urlparse(connstr)
        parsed_port = parsed_connstr.port or (80 if parsed_connstr.scheme == 'http' else 443)
        scheme, host, port = client.connection_details.get_scheme_host_and_port()
        assert f'{parsed_connstr.scheme}://{parsed_connstr.hostname}:{parsed_port}' == f'{scheme}://{host}:{port}'


class ConnectionTests(ConnectionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ConnectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ConnectionTests) if valid_test_method(meth)]
        test_list = set(ConnectionTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
