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

from datetime import timedelta
from typing import Dict, Type

import pytest

from acouchbase_analytics.protocol._core.client_adapter import _AsyncClientAdapter
from couchbase_analytics.credential import Credential
from couchbase_analytics.deserializer import DefaultJsonDeserializer, Deserializer, PassthroughDeserializer
from couchbase_analytics.options import (
    ClusterOptions,
    SecurityOptions,
    SecurityOptionsKwargs,
    TimeoutOptions,
    TimeoutOptionsKwargs,
)
from tests.utils import get_test_cert_list, get_test_cert_path, get_test_cert_str

TEST_CERT_PATH = get_test_cert_path()
TEST_CERT_LIST = get_test_cert_list()
TEST_CERT_STR = get_test_cert_str()


class ClusterOptionsTestSuite:

    TEST_MANIFEST = [
        'test_options_deserializer',
        'test_options_deserializer_kwargs',
        'test_security_options',
        'test_security_options_classmethods',
        'test_security_options_kwargs',
        'test_security_options_invalid',
        'test_security_options_invalid_kwargs',
        'test_timeout_options',
        'test_timeout_options_kwargs',
        'test_timeout_options_must_be_positive',
        'test_timeout_options_must_be_positive_kwargs',
    ]

    @pytest.mark.parametrize('deserializer_cls', [DefaultJsonDeserializer, PassthroughDeserializer])
    def test_options_deserializer(self, deserializer_cls:Type[Deserializer]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        deserializer_instance = deserializer_cls()
        client = _AsyncClientAdapter('https://localhost', cred, ClusterOptions(deserializer=deserializer_instance))
        assert isinstance(client.connection_details.default_deserializer, deserializer_cls)

    @pytest.mark.parametrize('deserializer_cls', [DefaultJsonDeserializer, PassthroughDeserializer])
    def test_options_deserializer_kwargs(self, deserializer_cls:Type[Deserializer]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        deserializer_instance = deserializer_cls()
        client = _AsyncClientAdapter('https://localhost', cred, **{'deserializer': deserializer_instance})
        assert isinstance(client.connection_details.default_deserializer, deserializer_cls)

    @pytest.mark.parametrize('opts, expected_opts',
                             [({}, None),
                              ({'trust_only_capella': True},
                               {'trust_only_capella': True}),
                              ({'trust_only_pem_file': TEST_CERT_PATH},
                               {'trust_only_pem_file': TEST_CERT_PATH,
                                'trust_only_capella': False}),
                              ({'trust_only_pem_str': TEST_CERT_STR},
                               {'trust_only_pem_str': TEST_CERT_STR,
                                'trust_only_capella': False}),
                              ({'trust_only_certificates': TEST_CERT_LIST},
                               {'trust_only_certificates': TEST_CERT_LIST,
                                'trust_only_capella': False}),
                              ({'disable_server_certificate_verification': True},
                               {'disable_server_certificate_verification': True}),
                              ])
    def test_security_options(self, opts: SecurityOptionsKwargs, expected_opts: SecurityOptionsKwargs) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('https://localhost',
                                     cred,
                                     ClusterOptions(security_options=SecurityOptions(**opts)))
        assert expected_opts == client.connection_details.cluster_options.get('security_options')

    @pytest.mark.parametrize('opts, expected_opts',
                             [(SecurityOptions.trust_only_capella(),
                               {'trust_only_capella': True}),
                              (SecurityOptions.trust_only_pem_file(TEST_CERT_PATH),
                               {'trust_only_pem_file': TEST_CERT_PATH,
                                'trust_only_capella': False}),
                              (SecurityOptions.trust_only_pem_str(TEST_CERT_STR),
                               {'trust_only_pem_str': TEST_CERT_STR,
                                'trust_only_capella': False}),
                              (SecurityOptions.trust_only_certificates(TEST_CERT_LIST),
                               {'trust_only_certificates': TEST_CERT_LIST,
                                'trust_only_capella': False}),
                              ])
    def test_security_options_classmethods(self, opts: SecurityOptions, expected_opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('https://localhost',
                                     cred,
                                     ClusterOptions(security_options=opts))
        assert expected_opts == client.connection_details.cluster_options.get('security_options')

    @pytest.mark.parametrize('opts, expected_opts',
                             [({}, None),
                              ({'trust_only_capella': True},
                               {'trust_only_capella': True}),
                              ({'trust_only_pem_file': TEST_CERT_PATH},
                               {'trust_only_pem_file': TEST_CERT_PATH,
                                'trust_only_capella': False}),
                              ({'trust_only_pem_str': TEST_CERT_STR},
                               {'trust_only_pem_str': TEST_CERT_STR,
                                'trust_only_capella': False}),
                              ({'trust_only_certificates': TEST_CERT_LIST},
                               {'trust_only_certificates': TEST_CERT_LIST,
                                'trust_only_capella': False}),
                              ({'disable_server_certificate_verification': True},
                               {'disable_server_certificate_verification': True}),
                              ])
    def test_security_options_kwargs(self, opts: Dict[str, object], expected_opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('https://localhost', cred, **opts)
        assert expected_opts == client.connection_details.cluster_options.get('security_options')

    @pytest.mark.parametrize('opts',
                             [{'trust_only_capella': True,
                               'trust_only_pem_file': TEST_CERT_PATH},
                              {'trust_only_capella': True,
                               'trust_only_pem_str': TEST_CERT_STR},
                              {'trust_only_capella': True,
                               'trust_only_certificates': TEST_CERT_LIST},
                              ])
    def test_security_options_invalid(self, opts: SecurityOptionsKwargs) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        with pytest.raises(ValueError):
            _AsyncClientAdapter('https://localhost',
                                cred,
                                ClusterOptions(security_options=SecurityOptions(**opts)))

    @pytest.mark.parametrize('opts',
                             [{'trust_only_capella': True,
                               'trust_only_pem_file': TEST_CERT_PATH},
                              {'trust_only_capella': True,
                               'trust_only_pem_str': TEST_CERT_STR},
                              {'trust_only_capella': True,
                               'trust_only_certificates': TEST_CERT_LIST},
                              ])
    def test_security_options_invalid_kwargs(self, opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        with pytest.raises(ValueError):
            _AsyncClientAdapter('https://localhost', cred, **opts)

    @pytest.mark.parametrize('opts, expected_opts',
                             [({}, None),
                              ({'connect_timeout': timedelta(seconds=30)},
                               {'connect_timeout': 30}),
                              ({'query_timeout': timedelta(seconds=30)},
                               {'query_timeout': 30}),
                              ({'connect_timeout': timedelta(seconds=60),
                                'query_timeout': timedelta(seconds=30)},
                               {'connect_timeout': 60,
                                'query_timeout': 30}),
                              ])
    def test_timeout_options(self, opts: TimeoutOptionsKwargs, expected_opts: TimeoutOptionsKwargs) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('https://localhost',
                                     cred,
                                     ClusterOptions(timeout_options=TimeoutOptions(**opts)))
        assert expected_opts == client.connection_details.cluster_options.get('timeout_options')

    @pytest.mark.parametrize('opts, expected_opts',
                             [({'connect_timeout': timedelta(seconds=30)},
                               {'connect_timeout': 30}),
                              ({'query_timeout': timedelta(seconds=30)},
                               {'query_timeout': 30}),
                              ({'connect_timeout': timedelta(seconds=60),
                                'query_timeout': timedelta(seconds=30)},
                               {'connect_timeout': 60,
                                'query_timeout': 30}),
                              ])
    def test_timeout_options_kwargs(self, opts: Dict[str, object], expected_opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('https://localhost', cred, **opts)
        assert expected_opts == client.connection_details.cluster_options.get('timeout_options')

    @pytest.mark.parametrize('opts',
                             [{'connect_timeout': timedelta(seconds=-1)},
                              {'query_timeout': timedelta(seconds=-1)}])
    def test_timeout_options_must_be_positive(self, opts: TimeoutOptionsKwargs) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        with pytest.raises(ValueError):
            _AsyncClientAdapter('https://localhost',
                                cred,
                                ClusterOptions(timeout_options=TimeoutOptions(**opts)))

    @pytest.mark.parametrize('opts',
                             [{'connect_timeout': timedelta(seconds=-1)},
                              {'query_timeout': timedelta(seconds=-1)}])
    def test_timeout_options_must_be_positive_kwargs(self, opts: Dict[str, object]) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        with pytest.raises(ValueError):
            _AsyncClientAdapter('https://localhost', cred, **opts)


class ClusterOptionsTests(ClusterOptionsTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(ClusterOptionsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClusterOptionsTests) if valid_test_method(meth)]
        test_list = set(ClusterOptionsTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')