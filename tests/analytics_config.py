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

import os
import pathlib
from configparser import ConfigParser
from typing import Optional, Tuple
from uuid import uuid4

import pytest

from tests import AnalyticsTestEnvironmentError

BASEDIR = pathlib.Path(__file__).parent.parent
CONFIG_FILE = os.path.join(pathlib.Path(__file__).parent, 'test_config.ini')
ENV_TRUE = ['true', '1', 'y', 'yes', 'on']


class AnalyticsConfig:
    def __init__(self) -> None:
        self._scheme = 'http'
        self._host = 'localhost'
        self._port = 8095
        self._username = 'Administrator'
        self._password = 'password'
        self._nonprod = False
        self._database_name = ''
        self._scope_name = ''
        self._collection_name = ''
        self._disable_server_certificate_verification = False
        self._create_keyspace = False

    @property
    def database_name(self) -> str:
        return self._database_name

    @property
    def collection_name(self) -> str:
        return self._collection_name

    @property
    def create_keyspace(self) -> bool:
        return self._create_keyspace

    @property
    def fqdn(self) -> str:
        return f'`{self._database_name}`.`{self._scope_name}`.`{self._collection_name}`'

    @property
    def nonprod(self) -> bool:
        return self._nonprod

    @property
    def disable_server_certificate_verification(self) -> bool:
        return self._disable_server_certificate_verification

    @property
    def scope_name(self) -> str:
        return self._scope_name

    def get_connection_string(self, ignore_port: Optional[bool] = False) -> str:
        if ignore_port is None or ignore_port is False and self._port is not None:
            return f'{self._scheme}://{self._host}:{self._port}'
        return f'{self._scheme}://{self._host}'

    def get_username_and_pw(self) -> Tuple[str, str]:
        return self._username, self._password

    @classmethod
    def load_config(cls) -> AnalyticsConfig:
        analytics_config = cls()
        try:
            test_config = ConfigParser()
            test_config.read(CONFIG_FILE)
            test_config_analytics = test_config['analytics']
            analytics_config._scheme = os.environ.get(
                'PYCBAC_SCHEME', test_config_analytics.get('scheme', fallback='https')
            )
            analytics_config._host = os.environ.get(
                'PYCBAC_HOST', test_config_analytics.get('host', fallback='localhost')
            )
            port = os.environ.get('PYCBAC_PORT', test_config_analytics.get('port', fallback='8095'))
            analytics_config._port = int(port)
            analytics_config._username = os.environ.get(
                'PYCBAC_USERNAME', test_config_analytics.get('username', fallback='Administrator')
            )
            analytics_config._password = os.environ.get(
                'PYCBAC_PASSWORD', test_config_analytics.get('password', fallback='password')
            )
            use_nonprod = os.environ.get('PYCBAC_NONPROD', test_config_analytics.get('nonprod', fallback='OFF'))
            if use_nonprod.lower() in ENV_TRUE:
                analytics_config._nonprod = True
            else:
                analytics_config._nonprod = False
            analytics_config._database_name = os.environ.get(
                'PYCBAC_DATABASE', test_config_analytics.get('database_name', fallback='travel-sample')
            )
            analytics_config._scope_name = os.environ.get(
                'PYCBAC_SCOPE', test_config_analytics.get('scope_name', fallback='inventory')
            )
            analytics_config._collection_name = os.environ.get(
                'PYCBAC_COLLECTION', test_config_analytics.get('collection_name', fallback='airline')
            )
            disable_cert_verification = os.environ.get(
                'PYCBAC_DISABLE_SERVER_CERT_VERIFICATION',
                test_config_analytics.get('disable_server_cert_verification', fallback='ON'),
            )
            if disable_cert_verification.lower() in ENV_TRUE:
                analytics_config._disable_server_certificate_verification = True
            fqdn = os.environ.get('PYCBAC_FQDN', test_config_analytics.get('fqdn', fallback=None))
            if fqdn is not None:
                fqdn_tokens = fqdn.split('.')
                if len(fqdn_tokens) != 3:
                    raise AnalyticsTestEnvironmentError(
                        (f'Invalid FQDN provided. Expected database.scope.collection. FQDN provide={fqdn}')
                    )

                analytics_config._database_name = f'{fqdn_tokens[0]}'
                analytics_config._scope_name = f'{fqdn_tokens[1]}'
                analytics_config._collection_name = f'{fqdn_tokens[2]}'
                analytics_config._create_keyspace = False
            else:
                # lets make the database unique (enough)
                analytics_config._database_name = f'travel-sample-{str(uuid4())[:8]}'
                analytics_config._scope_name = 'inventory'
                analytics_config._collection_name = 'airline'

        except Exception as ex:
            raise AnalyticsTestEnvironmentError(f'Problem trying read/load test configuration:\n{ex}') from None

        return analytics_config


@pytest.fixture(name='analytics_config', scope='session')
def analytics_test_config() -> AnalyticsConfig:
    config = AnalyticsConfig.load_config()
    return config
