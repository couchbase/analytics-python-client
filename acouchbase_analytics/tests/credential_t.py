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

from base64 import b64encode
from typing import Any

import pytest
from httpx import Request

from acouchbase_analytics.protocol._core.client_adapter import _AsyncClientAdapter
from couchbase_analytics.credential import Credential
from couchbase_analytics.protocol._core.auth import DynamicCredentialAuth

_SAMPLE_JWT = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature'


def _authorization_header(client: Any) -> str:
    auth = DynamicCredentialAuth(client.credential_holder)
    request_url = f'{client.connection_details.url.get_formatted_url()}{client.analytics_path}'
    req = Request('POST', request_url)
    flow = auth.auth_flow(req)
    dispatched = next(flow)
    return dispatched.headers['Authorization']


class CredentialTestSuite:
    TEST_MANIFEST = [
        'test_jwt_credential_creation',
        'test_jwt_credential_strips_token',
        'test_jwt_credential_rejects_non_string',
        'test_credential_direct_construction_with_jwt',
        'test_credential_direct_construction_with_password',
        'test_credential_rejects_unknown_kwargs',
        'test_credential_hides_internal_details',
        'test_credential_from_callable_with_jwt',
        'test_jwt_credential_repr_redacts_token',
        'test_jwt_credential_rejects_http_endpoint',
        'test_jwt_credential_accepts_https_endpoint',
        'test_password_credential_http_authorization_header',
        'test_password_credential_repr_redacts_password',
        'test_dynamic_auth_sets_header_from_current_credential',
        'test_async_dynamic_auth_sets_header_from_current_credential',
        'test_dynamic_auth_picks_up_rotated_credential',
        'test_set_credential_same_type_updates_state',
        'test_set_credential_password_to_jwt_fails',
        'test_set_credential_jwt_to_password_fails',
        'test_set_credential_failure_does_not_change_state',
    ]

    def test_jwt_credential_creation(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    def test_jwt_credential_strips_token(self) -> None:
        cred = Credential.from_jwt(f'  {_SAMPLE_JWT}\n')
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    @pytest.mark.parametrize(
        ('bad_token', 'expected_exc'),
        [
            (12345, TypeError),
            (None, TypeError),
            (b'bytes.jwt.token', TypeError),
            (1.5, TypeError),
            (['a', 'b'], TypeError),
            ('', ValueError),
            ('   \n', ValueError),
        ],
    )
    def test_jwt_credential_rejects_non_string(self, bad_token: object, expected_exc: type) -> None:
        with pytest.raises(expected_exc):
            Credential.from_jwt(bad_token)  # type: ignore[arg-type]

    def test_credential_direct_construction_with_jwt(self) -> None:
        cred = Credential(jwt_token=f'  {_SAMPLE_JWT}\n')
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    def test_credential_direct_construction_with_password(self) -> None:
        cred = Credential(username='Administrator', password='password')
        client = _AsyncClientAdapter('http://localhost', cred)
        expected = 'Basic ' + b64encode(b'Administrator:password').decode('ascii')
        assert _authorization_header(client) == expected

    def test_credential_rejects_unknown_kwargs(self) -> None:
        with pytest.raises(TypeError, match='unexpected keyword argument'):
            Credential(usernme='Administrator', password='password')  # type: ignore[call-arg]
        with pytest.raises(TypeError, match='unexpected keyword argument'):
            Credential(jwt_token=_SAMPLE_JWT, extra='ignored')  # type: ignore[call-arg]

    def test_credential_hides_internal_details(self) -> None:
        public_attrs = {name for name in dir(Credential.from_jwt(_SAMPLE_JWT)) if not name.startswith('_')}
        assert public_attrs == {'from_callable', 'from_jwt', 'from_username_and_password'}

    def test_credential_from_callable_with_jwt(self) -> None:
        cred = Credential.from_callable(lambda: Credential.from_jwt(_SAMPLE_JWT))
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    def test_jwt_credential_repr_redacts_token(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        rendered = repr(cred)
        assert _SAMPLE_JWT not in rendered
        assert '****' in rendered

    def test_jwt_credential_rejects_http_endpoint(self) -> None:
        with pytest.raises(ValueError, match='require a secure'):
            _AsyncClientAdapter('http://localhost', Credential.from_jwt(_SAMPLE_JWT))

    def test_jwt_credential_accepts_https_endpoint(self) -> None:
        client = _AsyncClientAdapter('https://localhost', Credential.from_jwt(_SAMPLE_JWT))
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    def test_password_credential_http_authorization_header(self) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('http://localhost', cred)
        expected = 'Basic ' + b64encode(b'Administrator:password').decode('ascii')
        assert _authorization_header(client) == expected

    def test_password_credential_repr_redacts_password(self) -> None:
        cred = Credential.from_username_and_password('Administrator', 'super-secret')
        rendered = repr(cred)
        assert 'super-secret' not in rendered
        assert '****' in rendered
        assert 'Administrator' in rendered

    def test_dynamic_auth_sets_header_from_current_credential(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        auth = DynamicCredentialAuth(client.credential_holder)

        req = Request('POST', 'https://localhost/api/v1/request')
        flow = auth.auth_flow(req)
        dispatched = next(flow)
        assert dispatched.headers['Authorization'] == f'Bearer {_SAMPLE_JWT}'

    @pytest.mark.anyio
    async def test_async_dynamic_auth_sets_header_from_current_credential(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        auth = DynamicCredentialAuth(client.credential_holder)

        request_url = f'{client.connection_details.url.get_formatted_url()}{client.analytics_path}'
        req = Request('POST', request_url)
        flow = auth.async_auth_flow(req)
        dispatched = await flow.__anext__()
        assert dispatched.headers['Authorization'] == f'Bearer {_SAMPLE_JWT}'

    @pytest.mark.anyio
    async def test_dynamic_auth_picks_up_rotated_credential(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        auth = DynamicCredentialAuth(client.credential_holder)

        new_token = 'rotated.jwt.token'
        await client.update_credential(Credential.from_jwt(new_token))

        req = Request('POST', 'https://localhost/api/v1/request')
        flow = auth.auth_flow(req)
        dispatched = next(flow)
        assert dispatched.headers['Authorization'] == f'Bearer {new_token}'

    @pytest.mark.anyio
    async def test_set_credential_same_type_updates_state(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

        new_token = 'fresh.jwt.token'
        await client.update_credential(Credential.from_jwt(new_token))

        assert _authorization_header(client) == f'Bearer {new_token}'

    @pytest.mark.anyio
    async def test_set_credential_password_to_jwt_fails(self) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('http://localhost', cred)
        with pytest.raises(TypeError, match='Cannot switch credential type'):
            await client.update_credential(Credential.from_jwt(_SAMPLE_JWT))

    @pytest.mark.anyio
    async def test_set_credential_jwt_to_password_fails(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        with pytest.raises(TypeError, match='Cannot switch credential type'):
            await client.update_credential(Credential.from_username_and_password('Administrator', 'password'))

    @pytest.mark.anyio
    async def test_set_credential_failure_does_not_change_state(self) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('http://localhost', cred)
        original_header = _authorization_header(client)

        with pytest.raises(TypeError):
            await client.update_credential(Credential.from_jwt(_SAMPLE_JWT))

        assert _authorization_header(client) == original_header


class CredentialTests(CredentialTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(CredentialTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(CredentialTests) if valid_test_method(meth)]
        test_list = set(CredentialTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
