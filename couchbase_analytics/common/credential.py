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

"""Credentials for the Analytics SDK."""

from __future__ import annotations

from base64 import b64encode
from typing import TYPE_CHECKING, Callable, Dict, Literal, Optional

if TYPE_CHECKING:
    from httpx import Request


class Credential:
    """A credential for authenticating with a Couchbase Analytics endpoint.

    Construct via a factory classmethod or direct keyword arguments::

        Credential(username='Administrator', password='swordfish')
        Credential(jwt_token='eyJ...')
        Credential.from_username_and_password('Administrator', 'swordfish')
        Credential.from_jwt('eyJ...')
        Credential.from_callable(lambda: Credential.from_jwt(get_token()))
    """

    __slots__ = ('_kind', '_header', '_password', '_token', '_username')

    _kind: Literal['password', 'jwt']
    _header: str
    _password: Optional[str]
    _token: Optional[str]
    _username: Optional[str]

    def __init__(
        self,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        jwt_token: Optional[str] = None,
    ) -> None:
        if jwt_token is not None:
            if username is not None or password is not None:
                raise ValueError('Cannot provide both a JWT token and username/password.')
            if not isinstance(jwt_token, str):
                raise TypeError('The JWT token must be a str.')
            jwt_token = jwt_token.strip()
            if not jwt_token:
                raise ValueError('The JWT token must not be empty.')
            self._kind = 'jwt'
            self._header = f'Bearer {jwt_token}'
            self._password = None
            self._token = jwt_token
            self._username = None
        elif username is not None or password is not None:
            if username is None:
                raise ValueError('Must provide a username.')
            if password is None:
                raise ValueError('Must provide a password.')
            if not isinstance(username, str):
                raise TypeError('The username must be a str.')
            if not isinstance(password, str):
                raise TypeError('The password must be a str.')
            self._kind = 'password'
            self._header = 'Basic ' + b64encode(f'{username}:{password}'.encode()).decode('ascii')
            self._password = password
            self._token = None
            self._username = username
        else:
            raise ValueError('Must provide either jwt_token or username and password.')

    def _asdict(self) -> Dict[str, str]:
        """**INTERNAL**"""
        if self._kind == 'jwt':
            return {'jwt_token': self._token or ''}
        return {'username': self._username or '', 'password': self._password or ''}

    @classmethod
    def from_username_and_password(cls, username: str, password: str) -> Credential:
        """Create a :class:`.Credential` from a username and password.

        Args:
            username: The username for the Analytics endpoint.
            password: The password for the Analytics endpoint.
        """
        if not isinstance(username, str):
            raise TypeError('The username must be a str.')
        if not isinstance(password, str):
            raise TypeError('The password must be a str.')
        return cls(username=username, password=password)

    @classmethod
    def from_jwt(cls, token: str) -> Credential:
        """Create a :class:`.Credential` from a JSON Web Token (JWT).

        The SDK sends an ``Authorization: Bearer <jwt>`` header on every HTTP request.
        JWT credentials may only be used with an ``https://`` endpoint.

        .. note::
            JWT credentials have a limited validity period.  To avoid authentication
            failures, periodically pass a fresh credential via
            :meth:`~couchbase_analytics.cluster.Cluster.set_credential`.

        Args:
            token: The JSON Web Token.
        """
        if not isinstance(token, str):
            raise TypeError('The JWT token must be a str.')
        return cls(jwt_token=token)

    @classmethod
    def from_callable(cls, callback: Callable[[], Credential]) -> Credential:
        """Create a :class:`.Credential` by invoking a callback.

        The callback is invoked once at construction time; it is not retained for
        dynamic credential lookup.  To pick up a refreshed credential later, pass
        a fresh :class:`.Credential` to
        :meth:`~couchbase_analytics.cluster.Cluster.set_credential`.

        Args:
            callback: Callback that returns a :class:`.Credential`.

        Example::

            cred = Credential.from_callable(
                lambda: Credential.from_username_and_password(
                    os.environ['USERNAME'], os.environ['PASSWORD']
                )
            )
        """
        return cls(**callback()._asdict())

    # Internal contract for the transport layer.  Credential rotation is
    # last-writer-wins; each request sees a fully-constructed credential.

    def _apply_to_request(self, request: Request) -> None:
        request.headers['Authorization'] = self._header

    def _check_endpoint_compatible(self, is_secure: bool) -> None:
        if self._kind == 'jwt' and not is_secure:
            raise ValueError('JWT credentials require a secure (https) connection.')

    def _check_replaceable_with(self, new_credential: Credential) -> None:
        if self._kind != new_credential._kind:
            raise TypeError(
                f'Cannot switch credential type at runtime; current type is '
                f'{self._kind}, new type is {new_credential._kind}.'
            )

    def __repr__(self) -> str:
        if self._kind == 'password':
            return f'Credential(username={self._username!r}, password=****)'
        return 'Credential(jwt_token=****)'


class _CredentialHolder:
    """**INTERNAL** Owns the mutable current credential; transport calls
    apply_to_request per request and replace for rotation.
    """

    __slots__ = ('_credential',)

    def __init__(self, credential: Credential) -> None:
        self._credential = credential

    @property
    def credential(self) -> Credential:
        return self._credential

    def apply_to_request(self, request: Request) -> None:
        self._credential._apply_to_request(request)

    def replace(self, new_credential: Credential) -> None:
        # GIL-atomic store; concurrent rotators are last-writer-wins.
        self._credential._check_replaceable_with(new_credential)
        self._credential = new_credential
