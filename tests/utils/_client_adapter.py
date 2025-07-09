import socket
from typing import Dict

from httpx import URL, Response

from couchbase_analytics.protocol._core.client_adapter import _ClientAdapter
from couchbase_analytics.protocol._core.request import QueryRequest


def client_adapter_init_override(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
    if not hasattr(self, 'PYCBAC_TESTING'):
        raise RuntimeError('This is a testing only adapter')
    self._http_transport_cls = kwargs.pop('http_transport_cls', None)
    if self._http_transport_cls is not None and not hasattr(self._http_transport_cls, 'PYCBAC_TESTING'):
        raise RuntimeError('http_transport_cls must be a test transport')
    adapter: _ClientAdapter = kwargs.pop('adapter', None)
    adapter.close_client()
    self._client_id = adapter._client_id
    self._opts_builder = adapter._opts_builder
    self._conn_details = adapter._conn_details
    if self._http_transport_cls is None:
        self._http_transport_cls = adapter._http_transport_cls


# def create_client_override(self) -> None:
#     if not hasattr(self, '_client'):
#         if self._conn_details.is_secure():
#             transport = None
#             if self._http_transport_cls is not None:
#                 transport = self._http_transport_cls(verify=self._conn_details.ssl_context)
#             self._client = Client(verify=self._conn_details.ssl_context,
#                                     auth=BasicAuth(*self._conn_details.credential),
#                                     transport=transport)
#         else:
#             transport = None
#             if self._http_transport_cls is not None:
#                 transport = self._http_transport_cls()
#             self._client = Client(auth=BasicAuth(*self._conn_details.credential),
#                                     transport=transport)


def send_request_override(self: _ClientAdapter, request: QueryRequest) -> Response:
    if not hasattr(self, '_client'):
        raise RuntimeError('Client not created yet')

    # if request.url is None:
    #     raise ValueError('Request URL cannot be None')

    print(f'Sending request: {request.method} {request.url}')
    request_json = request.body
    if hasattr(self, '_request_json') and self._request_json is not None:
        request_json.update(self._request_json)

    request_extensions = request.extensions
    if hasattr(self, '_request_extensions') and self._request_extensions is not None:
        if request_extensions is None:
            request_extensions = self._request_extensions
        else:
            if 'timeout' in self._request_extensions:
                request_extensions['timeout'].update(self._request_extensions['timeout'])

    print(f'{request_extensions=}')

    url = URL(scheme=request.url.scheme, host=request.url.host, port=request.url.port, path=request.url.path)
    req = self._client.build_request(request.method, url, json=request_json, extensions=request_extensions)
    try:
        return self._client.send(req, stream=True)
    except socket.gaierror as err:
        req_url = self._conn_details.url.get_formatted_url()
        raise RuntimeError(f'Unable to connect to {req_url}') from err


def set_request_path(self: _ClientAdapter, path: str) -> None:
    self._ANALYTICS_PATH = path


def update_request_json(self: _ClientAdapter, json: Dict[str, object]) -> None:
    self._request_json = json  # type: ignore[attr-defined]


def update_request_extensions(self: _ClientAdapter, extensions: Dict[str, str]) -> None:
    self._request_extensions = extensions  # type: ignore[attr-defined]


class _TestClientAdapter(_ClientAdapter):
    pass


_TestClientAdapter.__init__ = client_adapter_init_override  # type: ignore[method-assign]
# _TestClientAdapter.create_client = create_client_override
_TestClientAdapter.send_request = send_request_override  # type: ignore[method-assign]
_TestClientAdapter.set_request_path = set_request_path
_TestClientAdapter.update_request_json = update_request_json
_TestClientAdapter.update_request_extensions = update_request_extensions
_TestClientAdapter.PYCBAC_TESTING = True

__all__ = ['_TestClientAdapter']
