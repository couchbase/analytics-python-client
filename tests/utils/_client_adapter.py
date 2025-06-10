import socket

from typing import Dict

from httpx import Response

from couchbase_analytics.protocol.core.client_adapter import _ClientAdapter
from couchbase_analytics.protocol.core.request import QueryRequest
    

def client_adapter_init_override(self, *args, **kwargs) -> None:
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
            
def send_request_override(self, request: QueryRequest) -> Response:
    if not hasattr(self, '_client'):
        raise RuntimeError('Client not created yet')
    
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

    req = self._client.build_request(request.method,
                                     request.url,
                                     json=request_json,
                                     extensions=request_extensions)
    try:
        return self._client.send(req, stream=True)
    except socket.gaierror as err:
        raise RuntimeError(f'Unable to connect to {self._conn_details.get_scheme_host_and_port()}') from err
    
def set_request_path(self, path: str) -> None:
    self._ANALYTICS_PATH = path

def update_request_json(self, json: Dict[str, object]) -> None:
    self._request_json = json

def update_request_extensions(self, extensions: Dict[str, str]) -> None:
    self._request_extensions = extensions

_ClientAdapter.__init__ = client_adapter_init_override
# _ClientAdapter.create_client = create_client_override
_ClientAdapter.send_request = send_request_override
setattr(_ClientAdapter, 'set_request_path', set_request_path)
setattr(_ClientAdapter, 'update_request_json', update_request_json)
setattr(_ClientAdapter, 'update_request_extensions', update_request_extensions)
setattr(_ClientAdapter, 'PYCBAC_TESTING', True)