
import ssl
import time
from types import TracebackType
from typing import Iterable, Optional, TypeVar, Union

from httpcore import (
    ConnectionInterface,
    ConnectionPool,
    HTTP2Connection,
    HTTP11Connection,
    HTTPConnection,
    Origin,
    Request,
)
from httpcore import Response as CoreResponse
from httpcore._exceptions import ConnectionNotAvailable, UnsupportedProtocol
from httpcore._sync.connection_pool import PoolByteStream, PoolRequest
from httpx import URL, BaseTransport, HTTPTransport, Limits, Proxy, Response, SyncByteStream, create_ssl_context
from httpx._transports.default import SOCKET_OPTION, ResponseStream, map_httpcore_exceptions
from httpx._types import CertTypes, ProxyTypes

# httpx._transports.default.py
T = TypeVar("T", bound="HTTPTransport")
DEFAULT_LIMITS = Limits(max_connections=100, max_keepalive_connections=20)

# ProxyTypes = Union["URL", str, "Proxy"]
# CertTypes = Union[str, Tuple[str, str], Tuple[str, str, str]]

class AnalyticsHTTPConnection(HTTPConnection):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)

    # The logic is the exact same as httpcore's Connection.handle_request, with the following additions:
    #       - We update the request's read timeout to remove the time taken to establish a connection
    # 2025-06-05: https://github.com/encode/httpcore/blob/98209758cc14e1a5f966fe1dfdc1064b94055d8c/httpcore/_sync/connection.py#L69
    def handle_request(self, request: Request) -> CoreResponse:
        if not self.can_handle_request(request.url.origin):
            raise RuntimeError(
                f"Attempted to send request to {request.url.origin} on connection to {self._origin}"
            )

        # PYCBAC Addition: track the query deadline
        timeouts = request.extensions.get("timeout", {})
        timeout = timeouts.get("read", None)
        deadline = time.monotonic() + timeout
        try:
            with self._request_lock:
                if self._connection is None:
                    stream = self._connect(request)

                    ssl_object = stream.get_extra_info("ssl_object")
                    http2_negotiated = (
                        ssl_object is not None
                        and ssl_object.selected_alpn_protocol() == "h2"
                    )
                    if http2_negotiated or (self._http2 and not self._http1):
                        self._connection = HTTP2Connection(
                            origin=self._origin,
                            stream=stream,
                            keepalive_expiry=self._keepalive_expiry,
                        )
                    else:
                        self._connection = HTTP11Connection(
                            origin=self._origin,
                            stream=stream,
                            keepalive_expiry=self._keepalive_expiry,
                        )
        except BaseException as exc:
            self._connect_failed = True
            raise exc

        # PYCBAC Addition: We _always_ set the request timeouts, so no need to validate keys
        query_timeout = round(deadline - time.monotonic(), 6) # round to microseconds
        request.extensions["timeout"]["read"] = query_timeout

        return self._connection.handle_request(request)

class AnalyticsConnectionPool(ConnectionPool):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        super().__init__(*args, **kwargs)

    # The logic is the exact same as httpcore's ConnectionPool.handle_request, with the following additions:
    #       - We update the request's connect timeout to remove the time taken to obtain a connection from the pool
    #       - For any retries in obtaining a connection from the pool, we update the timeout for subsequent attempts
    # 2025.05.30: https://github.com/encode/httpcore/blob/98209758cc14e1a5f966fe1dfdc1064b94055d8c/httpcore/_sync/connection_pool.py#L199
    def handle_request(self, request: Request) -> CoreResponse:
        """
        Send an HTTP request, and return an HTTP response.

        This is the core implementation that is called into by `.request()` or `.stream()`.
        """
        scheme = request.url.scheme.decode()
        if scheme == "":
            raise UnsupportedProtocol(
                "Request URL is missing an 'http://' or 'https://' protocol."
            )
        if scheme not in ("http", "https", "ws", "wss"):
            raise UnsupportedProtocol(
                f"Request URL has an unsupported protocol '{scheme}://'."
            )

        timeouts = request.extensions.get("timeout", {})
        timeout = timeouts.get("pool", None)

        with self._optional_thread_lock:
            # Add the incoming request to our request queue.
            pool_request = PoolRequest(request)
            self._requests.append(pool_request)

        # PYCBAC Addition: track the deadline
        deadline = time.monotonic() + timeout
        try:
            while True:
                with self._optional_thread_lock:
                    # Assign incoming requests to available connections,
                    # closing or creating new connections as required.
                    closing = self._assign_requests_to_connections()
                self._close_connections(closing)

                # Wait until this request has an assigned connection.
                connection = pool_request.wait_for_connection(timeout=timeout)
                # PYCBAC Addition: We _always_ set the request timeouts, so no need to validate keys
                connect_timeout = round(deadline - time.monotonic(), 6) # round to microseconds
                pool_request.request.extensions["timeout"]["connect"] = connect_timeout

                try:
                    # Send the request on the assigned connection.
                    response = connection.handle_request(
                        pool_request.request
                    )
                except ConnectionNotAvailable:
                    # In some cases a connection may initially be available to
                    # handle a request, but then become unavailable.
                    #
                    # In this case we clear the connection and try again.
                    pool_request.clear_connection()
                    # PYCBAC Addition: We update the timeout for the next attempt
                    timeout = round(deadline - time.monotonic(), 6) # round to microseconds
                else:
                    break  # pragma: nocover

        except BaseException as exc:
            with self._optional_thread_lock:
                # For any exception or cancellation we remove the request from
                # the queue, and then re-assign requests to connections.
                self._requests.remove(pool_request)
                closing = self._assign_requests_to_connections()

            self._close_connections(closing)
            raise exc from None

        # Return the response. Note that in this case we still have to manage
        # the point at which the response is closed.
        assert isinstance(response.stream, Iterable)
        return CoreResponse(
            status=response.status,
            headers=response.headers,
            content=PoolByteStream(
                stream=response.stream, pool_request=pool_request, pool=self
            ),
            extensions=response.extensions,
        )

    # Override httpcore's ConnectionPool.create_connection to only return our own AnalyticsHTTPConnection.
    # 2025-06-05: https://github.com/encode/httpcore/blob/98209758cc14e1a5f966fe1dfdc1064b94055d8c/httpcore/_sync/connection_pool.py#L128
    def create_connection(self, origin: Origin) -> ConnectionInterface:
        return AnalyticsHTTPConnection(
            origin=origin,
            ssl_context=self._ssl_context,
            keepalive_expiry=self._keepalive_expiry,
            http1=self._http1,
            http2=self._http2,
            retries=self._retries,
            local_address=self._local_address,
            uds=self._uds,
            network_backend=self._network_backend,
            socket_options=self._socket_options,
        )

class AnalyticsHTTPTransport(BaseTransport):
    def __init__(
        self,
        verify: Union[ssl.SSLContext, str, bool] = True,
        cert: Optional[CertTypes] = None,
        trust_env: bool = True,
        http1: bool = True,
        http2: bool = False,
        limits: Limits = DEFAULT_LIMITS,
        proxy: Optional[ProxyTypes] = None,
        uds: Optional[str] = None,
        local_address: Optional[str] = None,
        retries: int = 0,
        socket_options: Optional[Iterable[SOCKET_OPTION]] = None,
    ) -> None:

        proxy = Proxy(url=proxy) if isinstance(proxy, (str, URL)) else proxy
        ssl_context = create_ssl_context(verify=verify, cert=cert, trust_env=trust_env)

        self._pool = AnalyticsConnectionPool(
            ssl_context=ssl_context,
            max_connections=limits.max_connections,
            max_keepalive_connections=limits.max_keepalive_connections,
            keepalive_expiry=limits.keepalive_expiry,
            http1=http1,
            http2=http2,
            uds=uds,
            local_address=local_address,
            retries=retries,
            socket_options=socket_options,
        )

    def __enter__(self: T) -> T:  # type: ignore
        self._pool.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        with map_httpcore_exceptions():
            self._pool.__exit__(exc_type, exc_value, traceback)

    def handle_request(
        self,
        request: Request,  # type: ignore
    ) -> Response:
        assert isinstance(request.stream, SyncByteStream)
        import httpcore

        req = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,  # type: ignore
                host=request.url.raw_host,  # type: ignore
                port=request.url.port,
                target=request.url.raw_path,  # type: ignore
            ),
            headers=request.headers.raw,  # type: ignore
            content=request.stream,
            extensions=request.extensions,
        )
        with map_httpcore_exceptions():
            resp = self._pool.handle_request(req)

        assert isinstance(resp.stream, Iterable)

        return Response(
            status_code=resp.status,
            headers=resp.headers,
            stream=ResponseStream(resp.stream),
            extensions=resp.extensions,
        )

    def close(self) -> None:
        self._pool.close()