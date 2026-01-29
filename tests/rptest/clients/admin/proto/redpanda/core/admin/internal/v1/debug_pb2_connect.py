from __future__ import annotations
from collections.abc import AsyncIterator
from collections.abc import Iterator
from collections.abc import Iterable
import aiohttp
import urllib3
import typing
import sys
from connectrpc.client_async import AsyncConnectClient
from connectrpc.client_sync import ConnectClient
from connectrpc.client_protocol import ConnectProtocol
from connectrpc.client_connect import ConnectProtocolError
from connectrpc.headers import HeaderInput
from connectrpc.server import ClientRequest
from connectrpc.server import ClientStream
from connectrpc.server import ServerResponse
from connectrpc.server import ServerStream
from connectrpc.server_sync import ConnectWSGI
from connectrpc.streams import StreamInput
from connectrpc.streams import AsyncStreamOutput
from connectrpc.streams import StreamOutput
from connectrpc.unary import UnaryOutput
from connectrpc.unary import ClientStreamingOutput
if typing.TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from wsgiref.types import WSGIApplication
    else:
        from _typeshed.wsgi import WSGIApplication
from ....... import proto

class DebugServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_throw_structured_exception(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionResponse]:
        """Low-level method to call ThrowStructuredException, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.DebugService/ThrowStructuredException'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionResponse, extra_headers, timeout_seconds)

    def throw_structured_exception(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionResponse:
        response = self.call_throw_structured_exception(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_start_stress_fiber(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberResponse]:
        """Low-level method to call StartStressFiber, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.DebugService/StartStressFiber'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberResponse, extra_headers, timeout_seconds)

    def start_stress_fiber(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberResponse:
        response = self.call_start_stress_fiber(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_stop_stress_fiber(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberResponse]:
        """Low-level method to call StopStressFiber, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.DebugService/StopStressFiber'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberResponse, extra_headers, timeout_seconds)

    def stop_stress_fiber(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberResponse:
        response = self.call_stop_stress_fiber(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_log_message(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageResponse]:
        """Low-level method to call LogMessage, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.DebugService/LogMessage'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageResponse, extra_headers, timeout_seconds)

    def log_message(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageResponse:
        response = self.call_log_message(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncDebugServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_throw_structured_exception(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionResponse]:
        """Low-level method to call ThrowStructuredException, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.DebugService/ThrowStructuredException'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionResponse, extra_headers, timeout_seconds)

    async def throw_structured_exception(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionResponse:
        response = await self.call_throw_structured_exception(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_start_stress_fiber(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberResponse]:
        """Low-level method to call StartStressFiber, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.DebugService/StartStressFiber'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberResponse, extra_headers, timeout_seconds)

    async def start_stress_fiber(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberResponse:
        response = await self.call_start_stress_fiber(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_stop_stress_fiber(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberResponse]:
        """Low-level method to call StopStressFiber, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.DebugService/StopStressFiber'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberResponse, extra_headers, timeout_seconds)

    async def stop_stress_fiber(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberResponse:
        response = await self.call_stop_stress_fiber(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_log_message(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageResponse]:
        """Low-level method to call LogMessage, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.DebugService/LogMessage'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageResponse, extra_headers, timeout_seconds)

    async def log_message(self, req: proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageResponse:
        response = await self.call_log_message(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class DebugServiceProtocol(typing.Protocol):

    def throw_structured_exception(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionResponse]:
        ...

    def start_stress_fiber(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberResponse]:
        ...

    def stop_stress_fiber(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberResponse]:
        ...

    def log_message(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageResponse]:
        ...
DEBUG_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.internal.DebugService'

def wsgi_debug_service(implementation: DebugServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.internal.DebugService/ThrowStructuredException', implementation.throw_structured_exception, proto.redpanda.core.admin.internal.v1.debug_pb2.ThrowStructuredExceptionRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.internal.DebugService/StartStressFiber', implementation.start_stress_fiber, proto.redpanda.core.admin.internal.v1.debug_pb2.StartStressFiberRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.internal.DebugService/StopStressFiber', implementation.stop_stress_fiber, proto.redpanda.core.admin.internal.v1.debug_pb2.StopStressFiberRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.internal.DebugService/LogMessage', implementation.log_message, proto.redpanda.core.admin.internal.v1.debug_pb2.LogMessageRequest)
    return app