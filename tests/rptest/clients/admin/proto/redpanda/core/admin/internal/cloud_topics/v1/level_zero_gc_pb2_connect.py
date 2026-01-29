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
from ........ import proto

class LevelZeroGcServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_get_status(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusResponse]:
        """Low-level method to call GetStatus, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/GetStatus'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusResponse, extra_headers, timeout_seconds)

    def get_status(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusResponse:
        response = self.call_get_status(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_start(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartResponse]:
        """Low-level method to call Start, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/Start'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartResponse, extra_headers, timeout_seconds)

    def start(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartResponse:
        response = self.call_start(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_pause(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseResponse]:
        """Low-level method to call Pause, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/Pause'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseResponse, extra_headers, timeout_seconds)

    def pause(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseResponse:
        response = self.call_pause(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_advance_epoch(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochResponse]:
        """Low-level method to call AdvanceEpoch, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/AdvanceEpoch'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochResponse, extra_headers, timeout_seconds)

    def advance_epoch(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochResponse:
        response = self.call_advance_epoch(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_epoch_info(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoResponse]:
        """Low-level method to call GetEpochInfo, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/GetEpochInfo'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoResponse, extra_headers, timeout_seconds)

    def get_epoch_info(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoResponse:
        response = self.call_get_epoch_info(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncLevelZeroGcServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_get_status(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusResponse]:
        """Low-level method to call GetStatus, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/GetStatus'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusResponse, extra_headers, timeout_seconds)

    async def get_status(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusResponse:
        response = await self.call_get_status(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_start(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartResponse]:
        """Low-level method to call Start, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/Start'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartResponse, extra_headers, timeout_seconds)

    async def start(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartResponse:
        response = await self.call_start(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_pause(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseResponse]:
        """Low-level method to call Pause, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/Pause'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseResponse, extra_headers, timeout_seconds)

    async def pause(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseResponse:
        response = await self.call_pause(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_advance_epoch(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochResponse]:
        """Low-level method to call AdvanceEpoch, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/AdvanceEpoch'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochResponse, extra_headers, timeout_seconds)

    async def advance_epoch(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochResponse:
        response = await self.call_advance_epoch(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_epoch_info(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoResponse]:
        """Low-level method to call GetEpochInfo, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/GetEpochInfo'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoResponse, extra_headers, timeout_seconds)

    async def get_epoch_info(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoResponse:
        response = await self.call_get_epoch_info(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class LevelZeroGcServiceProtocol(typing.Protocol):

    def get_status(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusResponse]:
        ...

    def start(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartResponse]:
        ...

    def pause(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseResponse]:
        ...

    def advance_epoch(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochResponse]:
        ...

    def get_epoch_info(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoResponse]:
        ...
LEVEL_ZERO_GC_SERVICE_PATH_PREFIX = '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService'

def wsgi_level_zero_gc_service(implementation: LevelZeroGcServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/GetStatus', implementation.get_status, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetStatusRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/Start', implementation.start, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.StartRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/Pause', implementation.pause, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.PauseRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/AdvanceEpoch', implementation.advance_epoch, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.AdvanceEpochRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroGcService/GetEpochInfo', implementation.get_epoch_info, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_gc_pb2.GetEpochInfoRequest)
    return app