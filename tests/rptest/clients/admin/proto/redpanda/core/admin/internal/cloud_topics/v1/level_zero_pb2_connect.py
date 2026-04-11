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

class LevelZeroServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_get_status(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusResponse]:
        """Low-level method to call GetStatus, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetStatus'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusResponse, extra_headers, timeout_seconds)

    def get_status(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusResponse:
        response = self.call_get_status(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_start_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcResponse]:
        """Low-level method to call StartGc, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/StartGc'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcResponse, extra_headers, timeout_seconds)

    def start_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcResponse:
        response = self.call_start_gc(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_pause_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcResponse]:
        """Low-level method to call PauseGc, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/PauseGc'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcResponse, extra_headers, timeout_seconds)

    def pause_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcResponse:
        response = self.call_pause_gc(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_reset_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcResponse]:
        """Low-level method to call ResetGc, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/ResetGc'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcResponse, extra_headers, timeout_seconds)

    def reset_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcResponse:
        response = self.call_reset_gc(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_advance_epoch(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochResponse]:
        """Low-level method to call AdvanceEpoch, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/AdvanceEpoch'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochResponse, extra_headers, timeout_seconds)

    def advance_epoch(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochResponse:
        response = self.call_advance_epoch(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_epoch_info(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoResponse]:
        """Low-level method to call GetEpochInfo, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetEpochInfo'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoResponse, extra_headers, timeout_seconds)

    def get_epoch_info(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoResponse:
        response = self.call_get_epoch_info(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_size_estimate(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateResponse]:
        """Low-level method to call GetSizeEstimate, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetSizeEstimate'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateResponse, extra_headers, timeout_seconds)

    def get_size_estimate(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateResponse:
        response = self.call_get_size_estimate(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncLevelZeroServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_get_status(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusResponse]:
        """Low-level method to call GetStatus, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetStatus'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusResponse, extra_headers, timeout_seconds)

    async def get_status(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusResponse:
        response = await self.call_get_status(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_start_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcResponse]:
        """Low-level method to call StartGc, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/StartGc'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcResponse, extra_headers, timeout_seconds)

    async def start_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcResponse:
        response = await self.call_start_gc(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_pause_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcResponse]:
        """Low-level method to call PauseGc, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/PauseGc'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcResponse, extra_headers, timeout_seconds)

    async def pause_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcResponse:
        response = await self.call_pause_gc(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_reset_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcResponse]:
        """Low-level method to call ResetGc, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/ResetGc'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcResponse, extra_headers, timeout_seconds)

    async def reset_gc(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcResponse:
        response = await self.call_reset_gc(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_advance_epoch(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochResponse]:
        """Low-level method to call AdvanceEpoch, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/AdvanceEpoch'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochResponse, extra_headers, timeout_seconds)

    async def advance_epoch(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochResponse:
        response = await self.call_advance_epoch(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_epoch_info(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoResponse]:
        """Low-level method to call GetEpochInfo, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetEpochInfo'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoResponse, extra_headers, timeout_seconds)

    async def get_epoch_info(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoResponse:
        response = await self.call_get_epoch_info(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_size_estimate(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateResponse]:
        """Low-level method to call GetSizeEstimate, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetSizeEstimate'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateResponse, extra_headers, timeout_seconds)

    async def get_size_estimate(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateResponse:
        response = await self.call_get_size_estimate(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class LevelZeroServiceProtocol(typing.Protocol):

    def get_status(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusResponse]:
        ...

    def start_gc(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcResponse]:
        ...

    def pause_gc(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcResponse]:
        ...

    def reset_gc(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcResponse]:
        ...

    def advance_epoch(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochResponse]:
        ...

    def get_epoch_info(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoResponse]:
        ...

    def get_size_estimate(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateResponse]:
        ...
LEVEL_ZERO_SERVICE_PATH_PREFIX = '/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService'

def wsgi_level_zero_service(implementation: LevelZeroServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetStatus', implementation.get_status, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetStatusRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/StartGc', implementation.start_gc, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.StartGcRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/PauseGc', implementation.pause_gc, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.PauseGcRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/ResetGc', implementation.reset_gc, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.ResetGcRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/AdvanceEpoch', implementation.advance_epoch, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.AdvanceEpochRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetEpochInfo', implementation.get_epoch_info, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetEpochInfoRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.LevelZeroService/GetSizeEstimate', implementation.get_size_estimate, proto.redpanda.core.admin.internal.cloud_topics.v1.level_zero_pb2.GetSizeEstimateRequest)
    return app