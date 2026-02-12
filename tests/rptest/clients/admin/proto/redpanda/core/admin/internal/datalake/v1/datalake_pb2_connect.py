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

class DatalakeServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_get_coordinator_state(self, req: proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateResponse]:
        """Low-level method to call GetCoordinatorState, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.datalake.v1.DatalakeService/GetCoordinatorState'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateResponse, extra_headers, timeout_seconds)

    def get_coordinator_state(self, req: proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateResponse:
        response = self.call_get_coordinator_state(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_coordinator_reset_topic_state(self, req: proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateResponse]:
        """Low-level method to call CoordinatorResetTopicState, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.datalake.v1.DatalakeService/CoordinatorResetTopicState'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateResponse, extra_headers, timeout_seconds)

    def coordinator_reset_topic_state(self, req: proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateResponse:
        response = self.call_coordinator_reset_topic_state(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncDatalakeServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_get_coordinator_state(self, req: proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateResponse]:
        """Low-level method to call GetCoordinatorState, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.datalake.v1.DatalakeService/GetCoordinatorState'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateResponse, extra_headers, timeout_seconds)

    async def get_coordinator_state(self, req: proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateResponse:
        response = await self.call_get_coordinator_state(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_coordinator_reset_topic_state(self, req: proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateResponse]:
        """Low-level method to call CoordinatorResetTopicState, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.datalake.v1.DatalakeService/CoordinatorResetTopicState'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateResponse, extra_headers, timeout_seconds)

    async def coordinator_reset_topic_state(self, req: proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateResponse:
        response = await self.call_coordinator_reset_topic_state(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class DatalakeServiceProtocol(typing.Protocol):

    def get_coordinator_state(self, req: ClientRequest[proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateResponse]:
        ...

    def coordinator_reset_topic_state(self, req: ClientRequest[proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateResponse]:
        ...
DATALAKE_SERVICE_PATH_PREFIX = '/redpanda.core.admin.internal.datalake.v1.DatalakeService'

def wsgi_datalake_service(implementation: DatalakeServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.internal.datalake.v1.DatalakeService/GetCoordinatorState', implementation.get_coordinator_state, proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.GetCoordinatorStateRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.datalake.v1.DatalakeService/CoordinatorResetTopicState', implementation.coordinator_reset_topic_state, proto.redpanda.core.admin.internal.datalake.v1.datalake_pb2.CoordinatorResetTopicStateRequest)
    return app