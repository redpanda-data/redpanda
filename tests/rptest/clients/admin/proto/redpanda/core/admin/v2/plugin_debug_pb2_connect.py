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
from ...... import proto

class PluginDebugServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_list_committed_offsets(self, req: proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsResponse]:
        """Low-level method to call ListCommittedOffsets, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginDebugService/ListCommittedOffsets'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsResponse, extra_headers, timeout_seconds)

    def list_committed_offsets(self, req: proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsResponse:
        response = self.call_list_committed_offsets(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_garbage_collect_offsets(self, req: proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsResponse]:
        """Low-level method to call GarbageCollectOffsets, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginDebugService/GarbageCollectOffsets'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsResponse, extra_headers, timeout_seconds)

    def garbage_collect_offsets(self, req: proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsResponse:
        response = self.call_garbage_collect_offsets(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncPluginDebugServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_list_committed_offsets(self, req: proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsResponse]:
        """Low-level method to call ListCommittedOffsets, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginDebugService/ListCommittedOffsets'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsResponse, extra_headers, timeout_seconds)

    async def list_committed_offsets(self, req: proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsResponse:
        response = await self.call_list_committed_offsets(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_garbage_collect_offsets(self, req: proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsResponse]:
        """Low-level method to call GarbageCollectOffsets, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginDebugService/GarbageCollectOffsets'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsResponse, extra_headers, timeout_seconds)

    async def garbage_collect_offsets(self, req: proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsResponse:
        response = await self.call_garbage_collect_offsets(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class PluginDebugServiceProtocol(typing.Protocol):

    def list_committed_offsets(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsResponse]:
        ...

    def garbage_collect_offsets(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsResponse]:
        ...
PLUGIN_DEBUG_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.PluginDebugService'

def wsgi_plugin_debug_service(implementation: PluginDebugServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginDebugService/ListCommittedOffsets', implementation.list_committed_offsets, proto.redpanda.core.admin.v2.plugin_debug_pb2.ListCommittedOffsetsRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginDebugService/GarbageCollectOffsets', implementation.garbage_collect_offsets, proto.redpanda.core.admin.v2.plugin_debug_pb2.GarbageCollectOffsetsRequest)
    return app