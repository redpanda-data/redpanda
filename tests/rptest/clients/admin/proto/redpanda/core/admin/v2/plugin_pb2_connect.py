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

class PluginServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_create_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformResponse]:
        """Low-level method to call CreateTransform, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/CreateTransform'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformResponse, extra_headers, timeout_seconds)

    def create_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformResponse:
        response = self.call_create_transform(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.GetTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.GetTransformResponse]:
        """Low-level method to call GetTransform, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/GetTransform'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.GetTransformResponse, extra_headers, timeout_seconds)

    def get_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.GetTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.GetTransformResponse:
        response = self.call_get_transform(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_transforms(self, req: proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsResponse]:
        """Low-level method to call ListTransforms, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/ListTransforms'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsResponse, extra_headers, timeout_seconds)

    def list_transforms(self, req: proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsResponse:
        response = self.call_list_transforms(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_update_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformResponse]:
        """Low-level method to call UpdateTransform, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/UpdateTransform'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformResponse, extra_headers, timeout_seconds)

    def update_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformResponse:
        response = self.call_update_transform(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_delete_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformResponse]:
        """Low-level method to call DeleteTransform, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/DeleteTransform'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformResponse, extra_headers, timeout_seconds)

    def delete_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformResponse:
        response = self.call_delete_transform(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_create_binary(self, req: proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryResponse]:
        """Low-level method to call CreateBinary, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/CreateBinary'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryResponse, extra_headers, timeout_seconds)

    def create_binary(self, req: proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryResponse:
        response = self.call_create_binary(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_binaries(self, req: proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesResponse]:
        """Low-level method to call ListBinaries, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/ListBinaries'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesResponse, extra_headers, timeout_seconds)

    def list_binaries(self, req: proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesResponse:
        response = self.call_list_binaries(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_delete_binary(self, req: proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryResponse]:
        """Low-level method to call DeleteBinary, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/DeleteBinary'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryResponse, extra_headers, timeout_seconds)

    def delete_binary(self, req: proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryResponse:
        response = self.call_delete_binary(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncPluginServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_create_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformResponse]:
        """Low-level method to call CreateTransform, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/CreateTransform'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformResponse, extra_headers, timeout_seconds)

    async def create_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformResponse:
        response = await self.call_create_transform(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.GetTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.GetTransformResponse]:
        """Low-level method to call GetTransform, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/GetTransform'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.GetTransformResponse, extra_headers, timeout_seconds)

    async def get_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.GetTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.GetTransformResponse:
        response = await self.call_get_transform(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_transforms(self, req: proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsResponse]:
        """Low-level method to call ListTransforms, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/ListTransforms'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsResponse, extra_headers, timeout_seconds)

    async def list_transforms(self, req: proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsResponse:
        response = await self.call_list_transforms(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_update_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformResponse]:
        """Low-level method to call UpdateTransform, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/UpdateTransform'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformResponse, extra_headers, timeout_seconds)

    async def update_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformResponse:
        response = await self.call_update_transform(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_delete_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformResponse]:
        """Low-level method to call DeleteTransform, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/DeleteTransform'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformResponse, extra_headers, timeout_seconds)

    async def delete_transform(self, req: proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformResponse:
        response = await self.call_delete_transform(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_create_binary(self, req: proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryResponse]:
        """Low-level method to call CreateBinary, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/CreateBinary'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryResponse, extra_headers, timeout_seconds)

    async def create_binary(self, req: proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryResponse:
        response = await self.call_create_binary(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_binaries(self, req: proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesResponse]:
        """Low-level method to call ListBinaries, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/ListBinaries'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesResponse, extra_headers, timeout_seconds)

    async def list_binaries(self, req: proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesResponse:
        response = await self.call_list_binaries(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_delete_binary(self, req: proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryResponse]:
        """Low-level method to call DeleteBinary, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.PluginService/DeleteBinary'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryResponse, extra_headers, timeout_seconds)

    async def delete_binary(self, req: proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryResponse:
        response = await self.call_delete_binary(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class PluginServiceProtocol(typing.Protocol):

    def create_transform(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformResponse]:
        ...

    def get_transform(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_pb2.GetTransformRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_pb2.GetTransformResponse]:
        ...

    def list_transforms(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsResponse]:
        ...

    def update_transform(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformResponse]:
        ...

    def delete_transform(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformResponse]:
        ...

    def create_binary(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryResponse]:
        ...

    def list_binaries(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesResponse]:
        ...

    def delete_binary(self, req: ClientRequest[proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryResponse]:
        ...
PLUGIN_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.PluginService'

def wsgi_plugin_service(implementation: PluginServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginService/CreateTransform', implementation.create_transform, proto.redpanda.core.admin.v2.plugin_pb2.CreateTransformRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginService/GetTransform', implementation.get_transform, proto.redpanda.core.admin.v2.plugin_pb2.GetTransformRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginService/ListTransforms', implementation.list_transforms, proto.redpanda.core.admin.v2.plugin_pb2.ListTransformsRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginService/UpdateTransform', implementation.update_transform, proto.redpanda.core.admin.v2.plugin_pb2.UpdateTransformRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginService/DeleteTransform', implementation.delete_transform, proto.redpanda.core.admin.v2.plugin_pb2.DeleteTransformRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginService/CreateBinary', implementation.create_binary, proto.redpanda.core.admin.v2.plugin_pb2.CreateBinaryRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginService/ListBinaries', implementation.list_binaries, proto.redpanda.core.admin.v2.plugin_pb2.ListBinariesRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.PluginService/DeleteBinary', implementation.delete_binary, proto.redpanda.core.admin.v2.plugin_pb2.DeleteBinaryRequest)
    return app