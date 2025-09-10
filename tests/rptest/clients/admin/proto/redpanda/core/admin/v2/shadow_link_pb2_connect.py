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

class ShadowLinkServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_create_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkResponse]:
        """Low-level method to call CreateShadowLink, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/CreateShadowLink'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkResponse, extra_headers, timeout_seconds)

    def create_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkResponse:
        response = self.call_create_shadow_link(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_delete_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkResponse]:
        """Low-level method to call DeleteShadowLink, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/DeleteShadowLink'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkResponse, extra_headers, timeout_seconds)

    def delete_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkResponse:
        response = self.call_delete_shadow_link(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkResponse]:
        """Low-level method to call GetShadowLink, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/GetShadowLink'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkResponse, extra_headers, timeout_seconds)

    def get_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkResponse:
        response = self.call_get_shadow_link(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_shadow_links(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksResponse]:
        """Low-level method to call ListShadowLinks, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/ListShadowLinks'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksResponse, extra_headers, timeout_seconds)

    def list_shadow_links(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksResponse:
        response = self.call_list_shadow_links(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_update_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkResponse]:
        """Low-level method to call UpdateShadowLink, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/UpdateShadowLink'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkResponse, extra_headers, timeout_seconds)

    def update_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkResponse:
        response = self.call_update_shadow_link(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_fail_over(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverResponse]:
        """Low-level method to call FailOver, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/FailOver'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverResponse, extra_headers, timeout_seconds)

    def fail_over(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverResponse:
        response = self.call_fail_over(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_truncate_and_restore(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreResponse]:
        """Low-level method to call TruncateAndRestore, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/TruncateAndRestore'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreResponse, extra_headers, timeout_seconds)

    def truncate_and_restore(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreResponse:
        response = self.call_truncate_and_restore(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncShadowLinkServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_create_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkResponse]:
        """Low-level method to call CreateShadowLink, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/CreateShadowLink'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkResponse, extra_headers, timeout_seconds)

    async def create_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkResponse:
        response = await self.call_create_shadow_link(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_delete_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkResponse]:
        """Low-level method to call DeleteShadowLink, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/DeleteShadowLink'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkResponse, extra_headers, timeout_seconds)

    async def delete_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkResponse:
        response = await self.call_delete_shadow_link(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkResponse]:
        """Low-level method to call GetShadowLink, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/GetShadowLink'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkResponse, extra_headers, timeout_seconds)

    async def get_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkResponse:
        response = await self.call_get_shadow_link(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_shadow_links(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksResponse]:
        """Low-level method to call ListShadowLinks, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/ListShadowLinks'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksResponse, extra_headers, timeout_seconds)

    async def list_shadow_links(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksResponse:
        response = await self.call_list_shadow_links(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_update_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkResponse]:
        """Low-level method to call UpdateShadowLink, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/UpdateShadowLink'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkResponse, extra_headers, timeout_seconds)

    async def update_shadow_link(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkResponse:
        response = await self.call_update_shadow_link(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_fail_over(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverResponse]:
        """Low-level method to call FailOver, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/FailOver'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverResponse, extra_headers, timeout_seconds)

    async def fail_over(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverResponse:
        response = await self.call_fail_over(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_truncate_and_restore(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreResponse]:
        """Low-level method to call TruncateAndRestore, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.ShadowLinkService/TruncateAndRestore'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreResponse, extra_headers, timeout_seconds)

    async def truncate_and_restore(self, req: proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreResponse:
        response = await self.call_truncate_and_restore(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class ShadowLinkServiceProtocol(typing.Protocol):

    def create_shadow_link(self, req: ClientRequest[proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkResponse]:
        ...

    def delete_shadow_link(self, req: ClientRequest[proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkResponse]:
        ...

    def get_shadow_link(self, req: ClientRequest[proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkResponse]:
        ...

    def list_shadow_links(self, req: ClientRequest[proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksResponse]:
        ...

    def update_shadow_link(self, req: ClientRequest[proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkResponse]:
        ...

    def fail_over(self, req: ClientRequest[proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverResponse]:
        ...

    def truncate_and_restore(self, req: ClientRequest[proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreResponse]:
        ...
SHADOW_LINK_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.ShadowLinkService'

def wsgi_shadow_link_service(implementation: ShadowLinkServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.ShadowLinkService/CreateShadowLink', implementation.create_shadow_link, proto.redpanda.core.admin.v2.shadow_link_pb2.CreateShadowLinkRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.ShadowLinkService/DeleteShadowLink', implementation.delete_shadow_link, proto.redpanda.core.admin.v2.shadow_link_pb2.DeleteShadowLinkRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.ShadowLinkService/GetShadowLink', implementation.get_shadow_link, proto.redpanda.core.admin.v2.shadow_link_pb2.GetShadowLinkRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.ShadowLinkService/ListShadowLinks', implementation.list_shadow_links, proto.redpanda.core.admin.v2.shadow_link_pb2.ListShadowLinksRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.ShadowLinkService/UpdateShadowLink', implementation.update_shadow_link, proto.redpanda.core.admin.v2.shadow_link_pb2.UpdateShadowLinkRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.ShadowLinkService/FailOver', implementation.fail_over, proto.redpanda.core.admin.v2.shadow_link_pb2.FailOverRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.ShadowLinkService/TruncateAndRestore', implementation.truncate_and_restore, proto.redpanda.core.admin.v2.shadow_link_pb2.TruncateAndRestoreRequest)
    return app