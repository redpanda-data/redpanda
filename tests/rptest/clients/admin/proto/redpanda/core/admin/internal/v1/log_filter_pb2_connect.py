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

class LogFilterServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_set_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterResponse]:
        """Low-level method to call SetLogFilter, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.LogFilterService/SetLogFilter'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterResponse, extra_headers, timeout_seconds)

    def set_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterResponse:
        response = self.call_set_log_filter(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_reset_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterResponse]:
        """Low-level method to call ResetLogFilter, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.LogFilterService/ResetLogFilter'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterResponse, extra_headers, timeout_seconds)

    def reset_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterResponse:
        response = self.call_reset_log_filter(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterResponse]:
        """Low-level method to call GetLogFilter, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.LogFilterService/GetLogFilter'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterResponse, extra_headers, timeout_seconds)

    def get_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterResponse:
        response = self.call_get_log_filter(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_log_callsites(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesResponse]:
        """Low-level method to call ListLogCallsites, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.LogFilterService/ListLogCallsites'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesResponse, extra_headers, timeout_seconds)

    def list_log_callsites(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesResponse:
        response = self.call_list_log_callsites(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncLogFilterServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_set_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterResponse]:
        """Low-level method to call SetLogFilter, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.LogFilterService/SetLogFilter'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterResponse, extra_headers, timeout_seconds)

    async def set_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterResponse:
        response = await self.call_set_log_filter(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_reset_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterResponse]:
        """Low-level method to call ResetLogFilter, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.LogFilterService/ResetLogFilter'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterResponse, extra_headers, timeout_seconds)

    async def reset_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterResponse:
        response = await self.call_reset_log_filter(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterResponse]:
        """Low-level method to call GetLogFilter, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.LogFilterService/GetLogFilter'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterResponse, extra_headers, timeout_seconds)

    async def get_log_filter(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterResponse:
        response = await self.call_get_log_filter(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_log_callsites(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesResponse]:
        """Low-level method to call ListLogCallsites, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.internal.LogFilterService/ListLogCallsites'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesResponse, extra_headers, timeout_seconds)

    async def list_log_callsites(self, req: proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesResponse:
        response = await self.call_list_log_callsites(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class LogFilterServiceProtocol(typing.Protocol):

    def set_log_filter(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterResponse]:
        ...

    def reset_log_filter(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterResponse]:
        ...

    def get_log_filter(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterResponse]:
        ...

    def list_log_callsites(self, req: ClientRequest[proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesResponse]:
        ...
LOG_FILTER_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.internal.LogFilterService'

def wsgi_log_filter_service(implementation: LogFilterServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.internal.LogFilterService/SetLogFilter', implementation.set_log_filter, proto.redpanda.core.admin.internal.v1.log_filter_pb2.SetLogFilterRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.internal.LogFilterService/ResetLogFilter', implementation.reset_log_filter, proto.redpanda.core.admin.internal.v1.log_filter_pb2.ResetLogFilterRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.internal.LogFilterService/GetLogFilter', implementation.get_log_filter, proto.redpanda.core.admin.internal.v1.log_filter_pb2.GetLogFilterRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.internal.LogFilterService/ListLogCallsites', implementation.list_log_callsites, proto.redpanda.core.admin.internal.v1.log_filter_pb2.ListLogCallsitesRequest)
    return app