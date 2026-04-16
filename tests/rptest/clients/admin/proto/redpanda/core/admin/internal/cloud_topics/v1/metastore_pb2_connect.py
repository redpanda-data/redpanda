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

class MetastoreServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_get_offsets(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsResponse]:
        """Low-level method to call GetOffsets, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetOffsets'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsResponse, extra_headers, timeout_seconds)

    def get_offsets(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsResponse:
        response = self.call_get_offsets(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_size(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeResponse]:
        """Low-level method to call GetSize, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetSize'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeResponse, extra_headers, timeout_seconds)

    def get_size(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeResponse:
        response = self.call_get_size(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_database_stats(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsResponse]:
        """Low-level method to call GetDatabaseStats, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetDatabaseStats'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsResponse, extra_headers, timeout_seconds)

    def get_database_stats(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsResponse:
        response = self.call_get_database_stats(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_write_rows(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsResponse]:
        """Low-level method to call WriteRows, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/WriteRows'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsResponse, extra_headers, timeout_seconds)

    def write_rows(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsResponse:
        response = self.call_write_rows(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_read_rows(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsResponse]:
        """Low-level method to call ReadRows, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ReadRows'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsResponse, extra_headers, timeout_seconds)

    def read_rows(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsResponse:
        response = self.call_read_rows(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_cloud_topics(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsResponse]:
        """Low-level method to call ListCloudTopics, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ListCloudTopics'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsResponse, extra_headers, timeout_seconds)

    def list_cloud_topics(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsResponse:
        response = self.call_list_cloud_topics(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_validate_partition(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionResponse]:
        """Low-level method to call ValidatePartition, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ValidatePartition'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionResponse, extra_headers, timeout_seconds)

    def validate_partition(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionResponse:
        response = self.call_validate_partition(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncMetastoreServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_get_offsets(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsResponse]:
        """Low-level method to call GetOffsets, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetOffsets'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsResponse, extra_headers, timeout_seconds)

    async def get_offsets(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsResponse:
        response = await self.call_get_offsets(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_size(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeResponse]:
        """Low-level method to call GetSize, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetSize'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeResponse, extra_headers, timeout_seconds)

    async def get_size(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeResponse:
        response = await self.call_get_size(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_database_stats(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsResponse]:
        """Low-level method to call GetDatabaseStats, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetDatabaseStats'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsResponse, extra_headers, timeout_seconds)

    async def get_database_stats(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsResponse:
        response = await self.call_get_database_stats(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_write_rows(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsResponse]:
        """Low-level method to call WriteRows, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/WriteRows'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsResponse, extra_headers, timeout_seconds)

    async def write_rows(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsResponse:
        response = await self.call_write_rows(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_read_rows(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsResponse]:
        """Low-level method to call ReadRows, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ReadRows'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsResponse, extra_headers, timeout_seconds)

    async def read_rows(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsResponse:
        response = await self.call_read_rows(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_cloud_topics(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsResponse]:
        """Low-level method to call ListCloudTopics, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ListCloudTopics'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsResponse, extra_headers, timeout_seconds)

    async def list_cloud_topics(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsResponse:
        response = await self.call_list_cloud_topics(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_validate_partition(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionResponse]:
        """Low-level method to call ValidatePartition, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ValidatePartition'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionResponse, extra_headers, timeout_seconds)

    async def validate_partition(self, req: proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionResponse:
        response = await self.call_validate_partition(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class MetastoreServiceProtocol(typing.Protocol):

    def get_offsets(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsResponse]:
        ...

    def get_size(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeResponse]:
        ...

    def get_database_stats(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsResponse]:
        ...

    def write_rows(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsResponse]:
        ...

    def read_rows(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsResponse]:
        ...

    def list_cloud_topics(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsResponse]:
        ...

    def validate_partition(self, req: ClientRequest[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionRequest]) -> ServerResponse[proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionResponse]:
        ...
METASTORE_SERVICE_PATH_PREFIX = '/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService'

def wsgi_metastore_service(implementation: MetastoreServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetOffsets', implementation.get_offsets, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetOffsetsRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetSize', implementation.get_size, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetSizeRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/GetDatabaseStats', implementation.get_database_stats, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.GetDatabaseStatsRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/WriteRows', implementation.write_rows, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.WriteRowsRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ReadRows', implementation.read_rows, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ReadRowsRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ListCloudTopics', implementation.list_cloud_topics, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ListCloudTopicsRequest)
    app.register_unary_rpc('/redpanda.core.admin.internal.cloud_topics.v1.MetastoreService/ValidatePartition', implementation.validate_partition, proto.redpanda.core.admin.internal.cloud_topics.v1.metastore_pb2.ValidatePartitionRequest)
    return app