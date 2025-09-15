import random
from typing import Literal, Protocol, final

import urllib3
import urllib3.util
from connectrpc.client_protocol import ConnectProtocol
from ducktape.cluster.cluster import ClusterNode
from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    broker_pb2,
    broker_pb2_connect,
    shadow_link_pb2,
    shadow_link_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.admin.v2.internal import (
    debug_pb2,
    debug_pb2_connect,
)


class RedpandaServiceProto(Protocol):
    def started_nodes(self) -> list[ClusterNode]: ...


# Re-export some protobufs for convenience
broker_pb = broker_pb2
shadow_link_pb = shadow_link_pb2
debug_pb = debug_pb2


# A hacky workaround for https://github.com/connectrpc/connect-python/issues/37
class HeaderInjectingClient:
    def __init__(self, client, headers_to_inject: dict[str, str]):
        self.client = client
        self.headers_to_inject = headers_to_inject

    def call_unary(
        self,
        url: str,
        req,
        response_type,
        extra_headers: dict[str, str] | None = None,
        timeout_seconds: float | None = None,
    ):
        if extra_headers is None:
            extra_headers = self.headers_to_inject
        else:
            extra_headers = self.headers_to_inject | extra_headers
        return self.client.call_unary(
            url=url,
            req=req,
            response_type=response_type,
            extra_headers=extra_headers,
            timeout_seconds=timeout_seconds,
        )


@final
class Admin:
    """
    Wrapper for the Redpanda Admin v2 client.
    """

    def __init__(
        self,
        redpanda: RedpandaServiceProto,
        auth: tuple[str, str] | None = None,
        protocol: Literal["json"] | Literal["proto"] = "json",
    ) -> None:
        self._rp = redpanda
        if auth != None:
            self._headers = urllib3.util.make_headers(basic_auth=f"{auth[0]}:{auth[1]}")
        else:
            self._headers = {}
        self._protocol = protocol

    def _make_service(self, service_clazz):
        node = random.choice(self._rp.started_nodes())
        client = service_clazz(
            base_url=f"http://{node.account.hostname}:9644",
            protocol=ConnectProtocol.CONNECT_PROTOBUF
            if self._protocol == "proto"
            else ConnectProtocol.CONNECT_JSON,
        )
        client._connect_client = HeaderInjectingClient(
            client._connect_client, self._headers.copy()
        )
        return client

    def broker(self) -> broker_pb2_connect.BrokerServiceClient:
        return self._make_service(broker_pb2_connect.BrokerServiceClient)

    def debug(self) -> debug_pb2_connect.DebugServiceClient:
        return self._make_service(debug_pb2_connect.DebugServiceClient)

    def shadow_link(self) -> shadow_link_pb2_connect.ShadowLinkServiceClient:
        return self._make_service(shadow_link_pb2_connect.ShadowLinkServiceClient)
