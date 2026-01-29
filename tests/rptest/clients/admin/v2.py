import logging
import random
from typing import Literal, Protocol, final, Any

import urllib3
import urllib3.util
from connectrpc.client_protocol import ConnectProtocol
from ducktape.cluster.cluster import ClusterNode
from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    broker_pb2,
    broker_pb2_connect,
    cluster_pb2,
    cluster_pb2_connect,
    kafka_connections_pb2,
    security_pb2,
    security_pb2_connect,
    shadow_link_pb2,
    shadow_link_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.admin.internal.datalake.v1 import (
    datalake_pb2,
    datalake_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.admin.internal.v1 import (
    debug_pb2,
    debug_pb2_connect,
    breakglass_pb2,
    breakglass_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.admin.internal.shadow_link_internal.v1 import (
    shadow_link_internal_pb2,
    shadow_link_internal_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.admin.internal.cloud_topics.v1 import (
    metastore_pb2,
    metastore_pb2_connect,
)
from rptest.clients.admin.proto.redpanda.core.common.v1 import ntp_pb2


class RedpandaServiceProto(Protocol):
    def started_nodes(self) -> list[ClusterNode]: ...

    @property
    def logger(self) -> logging.Logger: ...


# Re-export some protobufs for convenience
broker_pb = broker_pb2
cluster_pb = cluster_pb2
datalake_pb = datalake_pb2
security_pb2 = security_pb2
shadow_link_pb = shadow_link_pb2
shadow_link_internal_pb = shadow_link_internal_pb2
debug_pb = debug_pb2
kafka_connections_pb = kafka_connections_pb2
breakglass_pb = breakglass_pb2
metastore_pb = metastore_pb2
ntp_pb = ntp_pb2


# A hacky workaround for https://github.com/connectrpc/connect-python/issues/37
class HeaderInjectingClient:
    def __init__(
        self, client, logger: logging.Logger, headers_to_inject: dict[str, str]
    ):
        self.client = client
        self.logger = logger
        self.headers_to_inject = headers_to_inject

    def call_unary(
        self,
        url: str,
        req,
        response_type,
        extra_headers: dict[str, str] | None = None,
        timeout_seconds: float | None = None,
    ):
        self.logger.debug(f"making admin RPC {url}")
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
        if auth is not None:
            self._headers = urllib3.util.make_headers(basic_auth=f"{auth[0]}:{auth[1]}")
        else:
            self._headers = {}
        self._protocol = protocol

    def _make_service(self, service_clazz, node: ClusterNode | None = None):
        if not node:
            node = random.choice(self._rp.started_nodes())
            assert node, "must have at least one started node"
        client = service_clazz(
            base_url=f"http://{node.account.hostname}:9644",
            protocol=ConnectProtocol.CONNECT_PROTOBUF
            if self._protocol == "proto"
            else ConnectProtocol.CONNECT_JSON,
        )
        client._connect_client = HeaderInjectingClient(
            client._connect_client, self._rp.logger, self._headers.copy()
        )
        return client

    def broker(self, **kwargs: Any) -> broker_pb2_connect.BrokerServiceClient:
        return self._make_service(broker_pb2_connect.BrokerServiceClient, **kwargs)

    def cluster(self, **kwargs: Any) -> cluster_pb2_connect.ClusterServiceClient:
        return self._make_service(cluster_pb2_connect.ClusterServiceClient, **kwargs)

    def datalake(self, **kwargs: Any) -> datalake_pb2_connect.DatalakeServiceClient:
        return self._make_service(datalake_pb2_connect.DatalakeServiceClient, **kwargs)

    def debug(self, **kwargs: Any) -> debug_pb2_connect.DebugServiceClient:
        return self._make_service(debug_pb2_connect.DebugServiceClient, **kwargs)

    def security(self, **kwargs: Any) -> security_pb2_connect.SecurityServiceClient:
        return self._make_service(security_pb2_connect.SecurityServiceClient, **kwargs)

    def shadow_link(
        self, **kwargs: Any
    ) -> shadow_link_pb2_connect.ShadowLinkServiceClient:
        return self._make_service(
            shadow_link_pb2_connect.ShadowLinkServiceClient, **kwargs
        )

    def breakglass(
        self, **kwargs: Any
    ) -> breakglass_pb2_connect.BreakglassServiceClient:
        return self._make_service(
            breakglass_pb2_connect.BreakglassServiceClient, **kwargs
        )

    def metastore(self, **kwargs: Any) -> metastore_pb2_connect.MetastoreServiceClient:
        return self._make_service(
            metastore_pb2_connect.MetastoreServiceClient, **kwargs
        )

    def internal_shadow_link(
        self, **kwargs: Any
    ) -> shadow_link_internal_pb2_connect.ShadowLinkInternalServiceClient:
        return self._make_service(
            shadow_link_internal_pb2_connect.ShadowLinkInternalServiceClient, **kwargs
        )
