import random
from typing import Literal, Protocol, final
import urllib3
import urllib3.util
import requests
import google.protobuf.json_format as pbjson
from ducktape.cluster.cluster import ClusterNode
from rptest.clients.admin.proto.redpanda.core.admin import admin_pb2
from rptest.clients.admin.proto.redpanda.core.admin.internal import debug_pb2


class RedpandaServiceProto(Protocol):
    def started_nodes(self) -> list[ClusterNode]:
        ...


# Re-export some protobufs for convenience
admin_pb = admin_pb2
debug_pb = debug_pb2


@final
class Admin:
    """
    Wrapper for the Redpanda Admin v2 client.
    """
    def __init__(
            self,
            redpanda: RedpandaServiceProto,
            auth: tuple[str, str] | None = None,
            protocol: Literal['json'] | Literal['proto'] = 'json') -> None:
        self._rp = redpanda
        if auth != None:
            self._headers = urllib3.util.make_headers(
                basic_auth=f'{auth[0]}:{auth[1]}')
        else:
            self._headers = {}
        self._headers['Content-Type'] = f"application/{protocol}"
        self._protocol = protocol

    # TODO(rockwood): Auto generate the clients instead of manually crafting RPCs here

    def get_build_info(
            self, req: admin_pb.GetBuildInfoRequest) -> admin_pb.BuildInfo:
        node = random.choice(self._rp.started_nodes())
        if self._protocol == 'json':
            body = pbjson.MessageToJson(req).encode()
        else:
            body = req.SerializeToString()
        resp = requests.post(
            url=
            f"http://{node.account.hostname}:9644/v2/redpanda.core.admin.AdminService/GetBuildInfo",
            headers=self._headers,
            data=body,
        )
        resp.raise_for_status()
        if self._protocol == 'proto':
            return admin_pb.BuildInfo.FromString(resp.content)
        else:
            return pbjson.Parse(resp.content, admin_pb.BuildInfo())
