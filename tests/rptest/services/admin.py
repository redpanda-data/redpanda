# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import random
import time
import urllib.parse
from dataclasses import dataclass
from enum import Enum
from json.decoder import JSONDecodeError
from logging import Logger
from typing import Any, Callable, NamedTuple, Optional, Protocol, overload
from uuid import UUID

import requests
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from requests import Response
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException
from urllib3.util.retry import Retry

from rptest.services.redpanda_types import SaslCredentials
from rptest.util import not_none, wait_until_result
from rptest.utils.mode_checks import is_debug_mode

DEFAULT_TIMEOUT = 30

MaybeNode = ClusterNode | None


# The Admin class is used by RedpandaService and we also pass
# a RedpandaService to Admin instances which use it for logging
# among other things. So a circular dependency though not at the
# runtime level (only RedpandaService imports Admin, Admin just uses
# service objects passed in from outside and doesn't need to import
# them). However, from a type checking point of view this circular
# dependency is real. There a few workarounds, but mine is just to use
# a small protocol to stand-in for RedpandaService with the properties
# we actually use.
class RedpandaServiceProto(Protocol):
    nodes: list[ClusterNode]

    @property
    def logger(self) -> Logger: ...

    def node_id(
        self, node: ClusterNode, force_refresh: bool = False, timeout_sec: int = 30
    ) -> int: ...

    def get_node(self, idx: int) -> ClusterNode: ...

    def started_nodes(self) -> list[ClusterNode]: ...


class AuthPreservingSession(requests.Session):
    """
    Override `requests` default behaviour of dropping Authorization
    headers when redirecting.  This makes sense as a general default,
    but in the case of the redpanda admin API, we trust the server to
    only redirect us to other equally privileged servers within
    the same cluster.
    """

    def should_strip_auth(self, old_url: str, new_url: str) -> bool:
        return False


class Replica:
    node_id: int
    core: int

    def __init__(self, replica_dict: dict[str, Any]) -> None:
        self.node_id = replica_dict["node_id"]
        self.core = replica_dict["core"]

    def __getitem__(self, item_type: str) -> int | None:
        if item_type == "node_id":
            return self.node_id
        elif item_type == "core":
            return self.core
        return None


class PartitionDetails:
    replicas: list[Replica]
    leader: int | None
    status: str | None

    def __init__(self) -> None:
        self.replicas = []
        self.leader = None
        self.status = None


class RedpandaNode(NamedTuple):
    # host or ip address
    ip: str
    # node id in redpanda.yaml
    id: int


class CommittedWasmOffset(NamedTuple):
    name: str
    partition: int
    offset: int


class RoleErrorCode(Enum):
    MALFORMED_DEF = 40001
    INVALID_NAME = 40002
    UNRECOGNIZED_FIELD = 40003
    MEMBER_LIST_CONFLICT = 40004
    ROLE_NOT_FOUND = 40401
    ROLE_ALERADY_EXISTS = 40901
    ROLE_NAME_CONFLICT = 40902


class RoleError:
    def __init__(self, code: RoleErrorCode, message: str):
        self.code = code
        self.message = message

    @classmethod
    def from_json(cls, body: str):
        data = json.loads(body)
        return cls(RoleErrorCode(data["code"]), data["message"])

    @classmethod
    def from_http_error(cls, e: HTTPError):
        data = e.response.json()
        return cls.from_json(data["message"])


class RoleUpdate(NamedTuple):
    role: str


class RoleDescription(NamedTuple):
    name: str


class RolesList:
    def __init__(self, roles: list[RoleDescription]):
        self.roles = roles

    def __getitem__(self, i: int) -> RoleDescription:
        return self.roles[i]

    def __len__(self) -> int:
        return len(self.roles)

    def __str__(self) -> str:
        return json.dumps(self.roles)

    @classmethod
    def from_json(cls, body: bytes) -> "RolesList":
        d = json.loads(body)
        for k in d:
            assert k == "roles", f"Unexpected key {k}"
        return cls([RoleDescription(**r) for r in d.get("roles", [])])

    @classmethod
    def from_response(cls, rsp: Response) -> "RolesList":
        return cls.from_json(rsp.content)


class RoleMember(NamedTuple):
    class PrincipalType(str, Enum):
        USER = "User"

    principal_type: PrincipalType
    name: str

    @classmethod
    def User(cls, name: str) -> "RoleMember":
        return cls(cls.PrincipalType.USER, name)


class RoleMemberList:
    members: list[RoleMember]

    def __init__(self, mems: list[dict[str, Any]] | None = None) -> None:
        if mems is None:
            mems = []
        self.members = [RoleMember(**m) for m in mems]

    def __getitem__(self, i: int) -> RoleMember:
        return self.members[i]

    def __len__(self) -> int:
        return len(self.members)

    def __str__(self) -> str:
        return str(self.members)

    @classmethod
    def from_json(cls, body: bytes) -> "RoleMemberList":
        d = json.loads(body)
        for k in d:
            assert k == "members", f"Unexpected key {k}"
        return cls(d["members"])

    # TODO(oren): factor this out to a base class maybe
    @classmethod
    def from_response(cls, rsp: Response) -> "RoleMemberList":
        return cls.from_json(rsp.content)


class Role:
    name: str
    members: RoleMemberList

    def __init__(self, name: str, members: RoleMemberList | None = None) -> None:
        self.name = name
        self.members = members if members is not None else RoleMemberList()

    @classmethod
    def from_json(cls, body: bytes) -> "Role":
        d = json.loads(body)
        expected_keys = set(["name", "members"])
        assert all(k in expected_keys for k in d), f"Unexpected key(s): {d.keys()}"
        assert "name" in d, "Expected 'name' key"
        name = d["name"]
        members = RoleMemberList(d.get("members", []))
        return cls(name, members=members)

    @classmethod
    def from_response(cls, rsp: Response) -> "Role":
        return cls.from_json(rsp.content)


class RoleMemberUpdateResponse:
    role: str
    added: RoleMemberList
    removed: RoleMemberList
    created: bool

    def __init__(
        self,
        role: str,
        added: RoleMemberList | None = None,
        removed: RoleMemberList | None = None,
        created: bool = False,
    ) -> None:
        self.role = role
        self.added = added if added is not None else RoleMemberList()
        self.removed = removed if removed is not None else RoleMemberList()
        self.created = created

    def __str__(self) -> str:
        return json.dumps(
            {
                "role": self.role,
                "added": [a for a in self.added],
                "removed": [r for r in self.removed],
                "created": self.created,
            }
        )

    @classmethod
    def from_json(cls, body: bytes) -> "RoleMemberUpdateResponse":
        d = json.loads(body)
        expected_keys = set(["role", "added", "removed", "created"])
        assert all(k in expected_keys for k in d), f"Unexpected key(s): {d.keys()}"
        assert "role" in d, "Expected 'role' key"
        role = d["role"]
        kwargs: dict[str, Any] = {}
        kwargs["added"] = RoleMemberList(d.get("added", []))
        kwargs["removed"] = RoleMemberList(d.get("removed", []))
        kwargs["created"] = d.get("created", False)
        return cls(role, **kwargs)

    @classmethod
    def from_response(cls, rsp: Response) -> "RoleMemberUpdateResponse":
        return cls.from_json(rsp.content)


class NamespacedTopic:
    def __init__(self, topic: str, namespace: str | None = "kafka") -> None:
        self.ns = namespace
        self.topic = topic

    def as_dict(self) -> dict[str, str]:
        ret: dict[str, str] = {"topic": self.topic}
        if self.ns is not None:
            ret["ns"] = self.ns
        return ret

    @classmethod
    def from_json(cls, body: bytes) -> "NamespacedTopic":
        d = json.loads(body)
        expected_keys = set(["ns", "topic"])
        assert all(k in expected_keys for k in d), f"Unexpected key(s): {d.keys()}"
        assert "topic" in d, "Expected 'topic' key"
        topic = d["topic"]
        namespace = "kafka"
        if "ns" in d:
            namespace = d["ns"]

        return cls(topic, namespace)


class OutboundDataMigration:
    migration_type: str
    topics: list[NamespacedTopic]
    consumer_groups: list[str]

    def __init__(
        self, topics: list[NamespacedTopic], consumer_groups: list[str]
    ) -> None:
        self.migration_type = "outbound"
        self.topics = topics
        self.consumer_groups = consumer_groups

    @classmethod
    def from_json(cls, body: bytes) -> "OutboundDataMigration":
        d = json.loads(body)
        expected_keys = set(["type", "topics", "consumer_groups"])
        assert all(k in expected_keys for k in d), f"Unexpected key(s): {d.keys()}"
        assert all(k in d for k in expected_keys), (
            f"Missing keys: {expected_keys - set(d)}"
        )

        return cls(d["topics"], d["consumer_groups"])

    def as_dict(self) -> dict[str, Any]:
        return {
            "migration_type": self.migration_type,
            "topics": [t.as_dict() for t in self.topics],
            "consumer_groups": self.consumer_groups,
        }


class InboundTopic:
    def __init__(
        self,
        source_topic_reference: NamespacedTopic,
        alias: NamespacedTopic | None = None,
    ) -> None:
        """
        source_topic_reference is the topic location in cloud storage.
        source_topic_reference.topic can be just a topic name (if there is just one instance
        of topic data in cloud storage), or a full location of the form
        "<original topic name>/<original cluster uuid>/<original revision>".

        alias is the name of the topic that will be created in the cluster
        (if None, name from source_topic_reference is used).
        """

        self.source_topic_reference = source_topic_reference
        self.alias = alias

    def as_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "source_topic_reference": self.source_topic_reference.as_dict(),
        }
        if self.alias:
            d["alias"] = self.alias.as_dict()
        return d


class InboundDataMigration:
    migration_type: str
    topics: list[InboundTopic]
    consumer_groups: list[str]

    def __init__(self, topics: list[InboundTopic], consumer_groups: list[str]) -> None:
        self.migration_type = "inbound"
        self.topics = topics
        self.consumer_groups = consumer_groups

    def as_dict(self) -> dict[str, Any]:
        return {
            "migration_type": self.migration_type,
            "topics": [t.as_dict() for t in self.topics],
            "consumer_groups": self.consumer_groups,
        }


class MigrationAction(Enum):
    prepare = "prepare"
    execute = "execute"
    finish = "finish"
    cancel = "cancel"


class EnterpriseLicenseStatus(Enum):
    valid = "valid"
    expired = "expired"
    not_present = "not_present"


class DebugBundleEncoder(json.JSONEncoder):
    """
    DebugBundleEncoder is a custom JSON encoder that extends the default JSONEncoder
    to handle named tuples and UUIDs.

    Attributes:
        ignore_none (bool): If True, fields with None values are ignored during encoding.

    Methods:
        default(o):
            Overrides the default method to provide custom serialization for named tuples
            and UUIDs. Named tuples are converted to dictionaries, and UUIDs are converted
            to strings. Other types are handled by the superclass method.

        encode(o: Any) -> str:
            Overrides the encode method to ensure that the custom default method is used
            during encoding.

    Usage:
        encoder = DebugBundleEncoder(ignore_none=True)
        json_str = encoder.encode(your_object)
    """

    def __init__(self, *args: Any, ignore_none: bool = False, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.ignore_none = ignore_none

    def default(
        self, o: Any
    ) -> dict[str, Any] | str | list[Any] | tuple[Any, ...] | int | float | bool | None:
        if hasattr(o, "_fields") and isinstance(o, tuple):
            return {
                k: self.default(v)
                for k, v in o._asdict().items()  # type: ignore[attr-defined]
                if not (self.ignore_none and v is None)
            }
        if isinstance(o, UUID):
            return str(o)
        if isinstance(o, (dict, list, tuple, str, int, float, bool)) or o is None:
            return o  # type: ignore[reportUnknownVariableType]
        if hasattr(o, "__dataclass_fields__"):  # assume SaslCredentials
            creds: dict[str, Any] = o.__dict__
            if isinstance(o, SaslCredentials):
                # Swap algorithm for mechansim
                creds["mechanism"] = creds.pop("algorithm")
            return creds
        return super().default(o)

    def encode(self, o: Any) -> str:
        return super().encode(self.default(o))


@dataclass
class DebugBundleLabelSelection:
    key: str
    value: str


class DebugBundleStartConfigParams(NamedTuple):
    authentication: Optional[SaslCredentials] = None
    controller_logs_size_limit_bytes: Optional[int] = None
    cpu_profiler_wait_seconds: Optional[int] = None
    logs_since: Optional[str] = None
    logs_size_limit_bytes: Optional[int] = None
    logs_until: Optional[str] = None
    metrics_interval_seconds: Optional[int] = None
    metrics_samples: Optional[int] = None
    partition: Optional[list[str]] = None
    tls_enabled: Optional[bool] = None
    tls_insecure_skip_verify: Optional[bool] = None
    namespace: Optional[str] = None
    label_selector: Optional[list[DebugBundleLabelSelection]] = None


class DebugBundleStartConfig(NamedTuple):
    job_id: UUID
    config: Optional[DebugBundleStartConfigParams] = None


class CrashType(Enum):
    SEGFAULT = "segfault"
    ABORT = "abort"
    ASSERT = "assert"
    ASAN_CRASH = "asan_crash"
    UBSAN_CRASH = "ubsan_crash"


class Admin:
    """
    Wrapper for Redpanda admin REST API.

    All methods on this class will raise on errors.  For GETs the return
    value is a decoded dict of the JSON payload, for other requests
    the successful HTTP response object is returned.
    """

    def __init__(
        self,
        redpanda: RedpandaServiceProto,
        default_node: ClusterNode | None = None,
        retry_codes: list[int] | None = None,
        auth: tuple[str, str] | None = None,
        retries_amount: int = 5,
        timeout_seconds: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.redpanda = redpanda

        self._session = AuthPreservingSession()
        if auth is not None:
            self._session.auth = auth

        self._default_node: ClusterNode | None = default_node

        # - We retry on 503s because at any time a POST to a leader-redirected
        # request will return 503 if the partition is leaderless -- this is common
        # at the very start of a test when working with the controller partition to
        # e.g. create users.
        # - We do not let urllib retry on connection errors, because we need to do our
        # own logic in _request for trying a different node in that case.
        # - If the caller wants to handle 503s directly, they can set retry_codes to []
        if retry_codes is None:
            retry_codes = [503]

        retries = Retry(
            status=retries_amount,
            connect=0,
            read=0,
            backoff_factor=1,
            status_forcelist=retry_codes,
            respect_retry_after_header=True,
            allowed_methods=None,  # Retry all methods
            remove_headers_on_redirect=[],
        )

        self._session.mount("http://", HTTPAdapter(max_retries=retries))
        self._timeout_seconds = timeout_seconds

    @staticmethod
    def ready(node: ClusterNode) -> dict[str, Any]:
        url = f"http://{node.account.hostname}:9644/v1/status/ready"
        return requests.get(url).json()

    @staticmethod
    def _url(node: ClusterNode, path: str) -> str:
        return f"http://{node.account.hostname}:9644/v1/{path}"

    @staticmethod
    def _equal_assignments(r0: list[dict[str, Any]], r1: list[dict[str, Any]]) -> bool:
        def to_tuple(a: dict[str, Any]) -> tuple[int, int]:
            return a["node_id"], a["core"]

        r0_tuples = [to_tuple(a) for a in r0]
        r1_tuples = [to_tuple(a) for a in r1]
        return set(r0_tuples) == set(r1_tuples)

    def _get_configuration(
        self, host: str, namespace: str, topic: str, partition: int
    ) -> dict[str, Any] | None:
        url = f"http://{host}:9644/v1/partitions/{namespace}/{topic}/{partition}"
        self.redpanda.logger.debug(f"Dispatching GET {url}")
        r = self._session.request("GET", url)
        if r.status_code != 200:
            self.redpanda.logger.warning(f"Response {r.status_code}: {r.text}")
            return None
        else:
            try:
                json = r.json()
                self.redpanda.logger.debug(f"Response OK, JSON: {json}")
                return json
            except JSONDecodeError as e:
                self.redpanda.logger.debug(
                    f"Response OK, Malformed JSON: '{r.text}' ({e})"
                )
                return None

    def _get_stable_configuration(
        self,
        hosts: list[str],
        topic: str,
        partition: int = 0,
        namespace: str = "kafka",
        replication: Optional[int] = None,
    ) -> Optional[PartitionDetails]:
        """
        Method iterates through hosts and checks that the configuration of
        namespace/topic/partition is the same on all hosts and that all
        hosts see the same leader.

        When replication is provided the method also checks that the confi-
        guration has exactly that amout of nodes.

        When the configuration isn't stable the method returns None
        """
        last_leader = -1
        replicas = None
        status = None
        for host in hosts:
            self.redpanda.logger.debug(
                f'requesting "{namespace}/{topic}/{partition}" details from {host})'
            )
            meta = self._get_configuration(host, namespace, topic, partition)
            if meta is None:
                return None
            if "replicas" not in meta:
                self.redpanda.logger.debug("replicas are missing")
                return None
            if "status" not in meta:
                self.redpanda.logger.debug("status is missing")
                return None
            if status is None:
                status = meta["status"]
                self.redpanda.logger.debug(f"get status:{status}")
            if status != meta["status"]:
                self.redpanda.logger.debug(
                    f"get status:{meta['status']} while already observed:{status} before"
                )
                return None
            read_replicas = meta["replicas"]
            if replicas is None:
                replicas = read_replicas
                self.redpanda.logger.debug(f"get replicas:{read_replicas} from {host}")
            elif not self._equal_assignments(replicas, read_replicas):
                self.redpanda.logger.debug(
                    f"get conflicting replicas:{read_replicas} from {host}"
                )
                return None
            if replication is not None:
                if len(meta["replicas"]) != replication:
                    self.redpanda.logger.debug(
                        f"expected replication:{replication} got:{len(meta['replicas'])}"
                    )
                    return None
            if meta["leader_id"] < 0:
                self.redpanda.logger.debug("doesn't have leader")
                return None
            if last_leader < 0:
                last_leader = int(meta["leader_id"])
                self.redpanda.logger.debug(f"get leader:{last_leader}")
            if last_leader not in [n["node_id"] for n in replicas]:
                self.redpanda.logger.debug(
                    f"leader:{last_leader} isn't in the replica set"
                )
                return None
            if last_leader != meta["leader_id"]:
                self.redpanda.logger.debug(
                    f"got leader:{meta['leader_id']} but observed {last_leader} before"
                )
                return None
        assert replicas is not None, "replicas must be set at this point"
        info = PartitionDetails()
        info.status = status
        info.leader = int(last_leader)
        info.replicas = [Replica(r) for r in replicas]
        return info

    def wait_stable_configuration(
        self,
        topic: str,
        *,
        partition: int = 0,
        namespace: str = "kafka",
        replication: int | None = None,
        timeout_s: int = 10,
        backoff_s: int = 1,
        hosts: Optional[list[str]] = None,
    ) -> PartitionDetails:
        """
        Method waits for timeout_s until the configuration is stable and returns it.

        When the timeout is exhaust it throws TimeoutException
        """
        if hosts is None:
            hosts = [n.account.hostname for n in self.redpanda.started_nodes()]
        hosts = list(hosts)

        def get_stable_configuration():
            random.shuffle(hosts)
            msg = ",".join(hosts)
            self.redpanda.logger.debug(
                f"wait details for {namespace}/{topic}/{partition} from nodes: {msg}"
            )
            try:
                info = self._get_stable_configuration(
                    hosts,
                    topic,
                    partition=partition,
                    namespace=namespace,
                    replication=replication,
                )
                if info is None:
                    return False
                return True, info
            except RequestException:
                self.redpanda.logger.exception(
                    "an error on getting stable configuration, retrying"
                )
                return False

        return wait_until_result(
            get_stable_configuration,
            timeout_sec=timeout_s,
            backoff_sec=backoff_s,
            err_msg=f"can't fetch stable replicas for {namespace}/{topic}/{partition} within {timeout_s} sec",
        )

    def await_stable_leader(
        self,
        topic: str,
        partition: int = 0,
        namespace: str = "kafka",
        replication: int | None = None,
        timeout_s: int = 10,
        backoff_s: int = 1,
        hosts: Optional[list[str]] = None,
        check: Callable[[int], bool] = lambda node_id: True,
    ) -> int:
        """
        Method waits for timeout_s until the configuration is stable and check
        predicate returns true when invoked on the configuration's leader.

        When the timeout is exhaust it throws TimeoutException
        """

        def is_leader_stable():
            info = self.wait_stable_configuration(
                topic,
                partition=partition,
                namespace=namespace,
                replication=replication,
                timeout_s=timeout_s,
                hosts=hosts,
                backoff_s=backoff_s,
            )
            if info.leader is not None and check(info.leader):
                return True, info.leader

            self.redpanda.logger.debug(f"check failed (leader id: {info.leader})")
            return False

        return wait_until_result(
            is_leader_stable,
            timeout_sec=timeout_s,
            backoff_sec=backoff_s,
            err_msg=f"can't get stable leader of {namespace}/{topic}/{partition} within {timeout_s} sec",
        )

    def _request(
        self,
        verb: str,
        path: str,
        node: MaybeNode = None,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response:
        if node is None and self._default_node is not None:
            # We were constructed with an explicit default node: use that one
            # and do not retry on others.
            node = self._default_node
            retry_connection = False
        elif node is None:
            # Pick a random node to run this request on.  If that node gives
            # connection errors we will retry on other nodes.
            node = random.choice(self.redpanda.started_nodes())
            retry_connection = True
        else:
            # We were called with a specific node to run on -- do no retry on
            # other nodes.
            retry_connection = False

        assert node is not None, "node must be set at this point"

        if kwargs.get("timeout", None) is None:
            kwargs["timeout"] = self._timeout_seconds

        # We will have to handle redirects ourselves (always set kwargs['allow_redirects'] = False),
        # see comment after _session.request() below.
        # If kwargs was passed with "allow_redirects:False", it is assumed that the user intends
        # to handle the redirect case at the call site. Otherwise, it will be retried in the
        # request loop below.
        handle_retry_backoff = kwargs.get("allow_redirects", True)
        kwargs["allow_redirects"] = False
        num_redirects = 0

        fallback_nodes: list[ClusterNode] = self.redpanda.nodes
        fallback_nodes = list(filter(lambda n: n != node, fallback_nodes))

        params_e = f"?{urllib.parse.urlencode(params)}" if params is not None else ""
        url = self._url(node, path + params_e)

        # On connection errors, retry until we run out of alternative nodes to try
        # (fall through on first successful request)
        while True:
            self.redpanda.logger.debug(f"Dispatching {verb} {url}")
            try:
                r = self._session.request(verb, url, **kwargs)
            except requests.ConnectionError:
                if retry_connection and fallback_nodes:
                    node = random.choice(fallback_nodes)
                    fallback_nodes = list(filter(lambda n: n != node, fallback_nodes))
                    self.redpanda.logger.info(
                        f"Connection error, retrying on node {node.account.hostname} (remaining {[n.account.hostname for n in fallback_nodes]})"
                    )
                    url = self._url(node, path + params_e)
                else:
                    raise
            else:
                # Requests library does NOT respect Retry-After with a redirect
                # error code (see https://github.com/psf/requests/pull/4562).
                # There is logic that respects Retry-After within urllib3,
                # but since the Requests library handles 30# error codes
                # internally, a Retry-After attached to a redirect response
                # will not be respected. We will have to handle this ourselves.
                if (
                    handle_retry_backoff
                    and r.is_redirect
                    and num_redirects < self._session.max_redirects
                ):
                    url = not_none(r.headers.get("Location"))
                    retry_after = r.headers.get("Retry-After")
                    if retry_after is not None:
                        self.redpanda.logger.info(
                            f"Retry-After: {retry_after} on redirect {url}"
                        )
                        time.sleep(int(retry_after))
                    num_redirects += 1
                else:
                    break

        # Log the response
        if r.status_code != 200:
            self.redpanda.logger.warning(f"Response {r.status_code}: {r.text}")
        else:
            if "application/json" in (not_none(r.headers.get("Content-Type"))) and len(
                r.text
            ):
                try:
                    self.redpanda.logger.debug(f"Response OK, JSON: {r.json()}")
                except json.decoder.JSONDecodeError as e:
                    self.redpanda.logger.debug(
                        f"Response OK, Malformed JSON: '{r.text}' ({e})"
                    )
            else:
                self.redpanda.logger.debug("Response OK")

        r.raise_for_status()
        return r

    @staticmethod
    def _bool_param(value: bool) -> str:
        """
        Converts a boolean value to a string representation for use in query parameters.
        """
        return "true" if value else "false"

    def get_status_ready(self, node: MaybeNode = None) -> dict[str, Any]:
        return self._request("GET", "status/ready", node=node).json()

    def get_cluster_config(
        self,
        node: MaybeNode = None,
        include_defaults: bool | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        if key is not None:
            kwargs = {"params": {"key": key}}
        elif include_defaults is not None:
            kwargs = {"params": {"include_defaults": include_defaults}}
        else:
            kwargs = {}

        return self._request("GET", "cluster_config", node=node, **kwargs).json()

    def get_cluster_config_schema(self, node: MaybeNode = None) -> dict[str, Any]:
        return self._request("GET", "cluster_config/schema", node=node).json()

    def patch_cluster_config(
        self,
        upsert: dict[str, str | int | None] = {},
        remove: list[str] = [],
        force: bool = False,
        dry_run: bool = False,
        node: MaybeNode = None,
    ) -> Any:
        path = "cluster_config"
        params: dict[str, str] = {}
        if force:
            params["force"] = "true"
        if dry_run:
            params["dry_run"] = "true"

        if params:
            joined = "&".join([f"{k}={v}" for k, v in params.items()])
            path = path + f"?{joined}"

        return self._request(
            "PUT", path, json={"upsert": upsert, "remove": remove}, node=node
        ).json()

    def get_cluster_config_status(self, node: MaybeNode = None):
        return self._request("GET", "cluster_config/status", node=node).json()

    def get_node_config(self, node: MaybeNode = None):
        return self._request("GET", "node_config", node).json()

    def get_features(self, node: MaybeNode = None):
        return self._request("GET", "features", node=node).json()

    def get_cloud_storage_lifecycle_markers(self, node: MaybeNode = None):
        return self._request("GET", "cloud_storage/lifecycle", node=node).json()

    def delete_cloud_storage_lifecycle_marker(
        self, topic: str, revision: str, node: MaybeNode = None
    ):
        return self._request(
            "DELETE", f"cloud_storage/lifecycle/{topic}/{revision}", node=node
        )

    def cloud_storage_trim(
        self,
        *,
        byte_limit: Optional[int],
        object_limit: Optional[int],
        node: ClusterNode,
    ):
        path = "cloud_storage/cache/trim"
        params: dict[str, str] = {}
        if byte_limit is not None:
            params["bytes"] = str(byte_limit)
        if object_limit is not None:
            params["objects"] = str(object_limit)

        if params:
            joined = "&".join([f"{k}={v}" for k, v in params.items()])
            path = path + f"?{joined}"

        return self._request("POST", path, node=node)

    def supports_feature(
        self, feature_name: str, nodes: list[ClusterNode] | None = None
    ):
        """
        Returns true whether all nodes in 'nodes' support the given feature. If
        no nodes are supplied, uses all nodes in the cluster.
        """
        if not nodes:
            nodes = self.redpanda.nodes

        def node_supports_feature(node: ClusterNode):
            features_resp = None
            try:
                features_resp = self.get_features(node=node)
            except RequestException as e:
                self.redpanda.logger.debug(
                    f"Could not get features on {node.account.hostname}: {e}"
                )
                return False
            features_dict = dict((f["name"], f) for f in features_resp["features"])
            return features_dict[feature_name]["state"] == "active"

        for node in nodes:
            if not node_supports_feature(node):
                return False
        return True

    def unsafe_reset_cloud_metadata(
        self, topic: str, partition: str, manifest: dict[str, Any]
    ):
        return self._request(
            "POST", f"debug/unsafe_reset_metadata/{topic}/{partition}", json=manifest
        )

    def unsafe_reset_metadata_from_cloud(
        self, namespace: str, topic: str, partition: int
    ):
        return self._request(
            "POST",
            f"cloud_storage/unsafe_reset_metadata_from_cloud/{namespace}/{topic}/{partition}",
        )

    def put_feature(self, feature_name: str, body: dict[str, Any]) -> Response:
        return self._request("PUT", f"features/{feature_name}", json=body)

    def get_license(self, node: MaybeNode = None, timeout: int | None = None):
        return self._request(
            "GET", "features/license", node=node, timeout=timeout
        ).json()

    @staticmethod
    def is_sample_license(resp: dict[str, Any]) -> bool:
        """
        Returns true if the given response to a `get_license` request returned the same license as the sample license
        configured in `sample_license` ("REDPANDA_SAMPLE_LICENSE" env var). Returns false for the built in evaluation
        period license and the second sample license 'REDPANDA_SECOND_SAMPLE_LICENSE'.
        """
        # NOTE: the initial implementation of the get license endpoint (before v22.3) didn't return the sha256.
        # We could remove those old tests, but it's simpler to use the type and the org to detect the installed
        # license instead.

        # REDPANDA_SAMPLE_LICENSE: {'loaded': True, 'license': {'format_version': 0, 'org': 'redpanda-testing', 'type': 'enterprise', 'expires': 4813252273, 'sha256': '2730125070a934ca1067ed073d7159acc9975dc61015892308aae186f7455daf'}}
        # REDPANDA_SECOND_SAMPLE_LICENSE: {'loaded': True, 'license': {'format_version': 0, 'org': 'redpanda-testing-2', 'type': 'enterprise', 'expires': 4827156118, 'sha256': '54240716865c1196fa6bd0ebb31821ab69160a3ed312b13bc810c17c9ec8852c'}}
        # Evaluation Period: {'loaded': True, 'license': {'format_version': 0, 'org': 'Redpanda Built-In Evaluation Period', 'type': 'free_trial', 'expires': 1733992567, 'sha256': ''}}

        return (
            resp.get("license", None) is not None
            and resp["license"]["type"] == "enterprise"
            and resp["license"]["org"] == "redpanda-testing"
        )

    def put_license(self, license: str, node: MaybeNode = None):
        return self._request("PUT", "features/license", data=license, node=node)

    def get_enterprise_features(self):
        return self._request("GET", "features/enterprise")

    def get_loggers(self, node: ClusterNode):
        """
        Get the names of all loggers.
        """
        return [l["name"] for l in self._request("GET", "loggers", node=node).json()]

    def get_log_level(self, name: str) -> list[dict[str, Any]]:
        """
        Get broker log level
        """
        responses: list[dict[str, Any]] = []
        name = name.replace("/", "%2F")
        for node in self.redpanda.nodes:
            path = f"config/log_level/{name}"
            responses.append(self._request("get", path, node=node).json())
        return responses

    def set_log_level(
        self, name: str, level: str, expires: int | None = None, force: bool = False
    ) -> list[dict[str, Any]]:
        """
        Set broker log level
        """
        responses: list[dict[str, Any]] = []
        name = name.replace("/", "%2F")
        for node in self.redpanda.nodes:
            path = f"config/log_level/{name}?level={level}"
            if expires is not None:
                path = f"{path}&expires={expires}"
            if force:
                path = f"{path}&force=true"
            responses.append(self._request("put", path, node=node).json())
        return responses

    def get_brokers(self, node: MaybeNode = None) -> list[dict[str, Any]]:
        """
        Return metadata about brokers.
        """
        return self._request("get", "brokers", node=node).json()

    def get_broker(self, id: int, node: MaybeNode = None) -> dict[str, Any]:
        """
        Return metadata about a broker.

        This differs from the `brokers/` endpoint in that it may contain
        additional context beyond what is reported for through `brokers/`.
        """
        return self._request("get", f"brokers/{id}", node=node).json()

    def get_broker_pre_restart_probe(
        self, limit: int | None = None, node: MaybeNode = None
    ) -> dict[str, Any]:
        """
        Return broker pre-restart probe.
        """
        assert node is not None
        params = None if limit is None else {"limit": limit}
        return self._request(
            "get", "broker/pre_restart_probe", node=node, params=params
        ).json()

    def get_broker_post_restart_probe(self, node: MaybeNode = None) -> dict[str, Any]:
        """
        Return broker post-restart probe.
        """
        assert node is not None
        return self._request("get", "broker/post_restart_probe", node=node).json()

    def get_cluster_view(self, node: MaybeNode = None) -> dict[str, Any]:
        """
        Return cluster_view.
        """
        return self._request("get", "cluster_view", node=node).json()

    def get_cluster_health_overview(self, node: MaybeNode = None) -> dict[str, Any]:
        return self._request("get", "cluster/health_overview", node=node).json()

    def decommission_broker(self, id: int, node: ClusterNode | None = None) -> Response:
        """
        Decommission broker
        """
        path = f"brokers/{id}/decommission"
        self.redpanda.logger.debug(f"decommissioning {path}")
        return self._request("put", path, node=node)

    def get_decommission_status(
        self, id: int, node: ClusterNode | None = None
    ) -> dict[str, Any]:
        """
        Get broker decommission status
        """
        path = f"brokers/{id}/decommission"
        return self._request("get", path, node=node).json()

    def recommission_broker(self, id: int, node: MaybeNode = None) -> Response:
        """
        Recommission broker i.e. abort ongoing decommissioning
        """
        path = f"brokers/{id}/recommission"
        self.redpanda.logger.debug(f"recommissioning {id}")
        return self._request("put", path, node=node)

    def trigger_rebalance(self, node: MaybeNode = None) -> Response:
        """
        Trigger on demand partitions rebalancing
        """
        path = "partitions/rebalance"

        return self._request("post", path, node=node)

    def trigger_cores_rebalance(self, node: MaybeNode) -> Response:
        """
        Trigger core placement rebalancing for partitions in this node.
        """
        path = "partitions/rebalance_cores"

        return self._request("post", path, node=node)

    def list_reconfigurations(self, node: MaybeNode = None) -> list[dict[str, Any]]:
        """
        List pending reconfigurations
        """
        path = "partitions/reconfigurations"

        return self._request("get", path, node=node).json()

    def cancel_all_reconfigurations(self, node: MaybeNode = None) -> dict[str, Any]:
        """
        Cancel all pending reconfigurations
        """
        path = "cluster/cancel_reconfigurations"

        return self._request("post", path, node=node).json()

    def cancel_all_node_reconfigurations(
        self, target_id: int, node: MaybeNode = None
    ) -> dict[str, Any]:
        """
        Cancel all reconfigurations moving partition replicas from node
        """
        path = f"brokers/{target_id}/cancel_partition_moves"

        return self._request("post", path, node=node).json()

    # Overloads for get_partitions: when partition is provided we return a single
    # partition detail dict; otherwise a list of partition detail dicts.
    @overload
    def get_partitions(
        self,
        topic: str,
        partition: int,
        *,
        namespace: str | None = None,
        node: MaybeNode = None,
    ) -> dict[str, Any]: ...

    @overload
    def get_partitions(
        self,
        topic: str | None = ...,
        partition: None = ...,
        *,
        namespace: str | None = None,
        node: MaybeNode = None,
    ) -> list[dict[str, Any]]: ...

    def get_partitions(
        self,
        topic: str | None = None,
        partition: int | None = None,
        *,
        namespace: str | None = None,
        node: MaybeNode = None,
    ) -> Any:
        """
        Return partition metadata from controller. This includes low-level
        information like replica set assignments with core affinities.

        The return type depends on the parameters, if a partition is specified,
        it is a dict with partition details. Otherwise, it is a list of partitions,
        filtered by topic if given.
        """
        assert (topic is None and partition is None) or (topic is not None)

        namespace = namespace or "kafka"
        path = "partitions"
        if topic:
            path = f"{path}/{namespace}/{topic}"

        if partition is not None:
            path = f"{path}/{partition}"

        return self._request("get", path, node=node).json()

    def get_partition(
        self, ns: str, topic: str, id: int, node: MaybeNode = None
    ) -> dict[str, Any]:
        return self._request("GET", f"partitions/{ns}/{topic}/{id}", node=node).json()

    def get_transactions(
        self, topic: str, partition: int, namespace: str, node: MaybeNode = None
    ) -> dict[str, Any]:
        """
        Get transaction for current partition
        """
        path = f"partitions/{namespace}/{topic}/{partition}/transactions"
        return self._request("get", path, node=node).json()

    def get_all_transactions(self, node: MaybeNode = None) -> list[Any]:
        """
        Get all transactions
        """
        cluster_config = self.get_cluster_config(include_defaults=True)
        tm_partition_amount = cluster_config["transaction_coordinator_partitions"]
        result: list[dict[str, Any]] = []
        for partition in range(tm_partition_amount):
            self.await_stable_leader(
                topic="tx", namespace="kafka_internal", partition=partition
            )
            path = f"transactions?coordinator_partition_id={partition}"
            partition_res = self._request("get", path, node=node).json()
            result.extend(partition_res)
        return result

    def mark_transaction_expired(
        self,
        topic: str,
        partition: int,
        pid: dict[str, Any],
        namespace: str,
        node: MaybeNode = None,
    ) -> Response:
        """
        Mark transaction expired for partition
        """
        path = f"partitions/{namespace}/{topic}/{partition}/mark_transaction_expired?id={pid['id']}&epoch={pid['epoch']}"
        return self._request("post", path, node=node)

    def delete_partition_from_transaction(
        self,
        tid: str,
        namespace: str,
        topic: str,
        partition_id: int,
        etag: int,
        node: MaybeNode = None,
    ) -> Response:
        """
        Delete partition from transaction
        """
        partition_info = {
            "namespace": namespace,
            "topic": topic,
            "partition_id": partition_id,
            "etag": etag,
        }

        params = "&".join([f"{k}={v}" for k, v in partition_info.items()])
        path = f"transaction/{tid}/delete_partition/?{params}"
        return self._request("post", path, node=node)

    def find_tx_coordinator(self, tid: str, node: MaybeNode = None) -> dict[str, Any]:
        """
        Find tx coordinator by tx.id
        """
        path = f"transaction/{tid}/find_coordinator"
        return self._request("get", path, node=node).json()

    def set_partition_replicas(
        self,
        topic: str,
        partition: int,
        replicas: list[dict[str, Any]],
        *,
        namespace: str = "kafka",
        node: MaybeNode = None,
    ) -> Response:
        """
        [ {"node_id": 0, "core": 1}, ... ]
        """
        path = f"partitions/{namespace}/{topic}/{partition}/replicas"
        return self._request("post", path, node=node, json=replicas)

    def force_set_partition_replicas(
        self,
        topic: str,
        partition: int,
        replicas: list[dict[str, Any]],
        *,
        namespace: str = "kafka",
        node: MaybeNode = None,
    ) -> Response:
        """
        [ {"node_id": 0, "core": 1}, ... ]
        """
        path = f"debug/partitions/{namespace}/{topic}/{partition}/force_replicas"
        return self._request("post", path, node=node, json=replicas)

    def toggle_failure_injection(
        self,
        topic: str,
        partition: int,
        op: str,
        *,
        inject: bool,
        node: MaybeNode,
        namespace: str = "kafka",
    ) -> Response:
        assert op == "append_entries"
        verb = "enable" if inject else "disable"
        path = f"debug/partitions/{namespace}/{topic}/{partition}/{verb}_error_injection/{op}"
        return self._request("post", path, node=node)

    def cancel_partition_move(
        self,
        topic: str,
        partition: int,
        namespace: str = "kafka",
        node: MaybeNode = None,
    ) -> Response:
        path = f"partitions/{namespace}/{topic}/{partition}/cancel_reconfiguration"
        return self._request("post", path, node=node)

    def force_abort_partition_move(
        self,
        topic: str,
        partition: int,
        namespace: str = "kafka",
        node: MaybeNode = None,
    ) -> Response:
        path = (
            f"partitions/{namespace}/{topic}/{partition}/unclean_abort_reconfiguration"
        )
        return self._request("post", path, node=node)

    def get_majority_lost_partitions_from_nodes(
        self, dead_brokers: list[int], node: MaybeNode = None, **kwargs: Any
    ) -> dict[str, Any]:
        assert dead_brokers
        brokers_csv = ",".join(str(b) for b in dead_brokers)
        path = f"partitions/majority_lost?dead_nodes={brokers_csv}"
        return self._request("get", path, node, **kwargs).json()

    def force_recover_partitions_from_nodes(
        self, payload: dict[str, Any], node: MaybeNode = None
    ) -> Response:
        assert payload
        path = "partitions/force_recover_from_nodes"
        return self._request("post", path, node, json=payload)

    def set_partition_replica_core(
        self,
        topic: str,
        partition: int,
        replica: int,
        core: int,
        namespace: str = "kafka",
        node: MaybeNode = None,
    ) -> Response:
        path = f"partitions/{namespace}/{topic}/{partition}/replicas/{replica}"
        return self._request("post", path, node=node, json={"core": core})

    def create_user(
        self,
        username: str,
        password: str = "12345678",
        algorithm: str = "SCRAM-SHA-256",
        await_exists: bool = False,
    ) -> None:
        self.redpanda.logger.debug(f"Creating user {username}:{password}:{algorithm}")

        path = "security/users"

        self._request(
            "POST",
            path,
            json=dict(
                username=username,
                password=password,
                algorithm=algorithm,
            ),
        )

        if await_exists:
            self.await_user_exists(username)

    def await_user_exists(
        self, username: str, timeout_sec: int = 15, backoff_sec: int = 1
    ):
        def user_exists():
            for node in self.redpanda.started_nodes():
                users = self.list_users(node=node)
                if username not in users:
                    return False
            return True

        wait_until(user_exists, timeout_sec=timeout_sec, backoff_sec=backoff_sec)

    def delete_user(self, username: str) -> None:
        self.redpanda.logger.info(f"Deleting user {username}")

        path = f"security/users/{username}"

        self._request("delete", path)

    def update_user(self, username: str, password: str, algorithm: str):
        self.redpanda.logger.info(f"Updating user {username}:{password}:{algorithm}")

        self._request(
            "PUT",
            f"security/users/{username}",
            json=dict(
                username=username,
                password=password,
                algorithm=algorithm,
            ),
        )

    def list_users(
        self, node: MaybeNode = None, include_ephemeral: Optional[bool] = False
    ) -> dict[str, Any]:
        params = (
            None
            if include_ephemeral is None
            else {"include_ephemeral": self._bool_param(include_ephemeral)}
        )
        return self._request("get", "security/users", node=node, params=params).json()

    def list_user_roles(self, filter: Optional[str] = None) -> Response:
        params: dict[str, Any] = {}
        if filter is not None:
            params["filter"] = filter
        return self._request("get", "security/users/roles", params=params)

    def create_role(self, role: str) -> Response:
        return self._request("post", "security/roles", json=dict(role=role))

    def get_role(self, role: str) -> Response:
        return self._request("get", f"security/roles/{role}")

    def delete_role(self, role: str, delete_acls: Optional[bool] = None) -> Response:
        params = None if delete_acls is None else dict(delete_acls=delete_acls)
        return self._request("delete", f"security/roles/{role}", params=params)

    def list_roles(
        self,
        filter: Optional[str] = None,
        principal: Optional[str] = None,
        principal_type: Optional[str] = None,
        node: MaybeNode = None,
    ) -> Response:
        params: dict[str, Any] = {}
        if filter is not None:
            params["filter"] = filter
        if principal is not None:
            params["principal"] = principal
        if principal_type is not None:
            params["principal_type"] = principal_type
        return self._request("get", "security/roles", params=params, node=node)

    def update_role_members(
        self,
        role: str,
        add: Optional[list[RoleMember]] = [],
        remove: Optional[list[RoleMember]] = [],
        create: Optional[bool] = None,
    ) -> Response:
        to_add = [m._asdict() for m in add] if add is not None else []
        to_remove = [m._asdict() for m in remove] if remove is not None else []

        params: dict[str, Any] = {}
        if create is not None:
            params["create"] = create

        return self._request(
            "post",
            f"security/roles/{role}/members",
            params=params,
            json=dict(add=to_add, remove=to_remove),
        )

    def security_report(self) -> Response:
        return self._request("get", "security/report")

    def list_role_members(self, role: str) -> Response:
        return self._request("get", f"security/roles/{role}/members")

    def partition_transfer_leadership(
        self,
        namespace: str,
        topic: str,
        partition: int,
        target_id: int | None = None,
    ) -> None:
        path = f"partitions/{namespace}/{topic}/{partition}/transfer_leadership"
        if target_id:
            path += f"?target={target_id}"

        self._request("POST", path)

    def get_partition_leader(
        self, *, namespace: str, topic: str, partition: int, node: MaybeNode = None
    ) -> int:
        partition_info = self.get_partitions(
            topic=topic, partition=partition, namespace=namespace, node=node
        )

        return partition_info["leader_id"]

    def transfer_leadership_to(
        self,
        *,
        namespace: str,
        topic: str,
        partition: int,
        target_id: int | None = None,
        leader_id: int | None = None,
    ) -> bool:
        """
        Looks up current ntp leader and transfer leadership to target node,
        this operations is NOP when current leader is the same as target.
        If user pass None for target this function will choose next replica for new leader.
        Returns true if leadership was transferred to the target node.
        """

        def _get_details() -> dict[str, Any]:
            p = self.get_partitions(
                topic=topic, partition=partition, namespace=namespace
            )
            self.redpanda.logger.debug(
                f"ntp {namespace}/{topic}/{partition} details: {p}"
            )
            return p

        #  check which node is current leader

        if leader_id is None:
            leader_id = self.await_stable_leader(
                topic,
                partition=partition,
                namespace=namespace,
                timeout_s=30,
                backoff_s=2,
            )

        details = _get_details()

        if target_id is not None:
            if leader_id == target_id:
                return True
            path = f"raft/{details['raft_group_id']}/transfer_leadership?target={target_id}"
        else:
            path = f"raft/{details['raft_group_id']}/transfer_leadership"

        leader = self.redpanda.get_node(leader_id)
        ret = self._request("post", path=path, node=leader)
        return ret.status_code == 200

    def maintenance_start(
        self, node: ClusterNode, dst_node: MaybeNode = None
    ) -> Response:
        """
        Start maintenance on 'node', sending the request to 'dst_node'.
        """
        id = self.redpanda.node_id(node)
        url = f"brokers/{id}/maintenance"
        self.redpanda.logger.info(f"Starting maintenance on node {node.name}/{id}")
        return self._request("put", url, node=dst_node)

    def maintenance_stop(
        self, node: ClusterNode, dst_node: MaybeNode = None
    ) -> Response:
        """
        Stop maintenance on 'node', sending the request to 'dst_node'.
        """
        id = self.redpanda.node_id(node)
        url = f"brokers/{id}/maintenance"
        self.redpanda.logger.info(f"Stopping maintenance on node {node.name}/{id}")
        return self._request("delete", url, node=dst_node)

    def maintenance_status(self, node: ClusterNode) -> dict[str, Any]:
        """
        Get maintenance status of a node.
        """
        id = self.redpanda.node_id(node)
        self.redpanda.logger.info(
            f"Getting maintenance status on node {node.name}/{id}"
        )
        return self._request("get", "maintenance", node=node).json()

    def reset_leaders_info(self, node: ClusterNode) -> Response:
        """
        Reset info for leaders on node
        """
        id = self.redpanda.node_id(node)
        self.redpanda.logger.info(f"Reset leaders info on {node.name}/{id}")
        url = "debug/reset_leaders"
        return self._request("post", url, node=node)

    def get_leaders_info(self, node: MaybeNode = None) -> dict[str, Any]:
        """
        Get info for leaders on node
        """
        if node:
            id = self.redpanda.node_id(node)
            self.redpanda.logger.info(f"Get leaders info on {node.name}/{id}")
        else:
            self.redpanda.logger.info("Get leaders info on any node")

        url = "debug/partition_leaders_table"
        return self._request("get", url, node=node).json()

    def si_sync_local_state(
        self, topic: str, partition: int, node: MaybeNode = None
    ) -> Response:
        """
        Check data in the S3 bucket and fix local index if needed
        """
        path = f"cloud_storage/sync_local_state/{topic}/{partition}"
        return self._request("post", path, node=node)

    def get_partition_balancer_status(
        self, node: MaybeNode = None, **kwargs: Any
    ) -> dict[str, Any]:
        return self._request(
            "GET", "cluster/partition_balancer/status", node=node, **kwargs
        ).json()

    def get_peer_status(self, node: ClusterNode, peer_id: int) -> dict[str, Any]:
        return self._request("GET", f"debug/peer_status/{peer_id}", node=node).json()

    def get_controller_status(self, node: MaybeNode) -> dict[str, Any]:
        return self._request("GET", "debug/controller_status", node=node).json()

    def get_cluster_uuid(self, node: MaybeNode = None) -> str | None:
        try:
            r = self._request("GET", "cluster/uuid", node=node)
        except HTTPError as ex:
            if ex.response.status_code == 404:
                return None
            raise
        if len(r.text) > 0:
            return r.json()["cluster_uuid"]
        return None

    def get_metrics_uuid(self, node: MaybeNode = None) -> str | None:
        """
        Returns the concents of the `/v1/cluster/metrics_uuid` endpoint.

        Parameters
        ----------
        node: ClusterNode
            The node to query the endpoint on. If None, a random node will be
            chosen.

        Returns
        -------
        str
            The Metrics UUID

        None
            If the endpoint returns a 404 status code.
        """
        try:
            r = self._request("GET", "cluster/metrics_uuid", node=node)
        except HTTPError as ex:
            if ex.response.status_code == 404:
                return None
            raise
        if len(r.text) > 0:
            return r.json()["uuid"]
        return None

    def initiate_topic_scan_and_recovery(
        self,
        payload: Optional[dict[str, Any]] = None,
        force_acquire_lock: bool = False,
        node: MaybeNode = None,
        **kwargs: Any,
    ) -> Response:
        request_args: dict[str, Any] = {"node": node, **kwargs}

        if payload:
            request_args["json"] = payload
        return self._request("post", "cloud_storage/topic_recovery", **request_args)

    def get_topic_recovery_status(
        self, node: MaybeNode = None, **kwargs: Any
    ) -> Response:
        request_args: dict[str, Any] = {"node": node, **kwargs}
        return self._request(
            "get", "cloud_storage/topic_recovery?extended=true", **request_args
        )

    def initialize_cluster_recovery(
        self,
        node: MaybeNode = None,
        cluster_uuid_override: str | None = None,
        **kwargs: Any,
    ) -> Response:
        if cluster_uuid_override is not None:
            assert "json" not in kwargs, "cannot pass cluster_uuid_override and json"
            kwargs["json"] = {"cluster_uuid_override": cluster_uuid_override}

        request_args: dict[str, Any] = {"node": node, **kwargs}

        return self._request("post", "cloud_storage/automated_recovery", **request_args)

    def get_cluster_recovery_status(
        self, node: MaybeNode = None, **kwargs: Any
    ) -> Response:
        request_args: dict[str, Any] = {"node": node, **kwargs}
        return self._request("get", "cloud_storage/automated_recovery", **request_args)

    def self_test_start(self, options: dict[str, Any]) -> Response:
        return self._request("POST", "debug/self_test/start", json=options)

    def self_test_stop(self) -> Response:
        return self._request("POST", "debug/self_test/stop")

    def self_test_status(self) -> dict[str, Any]:
        return self._request("GET", "debug/self_test/status").json()

    def restart_service(
        self, rp_service: Optional[str] = None, node: Optional[ClusterNode] = None
    ) -> Response:
        service_param = f"service={rp_service if rp_service is not None else ''}"
        return self._request("PUT", f"debug/restart_service?{service_param}", node=node)

    def is_node_isolated(self, node: ClusterNode) -> dict[str, Any]:
        return self._request("GET", "debug/is_node_isolated", node=node).json()

    def stress_fiber_start(
        self,
        node: MaybeNode,
        num_fibers: int,
        *,
        min_spins_per_scheduling_point: int | None = None,
        max_spins_per_scheduling_point: int | None = None,
        min_ms_per_scheduling_point: int | None = None,
        max_ms_per_scheduling_point: int | None = None,
        stack_depth: int | None = None,
    ) -> Response:
        p: dict[str, str] = {"num_fibers": str(num_fibers)}
        if min_spins_per_scheduling_point is not None:
            p["min_spins_per_scheduling_point"] = str(min_spins_per_scheduling_point)
        if max_spins_per_scheduling_point is not None:
            p["max_spins_per_scheduling_point"] = str(max_spins_per_scheduling_point)
        if min_ms_per_scheduling_point is not None:
            p["min_ms_per_scheduling_point"] = str(min_ms_per_scheduling_point)
        if max_ms_per_scheduling_point is not None:
            p["max_ms_per_scheduling_point"] = str(max_ms_per_scheduling_point)
        if stack_depth is not None:
            p["stack_depth"] = str(stack_depth)
        kwargs: dict[str, Any] = {"params": p}
        return self._request("PUT", "debug/stress_fiber_start", node=node, **kwargs)

    def stress_fiber_stop(self, node: ClusterNode) -> Response:
        return self._request("PUT", "debug/stress_fiber_stop", node=node)

    def cloud_storage_usage(self) -> int:
        return int(
            self._request("GET", "debug/cloud_storage_usage?retries_allowed=10").json()
        )

    def get_usage(self, node: ClusterNode, include_open: bool = True) -> dict[str, Any]:
        return self._request(
            "GET", f"usage?include_open_bucket={str(include_open)}", node=node
        ).json()

    def refresh_disk_health_info(self, node: Optional[ClusterNode] = None) -> Response:
        """
        Reset info for cluster health on node
        """
        return self._request("post", "debug/refresh_disk_health_info", node=node)

    def get_partition_cloud_storage_status(
        self, topic: str, partition: int, node: MaybeNode = None
    ) -> dict[str, Any]:
        return self._request(
            "GET", f"cloud_storage/status/{topic}/{partition}", node=node
        ).json()

    def get_partition_manifest(self, topic: str, partition: int) -> dict[str, Any]:
        """
        Get the in-memory partition manifest for the requested ntp
        """
        return self._request(
            "GET", f"cloud_storage/manifest/{topic}/{partition}"
        ).json()

    def get_partition_state(
        self,
        namespace: str,
        topic: str,
        partition: int,
        node: Optional[ClusterNode] = None,
    ) -> dict[str, Any]:
        path = f"debug/partition/{namespace}/{topic}/{partition}"
        return self._request("GET", path, node=node).json()

    def get_offset_for_leader_epoch(
        self, topic: str, partition: int, epoch: int, node: MaybeNode = None
    ) -> dict[str, Any]:
        path = f"debug/partitions/{topic}/{partition}/offset_for_leader_epoch?epoch={epoch}"
        return self._request("GET", path, node=node).json()

    def get_partitions_local_summary(self, node: ClusterNode) -> dict[str, Any]:
        path = "partitions/local_summary"
        return self._request("GET", path, node=node).json()

    def get_producers_state(
        self, namespace: str, topic: str, partition: int, node: MaybeNode = None
    ) -> dict[str, Any]:
        path = f"debug/producers/{namespace}/{topic}/{partition}"
        return self._request("GET", path, node=node).json()

    def get_local_storage_usage(self, node: MaybeNode = None) -> dict[str, int]:
        """
        Get the local storage usage report.
        """
        return self._request("get", "debug/local_storage_usage", node=node).json()

    def get_disk_stat(self, disk_type: str, node: ClusterNode) -> dict[str, Any]:
        """
        Get disk_type stat from node.
        """
        return self._request(
            "get", f"debug/storage/disk_stat/{disk_type}", node=node
        ).json()

    def set_disk_stat_override(
        self,
        disk_type: str,
        node: ClusterNode,
        *,
        total_bytes: int | None = None,
        free_bytes: int | None = None,
        free_bytes_delta: int | None = None,
    ) -> Response:
        """
        Get disk_type stat from node.
        """
        json: dict[str, int] = {}
        if total_bytes is not None:
            json["total_bytes"] = total_bytes
        if free_bytes is not None:
            json["free_bytes"] = free_bytes
        if free_bytes_delta is not None:
            json["free_bytes_delta"] = free_bytes_delta
        return self._request(
            "put", f"debug/storage/disk_stat/{disk_type}", json=json, node=node
        )

    def get_sampled_memory_profile(
        self, node: MaybeNode = None, shard: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Gets the sampled memory profile debug output
        """
        if shard is not None:
            kwargs: dict[str, Any] = {"params": {"shard": shard}}
        else:
            kwargs = {}

        return self._request(
            "get", "debug/sampled_memory_profile", node=node, **kwargs
        ).json()

    def get_cpu_profile(
        self, node: MaybeNode = None, wait_ms: int | None = None
    ) -> dict[str, Any]:
        """
        Get the CPU profile of a node.
        """
        path = "debug/cpu_profile"
        params: dict[str, str] = {}
        timeout = DEFAULT_TIMEOUT

        if wait_ms is not None:
            params["wait_ms"] = str(wait_ms)
            timeout = max(2 * (int(wait_ms) // 1_000), timeout)

        return self._request(
            "get", path, node=node, timeout=timeout, params=params
        ).json()

    def get_local_offsets_translated(
        self,
        offsets: list[int],
        topic: str,
        partition: int,
        translate_to: str = "kafka",
        node: MaybeNode = None,
    ) -> dict[str, Any]:
        """
        Query offset translator to translate offsets from one type to another

        Options for param "translate_to" are "kafka" and "redpanda"
        """
        return self._request(
            "get",
            f"debug/storage/offset_translator/kafka/{topic}/{partition}?translate_to={translate_to}",
            node=node,
            json=offsets,
        ).json()

    def set_storage_failure_injection(self, node: ClusterNode, value: bool) -> Response:
        return self._request(
            "PUT",
            f"debug/set_storage_failure_injection_enabled?value={self._bool_param(value)}",
            node=node,
        )

    def get_raft_recovery_status(self, *, node: ClusterNode) -> dict[str, Any]:
        """
        Node must be specified because this API reports on node-local state:
        it would not make sense to send it to just any node.
        """
        return self._request("GET", "raft/recovery/status", node=node).json()

    def get_cloud_storage_anomalies(
        self, namespace: str, topic: str, partition: int
    ) -> dict[str, Any]:
        return self._request(
            "GET", f"cloud_storage/anomalies/{namespace}/{topic}/{partition}"
        ).json()

    def reset_scrubbing_metadata(
        self,
        namespace: str,
        topic: str,
        partition: int,
        node: Optional[ClusterNode] = None,
    ) -> Response:
        return self._request(
            "POST",
            f"cloud_storage/reset_scrubbing_metadata/{namespace}/{topic}/{partition}",
            node=node,
        )

    def get_cluster_partitions(
        self,
        ns: str | None = None,
        topic: str | None = None,
        disabled: bool | None = None,
        with_internal: bool | None = None,
        node: MaybeNode = None,
    ) -> list[dict[str, Any]]:
        if topic is not None:
            assert ns is not None
            req = f"cluster/partitions/{ns}/{topic}"
        else:
            assert ns is None
            req = "cluster/partitions"

        if disabled is not None:
            req += f"?disabled={disabled}"

        if with_internal is not None:
            req += f"?with_internal={with_internal}"

        return self._request("GET", req, node=node).json()

    def set_partitions_disabled(
        self,
        ns: str | None = None,
        topic: str | None = None,
        partition: int | None = None,
        value: bool = True,
    ) -> Response:
        if partition is not None:
            req = f"cluster/partitions/{ns}/{topic}/{partition}"
        else:
            req = f"cluster/partitions/{ns}/{topic}"
        return self._request("POST", req, json={"disabled": value})

    def reset_crash_tracking(self, node: ClusterNode) -> Response:
        return self._request("PUT", "reset_crash_tracking", node=node)

    def migrate_tx_manager_in_recovery(self, node: ClusterNode) -> Response:
        return self._request("POST", "recovery/migrate_tx_manager", node=node)

    def get_tx_manager_recovery_status(
        self, node: Optional[ClusterNode] = None
    ) -> Response:
        return self._request("GET", "recovery/migrate_tx_manager", node=node)

    def get_broker_uuids(self, node: Optional[ClusterNode] = None) -> dict[str, Any]:
        return self._request("GET", "broker_uuids", node=node).json()

    def get_broker_uuid(self, node: ClusterNode) -> dict[str, Any]:
        return self._request("GET", "debug/broker_uuid", node=node).json()

    def override_node_id(
        self, node: ClusterNode, current_uuid: str, new_node_id: int, new_node_uuid: str
    ) -> Response:
        return self._request(
            "PUT",
            "debug/broker_uuid",
            node=node,
            json={
                "current_node_uuid": current_uuid,
                "new_node_uuid": new_node_uuid,
                "new_node_id": new_node_id,
            },
        )

    def transforms_list_committed_offsets(
        self, show_unknown: bool = False, node: Optional[ClusterNode] = None
    ) -> list[CommittedWasmOffset]:
        path = "transform/debug/committed_offsets"
        if show_unknown:
            path += "?show_unknown=true"
        raw = self._request("GET", path, node=node).json()
        return [
            CommittedWasmOffset(c["transform_name"], c["partition"], c["offset"])
            for c in raw
        ]

    def transforms_gc_committed_offsets(
        self, node: Optional[ClusterNode] = None
    ) -> Response:
        path = "transform/debug/committed_offsets/garbage_collect"
        return self._request("POST", path, node=node)

    def transforms_patch_meta(
        self, name: str, pause: bool | None = None, env: dict[str, str] | None = None
    ) -> Response:
        path = f"transform/{name}/meta"
        body: dict[str, Any] = {}
        if pause is not None:
            body["is_paused"] = pause
        if env is not None:
            body["env"] = [dict(key=k, value=env[k]) for k in env]
        return self._request("PUT", path, json=body)

    def list_data_migrations(self, node: Optional[ClusterNode] = None) -> Response:
        path = "migrations"
        return self._request("GET", path, node=node)

    def get_data_migration(
        self, migration_id: int, node: Optional[ClusterNode] = None
    ) -> Response:
        path = f"migrations/{migration_id}"
        return self._request("GET", path, node=node)

    def create_data_migration(
        self,
        migration: InboundDataMigration | OutboundDataMigration,
        node: Optional[ClusterNode] = None,
    ) -> Response:
        path = "migrations"
        return self._request("PUT", path, node=node, json=migration.as_dict())

    def execute_data_migration_action(
        self,
        migration_id: int,
        action: MigrationAction,
        node: Optional[ClusterNode] = None,
    ) -> Response:
        path = f"migrations/{migration_id}?action={action.value}"
        return self._request("POST", path, node=node)

    def delete_data_migration(
        self, migration_id: int, node: Optional[ClusterNode] = None
    ) -> Response:
        path = f"migrations/{migration_id}"
        return self._request("DELETE", path, node=node)

    def list_mountable_topics(self, node: Optional[ClusterNode] = None) -> Response:
        path = "topics/mountable"
        return self._request("GET", path, node=node)

    def get_migrated_entities_status(
        self,
        migration_id: int,
        node: Optional[ClusterNode] = None,
    ) -> Response:
        path = f"migrations/{migration_id}/entities_status"
        return self._request(
            "GET",
            path,
            node=node,
        )

    def put_migrated_entities_status(
        self,
        migration_id: int,
        data: dict[str, Any],
        node: Optional[ClusterNode] = None,
    ) -> Response:
        path = f"migrations/{migration_id}/entities_status"
        return self._request("PUT", path, node=node, json=data)

    def unmount_topics(
        self, topics: list[NamespacedTopic], node: Optional[ClusterNode] = None
    ) -> Response:
        path = "topics/unmount"
        return self._request(
            "POST", path, node=node, json={"topics": [t.as_dict() for t in topics]}
        )

    def mount_topics(
        self, topics: list[InboundTopic], node: Optional[ClusterNode] = None
    ) -> Response:
        path = "topics/mount"
        return self._request(
            "POST", path, node=node, json={"topics": [t.as_dict() for t in topics]}
        )

    def post_debug_bundle(
        self,
        config: DebugBundleStartConfig,
        ignore_none: bool = True,
        node: MaybeNode = None,
    ) -> Response:
        path = "debug/bundle"
        body = json.dumps(config, cls=DebugBundleEncoder, ignore_none=ignore_none)
        self.redpanda.logger.debug(f"Posting debug bundle: {body}")
        return self._request("POST", path, data=body, node=node)

    def get_debug_bundle(self, node: MaybeNode = None) -> Response:
        path = "debug/bundle"
        return self._request("GET", path, node=node)

    def delete_debug_bundle(self, job_id: UUID, node: MaybeNode = None) -> Response:
        path = f"debug/bundle/{job_id}"
        return self._request("DELETE", path, node=node)

    def get_debug_bundle_file(self, filename: str, node: MaybeNode = None) -> Response:
        path = f"debug/bundle/file/{filename}"
        return self._request("GET", path, node=node)

    def delete_debug_bundle_file(
        self, filename: str, node: MaybeNode = None
    ) -> Response:
        path = f"debug/bundle/file/{filename}"
        return self._request("DELETE", path, node=node)

    def unsafe_abort_group_transaction(
        self, group_id: str, *, pid: int, epoch: int, sequence: int
    ) -> Response:
        params = {
            "producer_id": pid,
            "producer_epoch": epoch,
            "sequence": sequence,
        }
        params_str = "&".join([f"{k}={v}" for k, v in params.items()])
        return self._request(
            "POST",
            f"transaction/unsafe_abort_group_transaction/{group_id}?{params_str}",
        )

    def put_ctracker_va_message(
        self, shard: int, msg: str, node: MaybeNode = None
    ) -> Response:
        params = {"message": msg}
        return self._request(
            "PUT", f"debug/ctracker/va/{shard}", node=node, data=json.dumps(params)
        )

    def log_backtrace(
        self, node: ClusterNode, simple_backtrace: bool = False
    ) -> Response:
        """
        Logs a backtrace to the redpanda log using the admin API.
        :simple_backtrace: If true, a one-line backtrace is logged, no tasktrace,
            if false, a full backtrace is logged including taasktrace.
        """
        path = f"debug/log_backtrace?simple={self._bool_param(simple_backtrace)}"
        return self._request("post", path, node=node)

    def trigger_crash(self, node: ClusterNode, crash_type: CrashType) -> None:
        """
        Trigger an immediate crash of the given type on the specified node.

        This admin API is only available in debug mode, this function throws if
        called in non-debug mode.
        """
        assert is_debug_mode(), "trigger_crash is only available in debug mode"
        path = f"debug/trigger_crash?type={crash_type.value}"
        try:
            self._request("post", path, node=node)
            raise RuntimeError("trigger_crash did not crash the node")
        except requests.exceptions.ConnectionError:
            # this is the expected error as the node will go down,
            # requests will retry and throw connection error
            return
