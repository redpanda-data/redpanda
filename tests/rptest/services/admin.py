# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
from enum import Enum
from logging import Logger
import random
import json
import time
import urllib.parse
from typing import Any, Optional, Callable, NamedTuple, Protocol, cast
from json.decoder import JSONDecodeError
from uuid import UUID
from ducktape.cluster.cluster import ClusterNode
import requests
from requests import Response
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException
from urllib3.util.retry import Retry
from rptest.util import wait_until_result
from rptest.services.redpanda_types import SaslCredentials

DEFAULT_TIMEOUT = 30

MaybeNode = ClusterNode | None


# The Admin class is used by RedpandaService and we also pass
# a RedpandaService to Admin instances which use it for logging
# amonng other things. So a circular dependency though not at the
# runtime level (only RedpandaService imports Admin, Admin just uses
# service objects passed in from outside and doesn't need to import
# them). However, from a type checking point of view this circular
# dependency is real. There a few workarounds, but mine is just to use
# a small protocol to stand-in for RedpandaService with the properties
# we actually use.
class RedpandaServiceProto(Protocol):
    @property
    def logger(self) -> Logger:
        ...


class AuthPreservingSession(requests.Session):
    """
    Override `requests` default behaviour of dropping Authorization
    headers when redirecting.  This makes sense as a general default,
    but in the case of the redpanda admin API, we trust the server to
    only redirect us to other equally privileged servers within
    the same cluster.
    """
    def should_strip_auth(self, old_url, new_url):
        return False


class Replica:
    node_id: int
    core: int

    def __init__(self, replica_dict):
        self.node_id = replica_dict["node_id"]
        self.core = replica_dict["core"]

    def __getitem__(self, item_type):
        if item_type == "node_id":
            return self.node_id
        elif item_type == "core":
            return self.core
        return None


class PartitionDetails:
    replicas: list[Replica]

    def __init__(self):
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
        return cls(RoleErrorCode(data['code']), data['message'])

    @classmethod
    def from_http_error(cls, e: HTTPError):
        data = e.response.json()
        return cls.from_json(e.response.json()['message'])


class RoleUpdate(NamedTuple):
    role: str


class RoleDescription(NamedTuple):
    name: str


class RolesList:
    def __init__(self, roles: list[RoleDescription]):
        self.roles = roles

    def __getitem__(self, i) -> RoleDescription:
        return self.roles[i]

    def __len__(self):
        return len(self.roles)

    def __str__(self):
        return json.dumps(self.roles)

    @classmethod
    def from_json(cls, body: bytes):
        d = json.loads(body)
        for k in d:
            assert k == 'roles', f"Unexpected key {k}"
        return cls([RoleDescription(**r) for r in d.get('roles', [])])

    @classmethod
    def from_response(cls, rsp: Response):
        return cls.from_json(rsp.content)


class RoleMember(NamedTuple):
    class PrincipalType(str, Enum):
        USER = 'User'

    principal_type: PrincipalType
    name: str

    @classmethod
    def User(cls, name: str):
        return cls(cls.PrincipalType.USER, name)


class RoleMemberList:
    members: list[RoleMember]

    def __init__(self, mems: list[dict] = []):
        self.members = [RoleMember(**m) for m in mems]

    def __getitem__(self, i) -> RoleMember:
        return self.members[i]

    def __len__(self):
        return len(self.members)

    def __str__(self):
        return str(self.members)

    @classmethod
    def from_json(cls, body: bytes):
        d = json.loads(body)
        for k in d:
            assert k == 'members', f"Unexpected key {k}"
        return cls(d['members'])

    # TODO(oren): factor this out to a base class maybe
    @classmethod
    def from_response(cls, rsp: Response):
        return cls.from_json(rsp.content)


class Role:
    name: str
    members: RoleMemberList

    def __init__(self, name: str, members: RoleMemberList = RoleMemberList()):
        self.name = name
        self.members = members

    @classmethod
    def from_json(cls, body: bytes):
        d = json.loads(body)
        expected_keys = set(['name', 'members'])
        assert all(k in expected_keys
                   for k in d), f"Unexpected key(s): {d.keys()}"
        assert 'name' in d, "Expected 'name' key"
        name = d['name']
        members = RoleMemberList(d.get('members', []))
        return cls(name, members=members)

    @classmethod
    def from_response(cls, rsp: Response):
        return cls.from_json(rsp.content)


class RoleMemberUpdateResponse:
    role: str
    added: RoleMemberList
    removed: RoleMemberList
    created: bool

    def __init__(self,
                 role: str,
                 added: RoleMemberList = RoleMemberList(),
                 removed: RoleMemberList = RoleMemberList(),
                 created: bool = False):
        self.role = role
        self.added = added
        self.removed = removed
        self.created = created

    def __str__(self):
        return json.dumps({
            'role': self.role,
            'added': [a for a in self.added],
            'removed': [r for r in self.removed],
            'created': self.created
        })

    @classmethod
    def from_json(cls, body: bytes):
        d = json.loads(body)
        expected_keys = set(['role', 'added', 'removed', 'created'])
        assert all(k in expected_keys
                   for k in d), f"Unexpected key(s): {d.keys()}"
        assert 'role' in d, "Expected 'role' key"
        role = d['role']
        kwargs = {}
        kwargs['added'] = RoleMemberList(d.get('added', []))
        kwargs['removed'] = RoleMemberList(d.get('removed', []))
        kwargs['created'] = d.get('created', False)
        return cls(role, **kwargs)

    @classmethod
    def from_response(cls, rsp: Response):
        return cls.from_json(rsp.content)


class NamespacedTopic:
    def __init__(self, topic: str, namespace: str | None = "kafka"):
        self.ns = namespace
        self.topic = topic

    def as_dict(self):
        ret = {'topic': self.topic}
        if self.ns is not None:
            ret['ns'] = self.ns
        return ret

    @classmethod
    def from_json(cls, body: bytes):
        d = json.loads(body)
        expected_keys = set(['ns', 'topic'])
        assert all(k in expected_keys
                   for k in d), f"Unexpected key(s): {d.keys()}"
        assert 'topic' in d, "Expected 'topic' key"
        topic = d['topic']
        namespace = "kafka"
        if 'ns' in d:
            namespace = d['ns']

        return cls(topic, namespace)


class OutboundDataMigration:
    migration_type: str
    topics: list[NamespacedTopic]
    consumer_groups: list[str]

    def __init__(self, topics: list[NamespacedTopic],
                 consumer_groups: list[str]):
        self.migration_type = "outbound"
        self.topics = topics
        self.consumer_groups = consumer_groups

    @classmethod
    def from_json(cls, body: bytes):
        d = json.loads(body)
        expected_keys = set(['type', 'topics', 'consumer_groups'])
        assert all(k in expected_keys
                   for k in d), f"Unexpected key(s): {d.keys()}"
        assert all(
            k in d
            for k in expected_keys), f"Missing keys: {expected_keys - set(d)}"

        return cls(d['topics'], d['consumer_groups'])

    def as_dict(self):
        return {
            'migration_type': self.migration_type,
            'topics': [t.as_dict() for t in self.topics],
            'consumer_groups': self.consumer_groups
        }


class InboundTopic:
    def __init__(self, src_topic: NamespacedTopic,
                 alias: NamespacedTopic | None):
        self.src_topic = src_topic
        self.alias = alias

    def as_dict(self):
        d = {
            'source_topic': self.src_topic.as_dict(),
        }
        if self.alias:
            d['alias'] = self.alias.as_dict()
        return d


class InboundDataMigration:
    migration_type: str
    topics: list[InboundTopic]
    consumer_groups: list[str]

    def __init__(self, topics: list[InboundTopic], consumer_groups: list[str]):
        self.migration_type = "inbound"
        self.topics = topics
        self.consumer_groups = consumer_groups

    def as_dict(self):
        return {
            "migration_type": self.migration_type,
            'topics': [t.as_dict() for t in self.topics],
            'consumer_groups': self.consumer_groups
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
    def __init__(self, *args, ignore_none: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.ignore_none = ignore_none

    def default(self, o):
        if isinstance(o, tuple) and hasattr(o, '_fields'):  # Detect NamedTuple
            return {
                k: self.default(v)
                for k, v in o._asdict().items()
                if not (self.ignore_none and v is None)
            }
        if isinstance(o, UUID):
            return str(o)
        if isinstance(o,
                      (dict, list, tuple, str, int, float, bool)) or o is None:
            return o
        if hasattr(o, '__dataclass_fields__'):  # assume SaslCredentials
            creds = o.__dict__
            if isinstance(o, SaslCredentials):
                # Swap algorithm for mechansim
                creds['mechanism'] = creds.pop('algorithm')
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


class Admin:
    """
    Wrapper for Redpanda admin REST API.

    All methods on this class will raise on errors.  For GETs the return
    value is a decoded dict of the JSON payload, for other requests
    the successful HTTP response object is returned.
    """
    def __init__(self,
                 redpanda: RedpandaServiceProto,
                 default_node: ClusterNode | None = None,
                 retry_codes: list[int] | None = None,
                 auth=None,
                 retries_amount=5):
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

        retries = Retry(status=retries_amount,
                        connect=0,
                        read=0,
                        backoff_factor=1,
                        status_forcelist=retry_codes,
                        respect_retry_after_header=True,
                        method_whitelist=None,
                        remove_headers_on_redirect=[])

        self._session.mount("http://", HTTPAdapter(max_retries=retries))

    @staticmethod
    def ready(node):
        url = f"http://{node.account.hostname}:9644/v1/status/ready"
        return requests.get(url).json()

    @staticmethod
    def _url(node, path):
        return f"http://{node.account.hostname}:9644/v1/{path}"

    @staticmethod
    def _equal_assignments(r0, r1):
        def to_tuple(a):
            return a["node_id"], a["core"]

        r0 = [to_tuple(a) for a in r0]
        r1 = [to_tuple(a) for a in r1]
        return set(r0) == set(r1)

    def _get_configuration(self, host, namespace, topic, partition):
        url = f"http://{host}:9644/v1/partitions/{namespace}/{topic}/{partition}"
        self.redpanda.logger.debug(f"Dispatching GET {url}")
        r = self._session.request("GET", url)
        if r.status_code != 200:
            self.redpanda.logger.warn(f"Response {r.status_code}: {r.text}")
            return None
        else:
            try:
                json = r.json()
                self.redpanda.logger.debug(f"Response OK, JSON: {json}")
                return json
            except JSONDecodeError as e:
                self.redpanda.logger.debug(
                    f"Response OK, Malformed JSON: '{r.text}' ({e})")
                return None

    def _get_stable_configuration(
            self,
            hosts,
            topic,
            partition=0,
            namespace="kafka",
            replication: Optional[int] = None) -> Optional[PartitionDetails]:
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
                f"requesting \"{namespace}/{topic}/{partition}\" details from {host})"
            )
            meta = self._get_configuration(host, namespace, topic, partition)
            if meta == None:
                return None
            if "replicas" not in meta:
                self.redpanda.logger.debug(f"replicas are missing")
                return None
            if "status" not in meta:
                self.redpanda.logger.debug(f"status is missing")
                return None
            if status == None:
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
                self.redpanda.logger.debug(
                    f"get replicas:{read_replicas} from {host}")
            elif not self._equal_assignments(replicas, read_replicas):
                self.redpanda.logger.debug(
                    f"get conflicting replicas:{read_replicas} from {host}")
                return None
            if replication != None:
                if len(meta["replicas"]) != replication:
                    self.redpanda.logger.debug(
                        f"expected replication:{replication} got:{len(meta['replicas'])}"
                    )
                    return None
            if meta["leader_id"] < 0:
                self.redpanda.logger.debug(f"doesn't have leader")
                return None
            if last_leader < 0:
                last_leader = int(meta["leader_id"])
                self.redpanda.logger.debug(f"get leader:{last_leader}")
            if last_leader not in [n["node_id"] for n in replicas]:
                self.redpanda.logger.debug(
                    f"leader:{last_leader} isn't in the replica set")
                return None
            if last_leader != meta["leader_id"]:
                self.redpanda.logger.debug(
                    f"got leader:{meta['leader_id']} but observed {last_leader} before"
                )
                return None
        info = PartitionDetails()
        info.status = status
        info.leader = int(last_leader)
        info.replicas = [Replica(r) for r in replicas]
        return info

    def wait_stable_configuration(
            self,
            topic,
            *,
            partition=0,
            namespace="kafka",
            replication=None,
            timeout_s=10,
            backoff_s=1,
            hosts: Optional[list[str]] = None) -> PartitionDetails:
        """
        Method waits for timeout_s until the configuration is stable and returns it.

        When the timeout is exhaust it throws TimeoutException
        """
        if hosts == None:
            hosts = [n.account.hostname for n in self.redpanda.started_nodes()]
        hosts = list(hosts)

        def get_stable_configuration():
            random.shuffle(hosts)
            msg = ",".join(hosts)
            self.redpanda.logger.debug(
                f"wait details for {namespace}/{topic}/{partition} from nodes: {msg}"
            )
            try:
                info = self._get_stable_configuration(hosts,
                                                      topic,
                                                      partition=partition,
                                                      namespace=namespace,
                                                      replication=replication)
                if info == None:
                    return False
                return True, info
            except RequestException:
                self.redpanda.logger.exception(
                    "an error on getting stable configuration, retrying")
                return False

        return wait_until_result(
            get_stable_configuration,
            timeout_sec=timeout_s,
            backoff_sec=backoff_s,
            err_msg=
            f"can't fetch stable replicas for {namespace}/{topic}/{partition} within {timeout_s} sec"
        )

    def await_stable_leader(self,
                            topic,
                            partition=0,
                            namespace="kafka",
                            replication=None,
                            timeout_s=10,
                            backoff_s=1,
                            hosts: Optional[list[str]] = None,
                            check: Callable[[int],
                                            bool] = lambda node_id: True):
        """
        Method waits for timeout_s until the configuration is stable and check
        predicate returns true when invoked on the configuration's leader.

        When the timeout is exhaust it throws TimeoutException
        """
        def is_leader_stable():
            info = self.wait_stable_configuration(topic,
                                                  partition=partition,
                                                  namespace=namespace,
                                                  replication=replication,
                                                  timeout_s=timeout_s,
                                                  hosts=hosts,
                                                  backoff_s=backoff_s)
            if check(info.leader):
                return True, info.leader

            self.redpanda.logger.debug(
                f"check failed (leader id: {info.leader})")
            return False

        return wait_until_result(
            is_leader_stable,
            timeout_sec=timeout_s,
            backoff_sec=backoff_s,
            err_msg=
            f"can't get stable leader of {namespace}/{topic}/{partition} within {timeout_s} sec"
        )

    def _request(self,
                 verb: str,
                 path: str,
                 node: MaybeNode = None,
                 params: Optional[dict] = None,
                 **kwargs: Any):
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

        if kwargs.get('timeout', None) is None:
            kwargs['timeout'] = DEFAULT_TIMEOUT

        #We will have to handle redirects ourselves (always set kwargs['allow_redirects'] = False),
        #see comment after _session.request() below.
        #If kwargs was passed with "allow_redirects:False", it is assumed that the user intends
        #to handle the redirect case at the call site. Otherwise, it will be retried in the
        #request loop below.
        handle_retry_backoff = kwargs.get('allow_redirects', True)
        kwargs['allow_redirects'] = False
        num_redirects = 0

        fallback_nodes = self.redpanda.nodes
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
                    fallback_nodes = list(
                        filter(lambda n: n != node, fallback_nodes))
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
                if handle_retry_backoff and r.is_redirect and num_redirects < self._session.max_redirects:
                    url = r.headers.get('Location')
                    retry_after = r.headers.get('Retry-After')
                    if retry_after is not None:
                        self.redpanda.logger.info(
                            f"Retry-After: {retry_after} on redirect {url}")
                        time.sleep(int(retry_after))
                    num_redirects += 1
                else:
                    break

        # Log the response
        if r.status_code != 200:
            self.redpanda.logger.warn(f"Response {r.status_code}: {r.text}")
        else:
            if 'application/json' in r.headers.get('Content-Type') and len(
                    r.text):
                try:
                    self.redpanda.logger.debug(
                        f"Response OK, JSON: {r.json()}")
                except json.decoder.JSONDecodeError as e:
                    self.redpanda.logger.debug(
                        f"Response OK, Malformed JSON: '{r.text}' ({e})")
            else:
                self.redpanda.logger.debug("Response OK")

        r.raise_for_status()
        return r

    def get_status_ready(self, node=None):
        return self._request("GET", "status/ready", node=node).json()

    def get_cluster_config(self, node=None, include_defaults=None, key=None):
        if key is not None:
            kwargs = {"params": {"key": key}}
        elif include_defaults is not None:
            kwargs = {"params": {"include_defaults": include_defaults}}
        else:
            kwargs = {}

        return self._request("GET", "cluster_config", node=node,
                             **kwargs).json()

    def get_cluster_config_schema(self, node=None):
        return self._request("GET", "cluster_config/schema", node=node).json()

    def patch_cluster_config(self,
                             upsert: dict[str, str | int | None] = {},
                             remove: list[str] = [],
                             force: bool = False,
                             dry_run: bool = False,
                             node=None):

        path = "cluster_config"
        params = {}
        if force:
            params['force'] = 'true'
        if dry_run:
            params['dry_run'] = 'true'

        if params:
            joined = "&".join([f"{k}={v}" for k, v in params.items()])
            path = path + f"?{joined}"

        return self._request("PUT",
                             path,
                             json={
                                 'upsert': upsert,
                                 'remove': remove
                             },
                             node=node).json()

    def get_cluster_config_status(self, node: MaybeNode = None):
        return self._request("GET", "cluster_config/status", node=node).json()

    def get_node_config(self, node: MaybeNode = None):
        return self._request("GET", "node_config", node).json()

    def get_features(self, node: MaybeNode = None):
        return self._request("GET", "features", node=node).json()

    def get_cloud_storage_lifecycle_markers(self, node: MaybeNode = None):
        return self._request("GET", "cloud_storage/lifecycle",
                             node=node).json()

    def delete_cloud_storage_lifecycle_marker(self,
                                              topic: str,
                                              revision: str,
                                              node: MaybeNode = None):
        return self._request("DELETE",
                             f"cloud_storage/lifecycle/{topic}/{revision}",
                             node=node)

    def cloud_storage_trim(self, *, byte_limit: Optional[int],
                           object_limit: Optional[int], node: ClusterNode):
        path = "cloud_storage/cache/trim"
        params = {}
        if byte_limit is not None:
            params['bytes'] = str(byte_limit)
        if object_limit is not None:
            params['objects'] = str(object_limit)

        if params:
            joined = "&".join([f"{k}={v}" for k, v in params.items()])
            path = path + f"?{joined}"

        return self._request("POST", path, node=node)

    def supports_feature(self,
                         feature_name: str,
                         nodes: list[ClusterNode] | None = None):
        """
        Returns true whether all nodes in 'nodes' support the given feature. If
        no nodes are supplied, uses all nodes in the cluster.
        """
        if not nodes:
            nodes = cast(list[ClusterNode], self.redpanda.nodes)

        def node_supports_feature(node: ClusterNode):
            features_resp = None
            try:
                features_resp = self.get_features(node=node)
            except RequestException as e:
                self.redpanda.logger.debug(
                    f"Could not get features on {node.account.hostname}: {e}")
                return False
            features_dict = dict(
                (f["name"], f) for f in features_resp["features"])
            return features_dict[feature_name]["state"] == "active"

        for node in nodes:
            if not node_supports_feature(node):
                return False
        return True

    def unsafe_reset_cloud_metadata(self, topic: str, partition: str,
                                    manifest: dict[str, Any]):
        return self._request(
            'POST',
            f"debug/unsafe_reset_metadata/{topic}/{partition}",
            json=manifest)

    def unsafe_reset_metadata_from_cloud(self, namespace: str, topic: str,
                                         partition: int):
        return self._request(
            'POST',
            f"cloud_storage/unsafe_reset_metadata_from_cloud/{namespace}/{topic}/{partition}"
        )

    def put_feature(self, feature_name: str, body):
        return self._request("PUT", f"features/{feature_name}", json=body)

    def get_license(self, node=None, timeout=None):
        return self._request("GET",
                             "features/license",
                             node=node,
                             timeout=timeout).json()

    def put_license(self, license):
        return self._request("PUT", "features/license", data=license)

    def get_enterprise_features(self):
        return self._request("GET", "features/enterprise")

    def get_loggers(self, node):
        """
        Get the names of all loggers.
        """
        return [
            l["name"]
            for l in self._request("GET", "loggers", node=node).json()
        ]

    def get_log_level(self, name):
        """
        Get broker log level
        """
        responses = []
        name = name.replace("/", "%2F")
        for node in self.redpanda.nodes:
            path = f"config/log_level/{name}"
            responses.append(self._request('get', path, node=node).json())
        return responses

    def set_log_level(self, name, level, expires=None, force=False):
        """
        Set broker log level
        """
        responses = []
        name = name.replace("/", "%2F")
        for node in self.redpanda.nodes:
            path = f"config/log_level/{name}?level={level}"
            if expires is not None:
                path = f"{path}&expires={expires}"
            if force:
                path = f"{path}&force=true"
            responses.append(self._request('put', path, node=node).json())
        return responses

    def get_brokers(self, node=None):
        """
        Return metadata about brokers.
        """
        return self._request('get', "brokers", node=node).json()

    def get_broker(self, id, node=None):
        """
        Return metadata about a broker.

        This differs from the `brokers/` endpoint in that it may contain
        additional context beyond what is reported for through `brokers/`.
        """
        return self._request('get', f"brokers/{id}", node=node).json()

    def get_cluster_view(self, node):
        """
        Return cluster_view.
        """
        return self._request('get', "cluster_view", node=node).json()

    def get_cluster_health_overview(self, node=None):

        return self._request('get', "cluster/health_overview",
                             node=node).json()

    def decommission_broker(self, id, node=None):
        """
        Decommission broker
        """
        path = f"brokers/{id}/decommission"
        self.redpanda.logger.debug(f"decommissioning {path}")
        return self._request('put', path, node=node)

    def get_decommission_status(self, id, node=None):
        """
        Get broker decommission status
        """
        path = f"brokers/{id}/decommission"
        return self._request('get', path, node=node).json()

    def recommission_broker(self, id, node=None):
        """
        Recommission broker i.e. abort ongoing decommissioning
        """
        path = f"brokers/{id}/recommission"
        self.redpanda.logger.debug(f"recommissioning {id}")
        return self._request('put', path, node=node)

    def trigger_rebalance(self, node=None):
        """
        Trigger on demand partitions rebalancing
        """
        path = f"partitions/rebalance"

        return self._request('post', path, node=node)

    def trigger_cores_rebalance(self, node):
        """
        Trigger core placement rebalancing for partitions in this node.
        """
        path = f"partitions/rebalance_cores"

        return self._request('post', path, node=node)

    def list_reconfigurations(self, node=None):
        """
        List pending reconfigurations
        """
        path = f"partitions/reconfigurations"

        return self._request('get', path, node=node).json()

    def cancel_all_reconfigurations(self, node=None):
        """
        Cancel all pending reconfigurations
        """
        path = "cluster/cancel_reconfigurations"

        return self._request('post', path, node=node).json()

    def cancel_all_node_reconfigurations(self, target_id, node=None):
        """
        Cancel all reconfigurations moving partition replicas from node
        """
        path = f"brokers/{target_id}/cancel_partition_moves"

        return self._request('post', path, node=node).json()

    def get_partitions(self,
                       topic=None,
                       partition=None,
                       *,
                       namespace=None,
                       node=None):
        """
        Return partition metadata from controller. This includes low-level
        information like replica set assignments with core affinities.
        """
        assert (topic is None and partition is None) or \
                (topic is not None)

        namespace = namespace or "kafka"
        path = "partitions"
        if topic:
            path = f"{path}/{namespace}/{topic}"

        if partition is not None:
            path = f"{path}/{partition}"

        return self._request('get', path, node=node).json()

    def get_partition(self, ns: str, topic: str, id: int, node=None):
        return self._request("GET", f"partitions/{ns}/{topic}/{id}",
                             node=node).json()

    def get_transactions(self, topic, partition, namespace, node=None):
        """
        Get transaction for current partition
        """
        path = f"partitions/{namespace}/{topic}/{partition}/transactions"
        return self._request('get', path, node=node).json()

    def get_all_transactions(self, node=None):
        """
        Get all transactions
        """
        cluster_config = self.get_cluster_config(include_defaults=True)
        tm_partition_amount = cluster_config[
            "transaction_coordinator_partitions"]
        result = []
        for partition in range(tm_partition_amount):
            self.await_stable_leader(topic="tx",
                                     namespace="kafka_internal",
                                     partition=partition)
            path = f"transactions?coordinator_partition_id={partition}"
            partition_res = self._request('get', path, node=node).json()
            result.extend(partition_res)
        return result

    def mark_transaction_expired(self,
                                 topic,
                                 partition,
                                 pid,
                                 namespace,
                                 node=None):
        """
        Mark transaction expired for partition
        """
        path = f"partitions/{namespace}/{topic}/{partition}/mark_transaction_expired?id={pid['id']}&epoch={pid['epoch']}"
        return self._request("post", path, node=node)

    def delete_partition_from_transaction(self,
                                          tid,
                                          namespace,
                                          topic,
                                          partition_id,
                                          etag,
                                          node=None):
        """
        Delete partition from transaction
        """
        partition_info = {
            "namespace": namespace,
            "topic": topic,
            "partition_id": partition_id,
            "etag": etag
        }

        params = "&".join([f"{k}={v}" for k, v in partition_info.items()])
        path = f"transaction/{tid}/delete_partition/?{params}"
        return self._request('post', path, node=node)

    def find_tx_coordinator(self, tid, node=None):
        """
        Find tx coordinator by tx.id
        """
        path = f"transaction/{tid}/find_coordinator"
        return self._request('get', path, node=node).json()

    def set_partition_replicas(self,
                               topic,
                               partition,
                               replicas,
                               *,
                               namespace="kafka",
                               node=None):
        """
        [ {"node_id": 0, "core": 1}, ... ]
        """
        path = f"partitions/{namespace}/{topic}/{partition}/replicas"
        return self._request('post', path, node=node, json=replicas)

    def force_set_partition_replicas(self,
                                     topic,
                                     partition,
                                     replicas,
                                     *,
                                     namespace="kafka",
                                     node=None):
        """
        [ {"node_id": 0, "core": 1}, ... ]
        """
        path = f"debug/partitions/{namespace}/{topic}/{partition}/force_replicas"
        return self._request('post', path, node=node, json=replicas)

    def cancel_partition_move(self,
                              topic,
                              partition,
                              namespace="kafka",
                              node=None):

        path = f"partitions/{namespace}/{topic}/{partition}/cancel_reconfiguration"
        return self._request('post', path, node=node)

    def force_abort_partition_move(self,
                                   topic,
                                   partition,
                                   namespace="kafka",
                                   node=None):

        path = f"partitions/{namespace}/{topic}/{partition}/unclean_abort_reconfiguration"
        return self._request('post', path, node=node)

    def get_majority_lost_partitions_from_nodes(self,
                                                dead_brokers: list[int],
                                                node=None,
                                                **kwargs):
        assert dead_brokers
        brokers_csv = ','.join(str(b) for b in dead_brokers)
        path = f"partitions/majority_lost?dead_nodes={brokers_csv}"
        return self._request('get', path, node, **kwargs).json()

    def force_recover_partitions_from_nodes(self, payload: dict, node=None):
        assert payload
        path = "partitions/force_recover_from_nodes"
        return self._request('post', path, node, json=payload)

    def set_partition_replica_core(self,
                                   topic: str,
                                   partition: int,
                                   replica: int,
                                   core: int,
                                   namespace: str = "kafka",
                                   node=None):
        path = f"partitions/{namespace}/{topic}/{partition}/replicas/{replica}"
        return self._request('post', path, node=node, json={"core": core})

    def create_user(self,
                    username,
                    password="12345678",
                    algorithm="SCRAM-SHA-256"):
        self.redpanda.logger.debug(
            f"Creating user {username}:{password}:{algorithm}")

        path = f"security/users"

        self._request("POST",
                      path,
                      json=dict(
                          username=username,
                          password=password,
                          algorithm=algorithm,
                      ))

    def delete_user(self, username):
        self.redpanda.logger.info(f"Deleting user {username}")

        path = f"security/users/{username}"

        self._request("delete", path)

    def update_user(self, username, password, algorithm):
        self.redpanda.logger.info(
            f"Updating user {username}:{password}:{algorithm}")

        self._request("PUT",
                      f"security/users/{username}",
                      json=dict(
                          username=username,
                          password=password,
                          algorithm=algorithm,
                      ))

    def list_users(self, node=None, include_ephemeral: Optional[bool] = False):
        params = None if include_ephemeral is None else {
            "include_ephemeral": f"{include_ephemeral}".lower()
        }
        return self._request("get", "security/users", node=node,
                             params=params).json()

    def list_user_roles(self, filter: Optional[str] = None):
        params = {}
        if filter is not None:
            params['filter'] = filter
        return self._request("get", f"security/users/roles", params=params)

    def create_role(self, role: str):
        return self._request("post", "security/roles", json=dict(role=role))

    def get_role(self, role: str):
        return self._request("get", f"security/roles/{role}")

    def delete_role(self, role: str, delete_acls: Optional[bool] = None):
        params = None if delete_acls is None else dict(delete_acls=delete_acls)
        return self._request("delete", f"security/roles/{role}", params=params)

    def list_roles(self,
                   filter: Optional[str] = None,
                   principal: Optional[str] = None,
                   principal_type: Optional[str] = None,
                   node=None):
        params = {}
        if filter is not None:
            params['filter'] = filter
        if principal is not None:
            params['principal'] = principal
        if principal_type is not None:
            params['principal_type'] = principal_type
        return self._request("get", "security/roles", params=params, node=node)

    def update_role_members(self,
                            role: str,
                            add: Optional[list[RoleMember]] = [],
                            remove: Optional[list[RoleMember]] = [],
                            create: Optional[bool] = None):

        to_add = [m._asdict() for m in add] if add is not None else []
        to_remove = [m._asdict() for m in remove] if remove is not None else []

        params = {}
        if create is not None:
            params['create'] = create

        return self._request("post",
                             f"security/roles/{role}/members",
                             params=params,
                             json=dict(add=to_add, remove=to_remove))

    def list_role_members(self, role: str):
        return self._request("get", f"security/roles/{role}/members")

    def partition_transfer_leadership(self,
                                      namespace,
                                      topic,
                                      partition,
                                      target_id=None):
        path = f"partitions/{namespace}/{topic}/{partition}/transfer_leadership"
        if target_id:
            path += f"?target={target_id}"

        self._request("POST", path)

    def get_partition_leader(self, *, namespace, topic, partition, node=None):
        partition_info = self.get_partitions(topic=topic,
                                             partition=partition,
                                             namespace=namespace,
                                             node=node)

        return partition_info['leader_id']

    def transfer_leadership_to(self,
                               *,
                               namespace,
                               topic,
                               partition,
                               target_id=None,
                               leader_id=None):
        """
        Looks up current ntp leader and transfer leadership to target node,
        this operations is NOP when current leader is the same as target.
        If user pass None for target this function will choose next replica for new leader.
        Returns true if leadership was transferred to the target node.
        """
        def _get_details():
            p = self.get_partitions(topic=topic,
                                    partition=partition,
                                    namespace=namespace)
            self.redpanda.logger.debug(
                f"ntp {namespace}/{topic}/{partition} details: {p}")
            return p

        #  check which node is current leader

        if leader_id == None:
            leader_id = self.await_stable_leader(topic,
                                                 partition=partition,
                                                 namespace=namespace,
                                                 timeout_s=30,
                                                 backoff_s=2)

        details = _get_details()

        if target_id is not None:
            if leader_id == target_id:
                return True
            path = f"raft/{details['raft_group_id']}/transfer_leadership?target={target_id}"
        else:
            path = f"raft/{details['raft_group_id']}/transfer_leadership"

        leader = self.redpanda.get_node(leader_id)
        ret = self._request('post', path=path, node=leader)
        return ret.status_code == 200

    def maintenance_start(self, node, dst_node=None):
        """
        Start maintenance on 'node', sending the request to 'dst_node'.
        """
        id = self.redpanda.node_id(node)
        url = f"brokers/{id}/maintenance"
        self.redpanda.logger.info(
            f"Starting maintenance on node {node.name}/{id}")
        return self._request("put", url, node=dst_node)

    def maintenance_stop(self, node, dst_node=None):
        """
        Stop maintenance on 'node', sending the request to 'dst_node'.
        """
        id = self.redpanda.node_id(node)
        url = f"brokers/{id}/maintenance"
        self.redpanda.logger.info(
            f"Stopping maintenance on node {node.name}/{id}")
        return self._request("delete", url, node=dst_node)

    def maintenance_status(self, node):
        """
        Get maintenance status of a node.
        """
        id = self.redpanda.node_id(node)
        self.redpanda.logger.info(
            f"Getting maintenance status on node {node.name}/{id}")
        return self._request("get", "maintenance", node=node).json()

    def reset_leaders_info(self, node):
        """
        Reset info for leaders on node
        """
        id = self.redpanda.node_id(node)
        self.redpanda.logger.info(f"Reset leaders info on {node.name}/{id}")
        url = "debug/reset_leaders"
        return self._request("post", url, node=node)

    def get_leaders_info(self, node=None):
        """
        Get info for leaders on node
        """
        if node:
            id = self.redpanda.node_id(node)
            self.redpanda.logger.info(f"Get leaders info on {node.name}/{id}")
        else:
            self.redpanda.logger.info(f"Get leaders info on any node")

        url = "debug/partition_leaders_table"
        return self._request("get", url, node=node).json()

    def si_sync_local_state(self, topic, partition, node=None):
        """
        Check data in the S3 bucket and fix local index if needed
        """
        path = f"cloud_storage/sync_local_state/{topic}/{partition}"
        return self._request('post', path, node=node)

    def get_partition_balancer_status(self, node=None, **kwargs):
        return self._request("GET",
                             "cluster/partition_balancer/status",
                             node=node,
                             **kwargs).json()

    def get_peer_status(self, node, peer_id):
        return self._request("GET", f"debug/peer_status/{peer_id}",
                             node=node).json()

    def get_controller_status(self, node):
        return self._request("GET", f"debug/controller_status",
                             node=node).json()

    def get_cluster_uuid(self, node=None):
        try:
            r = self._request("GET", "cluster/uuid", node=node)
        except HTTPError as ex:
            if ex.response.status_code == 404:
                return
            raise
        if len(r.text) > 0:
            return r.json()["cluster_uuid"]

    def initiate_topic_scan_and_recovery(self,
                                         payload: Optional[dict] = None,
                                         force_acquire_lock: bool = False,
                                         node=None,
                                         **kwargs):
        request_args = {'node': node, **kwargs}

        if payload:
            request_args['json'] = payload
        return self._request('post', "cloud_storage/topic_recovery",
                             **request_args)

    def get_topic_recovery_status(self, node=None, **kwargs):
        request_args = {'node': node, **kwargs}
        return self._request('get',
                             "cloud_storage/topic_recovery?extended=true",
                             **request_args)

    def initialize_cluster_recovery(self, node=None, **kwargs):
        request_args = {'node': node, **kwargs}

        return self._request('post', "cloud_storage/automated_recovery",
                             **request_args)

    def get_cluster_recovery_status(self, node=None, **kwargs):
        request_args = {'node': node, **kwargs}
        return self._request('get', "cloud_storage/automated_recovery",
                             **request_args)

    def self_test_start(self, options):
        return self._request("POST", "debug/self_test/start", json=options)

    def self_test_stop(self):
        return self._request("POST", "debug/self_test/stop")

    def self_test_status(self):
        return self._request("GET", "debug/self_test/status").json()

    def restart_service(self,
                        rp_service: Optional[str] = None,
                        node: Optional[ClusterNode] = None):
        service_param = f"service={rp_service if rp_service is not None else ''}"
        return self._request("PUT",
                             f"debug/restart_service?{service_param}",
                             node=node)

    def is_node_isolated(self, node):
        return self._request("GET", "debug/is_node_isolated", node=node).json()

    def stress_fiber_start(
        self,
        node,
        num_fibers,
        min_spins_per_scheduling_point=None,
        max_spins_per_scheduling_point=None,
        min_ms_per_scheduling_point=None,
        max_ms_per_scheduling_point=None,
    ):
        p = {"num_fibers": str(num_fibers)}
        if min_spins_per_scheduling_point is not None:
            p["min_spins_per_scheduling_point"] = str(
                min_spins_per_scheduling_point)
        if max_spins_per_scheduling_point is not None:
            p["max_spins_per_scheduling_point"] = str(
                max_spins_per_scheduling_point)
        if min_ms_per_scheduling_point is not None:
            p["min_ms_per_scheduling_point"] = str(min_ms_per_scheduling_point)
        if max_ms_per_scheduling_point is not None:
            p["max_ms_per_scheduling_point"] = str(max_ms_per_scheduling_point)
        kwargs = {"params": p}
        return self._request("PUT",
                             "debug/stress_fiber_start",
                             node=node,
                             **kwargs)

    def stress_fiber_stop(self, node):
        return self._request("PUT", "debug/stress_fiber_stop", node=node)

    def cloud_storage_usage(self) -> int:
        return int(
            self._request(
                "GET", "debug/cloud_storage_usage?retries_allowed=10").json())

    def get_usage(self, node, include_open: bool = True):
        return self._request("GET",
                             f"usage?include_open_bucket={str(include_open)}",
                             node=node).json()

    def refresh_disk_health_info(self, node: Optional[ClusterNode] = None):
        """
        Reset info for cluster health on node
        """
        return self._request("post",
                             "debug/refresh_disk_health_info",
                             node=node)

    def get_partition_cloud_storage_status(self, topic, partition, node=None):
        return self._request("GET",
                             f"cloud_storage/status/{topic}/{partition}",
                             node=node).json()

    def get_partition_manifest(self, topic: str, partition: int):
        """
        Get the in-memory partition manifest for the requested ntp
        """
        return self._request(
            "GET", f"cloud_storage/manifest/{topic}/{partition}").json()

    def get_partition_state(self, namespace, topic, partition, node=None):
        path = f"debug/partition/{namespace}/{topic}/{partition}"
        return self._request("GET", path, node=node).json()

    def get_local_storage_usage(self, node=None):
        """
        Get the local storage usage report.
        """
        return self._request("get", "debug/local_storage_usage",
                             node=node).json()

    def get_disk_stat(self, disk_type, node):
        """
        Get disk_type stat from node.
        """
        return self._request("get",
                             f"debug/storage/disk_stat/{disk_type}",
                             node=node).json()

    def set_disk_stat_override(self,
                               disk_type,
                               node,
                               *,
                               total_bytes=None,
                               free_bytes=None,
                               free_bytes_delta=None):
        """
        Get disk_type stat from node.
        """
        json = {}
        if total_bytes is not None:
            json["total_bytes"] = total_bytes
        if free_bytes is not None:
            json["free_bytes"] = free_bytes
        if free_bytes_delta is not None:
            json["free_bytes_delta"] = free_bytes_delta
        return self._request("put",
                             f"debug/storage/disk_stat/{disk_type}",
                             json=json,
                             node=node)

    def get_sampled_memory_profile(self, node=None, shard=None):
        """
        Gets the sampled memory profile debug output
        """
        if shard is not None:
            kwargs = {"params": {"shard": shard}}
        else:
            kwargs = {}

        return self._request("get",
                             "debug/sampled_memory_profile",
                             node=node,
                             **kwargs).json()

    def get_cpu_profile(self, node=None, wait_ms=None):
        """
        Get the CPU profile of a node.
        """
        path = "debug/cpu_profile"
        params = {}
        timeout = DEFAULT_TIMEOUT

        if wait_ms:
            params["wait_ms"] = wait_ms
            timeout = max(2 * (int(wait_ms) // 1_000), timeout)

        return self._request("get",
                             path,
                             node=node,
                             timeout=timeout,
                             params=params).json()

    def get_local_offsets_translated(self,
                                     offsets,
                                     topic,
                                     partition,
                                     translate_to="kafka",
                                     node=None):
        """
        Query offset translator to translate offsets from one type to another

        Options for param "translate_to" are "kafka" and "redpanda"
        """
        return self._request(
            "get",
            f"debug/storage/offset_translator/kafka/{topic}/{partition}?translate_to={translate_to}",
            node=node,
            json=offsets).json()

    def set_storage_failure_injection(self, node, value: bool):
        str_value = "true" if value else "false"
        return self._request(
            "PUT",
            f"debug/set_storage_failure_injection_enabled?value={str_value}",
            node=node)

    def get_raft_recovery_status(self, *, node: ClusterNode):
        """
        Node must be specified because this API reports on node-local state:
        it would not make sense to send it to just any node.
        """
        return self._request("GET", "raft/recovery/status", node=node).json()

    def get_cloud_storage_anomalies(self, namespace: str, topic: str,
                                    partition: int):
        return self._request(
            "GET",
            f"cloud_storage/anomalies/{namespace}/{topic}/{partition}").json()

    def reset_scrubbing_metadata(self,
                                 namespace: str,
                                 topic: str,
                                 partition: int,
                                 node: Optional[ClusterNode] = None):
        return self._request(
            "POST",
            f"cloud_storage/reset_scrubbing_metadata/{namespace}/{topic}/{partition}",
            node=node)

    def get_cluster_partitions(self,
                               ns: str | None = None,
                               topic: str | None = None,
                               disabled: bool | None = None,
                               with_internal: bool | None = None,
                               node=None):
        if topic is not None:
            assert ns is not None
            req = f"cluster/partitions/{ns}/{topic}"
        else:
            assert ns is None
            req = f"cluster/partitions"

        if disabled is not None:
            req += f"?disabled={disabled}"

        if with_internal is not None:
            req += f"?with_internal={with_internal}"

        return self._request("GET", req, node=node).json()

    def set_partitions_disabled(self,
                                ns: str | None = None,
                                topic: str | None = None,
                                partition: int | None = None,
                                value: bool = True):
        if partition is not None:
            req = f"cluster/partitions/{ns}/{topic}/{partition}"
        else:
            req = f"cluster/partitions/{ns}/{topic}"
        return self._request("POST", req, json={"disabled": value})

    def reset_crash_tracking(self, node):
        return self._request("PUT", "reset_crash_tracking", node=node)

    def migrate_tx_manager_in_recovery(self, node):
        return self._request("POST", "recovery/migrate_tx_manager", node=node)

    def get_tx_manager_recovery_status(self,
                                       node: Optional[ClusterNode] = None):
        return self._request("GET", "recovery/migrate_tx_manager", node=node)

    def get_broker_uuids(self, node: Optional[ClusterNode] = None):
        return self._request("GET", "broker_uuids", node=node).json()

    def get_broker_uuid(self, node: ClusterNode):
        return self._request("GET", "debug/broker_uuid", node=node).json()

    def override_node_id(self, node, current_uuid: str, new_node_id: int,
                         new_node_uuid: str):
        return self._request("PUT",
                             "debug/broker_uuid",
                             node=node,
                             json={
                                 "current_node_uuid": current_uuid,
                                 "new_node_uuid": new_node_uuid,
                                 "new_node_id": new_node_id,
                             })

    def transforms_list_committed_offsets(
            self,
            show_unknown: bool = False,
            node: Optional[ClusterNode] = None) -> list[CommittedWasmOffset]:
        path = "transform/debug/committed_offsets"
        if show_unknown:
            path += "?show_unknown=true"
        raw = self._request("GET", path, node=node).json()
        return [
            CommittedWasmOffset(c["transform_name"], c["partition"],
                                c["offset"]) for c in raw
        ]

    def transforms_gc_committed_offsets(self,
                                        node: Optional[ClusterNode] = None):
        path = "transform/debug/committed_offsets/garbage_collect"
        return self._request("POST", path, node=node)

    def transforms_patch_meta(self,
                              name: str,
                              pause: bool | None = None,
                              env: dict[str, str] | None = None):
        path = f"transform/{name}/meta"
        body = {}
        if pause is not None:
            body["is_paused"] = pause
        if env is not None:
            body["env"] = [dict(key=k, value=env[k]) for k in env]
        return self._request("PUT", path, json=body)

    def list_data_migrations(self, node: Optional[ClusterNode] = None):
        path = "migrations"
        return self._request("GET", path, node=node)

    def get_data_migration(self,
                           migration_id: int,
                           node: Optional[ClusterNode] = None):
        path = f"migrations/{migration_id}"
        return self._request("GET", path, node=node)

    def create_data_migration(self,
                              migration: InboundDataMigration
                              | OutboundDataMigration,
                              node: Optional[ClusterNode] = None):

        path = "migrations"
        return self._request("PUT", path, node=node, json=migration.as_dict())

    def execute_data_migration_action(self,
                                      migration_id: int,
                                      action: MigrationAction,
                                      node: Optional[ClusterNode] = None):

        path = f"migrations/{migration_id}?action={action.value}"
        return self._request("POST", path, node=node)

    def delete_data_migration(self,
                              migration_id: int,
                              node: Optional[ClusterNode] = None):

        path = f"migrations/{migration_id}"
        return self._request("DELETE", path, node=node)

    def unmount_topics(self,
                       topics: list[NamespacedTopic],
                       node: Optional[ClusterNode] = None):
        path = "topics/unmount"
        return self._request("POST",
                             path,
                             node=node,
                             json={"topics": [t.as_dict() for t in topics]})

    def mount_topics(self,
                     topics: list[InboundTopic],
                     node: Optional[ClusterNode] = None):
        path = "topics/mount"
        return self._request("POST",
                             path,
                             node=node,
                             json={"topics": [t.as_dict() for t in topics]})

    def post_debug_bundle(self,
                          config: DebugBundleStartConfig,
                          ignore_none: bool = True,
                          node: MaybeNode = None):
        path = "debug/bundle"
        body = json.dumps(config,
                          cls=DebugBundleEncoder,
                          ignore_none=ignore_none)
        self.redpanda.logger.debug(f"Posting debug bundle: {body}")
        return self._request("POST", path, data=body, node=node)

    def get_debug_bundle(self, node: MaybeNode = None):
        path = "debug/bundle"
        return self._request("GET", path, node=node)

    def delete_debug_bundle(self, job_id: UUID, node: MaybeNode = None):
        path = f"debug/bundle/{job_id}"
        return self._request("DELETE", path, node=node)

    def get_debug_bundle_file(self, filename: str, node: MaybeNode = None):
        path = f"debug/bundle/file/{filename}"
        return self._request("GET", path, node=node)

    def delete_debug_bundle_file(self, filename: str, node: MaybeNode = None):
        path = f"debug/bundle/file/{filename}"
        return self._request("DELETE", path, node=node)
