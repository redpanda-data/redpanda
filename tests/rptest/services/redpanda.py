# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import concurrent.futures
from connectrpc.errors import ConnectError, ConnectErrorCode
from contextlib import contextmanager
import copy
import dataclasses
import enum
import json
import os
import pathlib
import random
import re
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import uuid
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from logging import Logger
from typing import (
    Any,
    Callable,
    Collection,
    Iterable,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Protocol,
    Set,
    Tuple,
    TypedDict,
    TypeVar,
    cast,
)

from signal import SIGKILL, SIGTERM, Signals

import requests
import yaml
from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.remoteaccount import RemoteAccount, RemoteCommandError
from ducktape.errors import TimeoutError
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.utils.util import wait_until
from prometheus_client.metrics_core import Metric
from prometheus_client.parser import text_string_to_metric_families
from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError

from rptest.archival.abs_client import ABSClient
from rptest.archival.s3_client import S3AddressingStyle, S3Client
from rptest.clients.admin.proto.redpanda.core.admin.internal.cloud_topics.v1 import (
    metastore_pb2 as metastore_pb,
)
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.installpack import InstallPackClient
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kubectl import KubectlTool, is_redpanda_pod
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rp_storage_tool import RpStorageTool
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.context.cloud_storage import (
    CloudStorageType,  # noqa: F401 # Re-exported for backwards compatibility.
)
from rptest.context.gcp import GCPContext
from rptest.services import redpanda_types, tls
from rptest.services.admin import Admin
from rptest.services.cloud_broker import CloudBroker
from rptest.services.redpanda_cloud import (
    CloudCluster,
    get_config_profile_name,
    ThroughputTierInfo,
)
from rptest.services.redpanda_installer import (
    VERSION_RE as RI_VERSION_RE,
)
from rptest.services.redpanda_installer import (
    RedpandaInstaller,
    RedpandaVersionTriple,
)
from rptest.services.redpanda_installer import (
    int_tuple as ri_int_tuple,
)
from rptest.services.redpanda_types import (
    CompiledLogAllowElem,
    CompiledLogAllowList,
    KafkaClientSecurity,
    LogAllowList,
    LogAllowListElem,
)
from rptest.services.rolling_restarter import RollingRestarter
from rptest.services.storage import ClusterStorage, NodeCacheStorage, NodeStorage
from rptest.services.storage_failure_injection import FailureInjectionConfig
from rptest.services.utils import LogSearchCloud, LogSearchLocal, NodeCrash, Stopwatch
from rptest.util import (
    inject_remote_script,
    not_none,
    ssh_output_stderr,
    wait_until_result,
    wait_until_with_progress_check,
    debounce,
    get_fips_mode,
)
from rptest.utils.mode_checks import in_fips_environment
from rptest.utils.rpenv import sample_license

T = TypeVar("T")
U = TypeVar("U")


class Partition(NamedTuple):
    topic: str
    index: int  # type: ignore existing name clash, fix later
    leader: ClusterNode | None
    replicas: list[ClusterNode]


# TODO use the same tuple approach for replicas in Partition above and
# remove CloudStoragePartition
class CloudStoragePartition(NamedTuple):
    topic: str
    index: int  # pyright: ignore[reportIncompatibleMethodOverride] - fix this later
    leader: ClusterNode | None
    replicas: tuple[ClusterNode, ...]


class MetricSample(NamedTuple):
    sample: str
    """The metric name, i.e., the part before the `{` in Prometheus exposition format."""
    node: Any
    """The node the metric was queried from."""
    value: float
    """The value of the metric sample."""
    labels: dict[str, str]
    """The labels attached to the metric sample."""


@dataclass
class UsageStats:
    disk_bytes_read: int = 0
    disk_bytes_written: int = 0
    batches_read: int = 0
    batches_written: int = 0
    internal_rpc_bytes_recv: int = 0
    internal_rpc_bytes_sent: int = 0
    # Number of PUT operations
    cloud_storage_puts: int = 0
    # Number of GET operations
    cloud_storage_gets: int = 0


@dataclass
class CloudStorageUsage:
    object_count: int = 0
    total_bytes_stored: int = 0


class CloudStorageCleanupStrategy(enum.Enum):
    # I.e. if we are using a lifecycle rule, then we do NOT clean the bucket
    IF_NOT_USING_LIFECYCLE_RULE = "IF_NOT_USING_LIFECYCLE_RULE"

    # Ignore large buckets (based on number of objects). For small buckets, ALWAYS clean.
    ALWAYS_SMALL_BUCKETS_ONLY = "ALWAYS_SMALL_BUCKETS_ONLY"


class NodeNotFoundError(Exception):
    pass


SaslCredentials = redpanda_types.SaslCredentials

# Map of path -> (checksum, size)
FileToChecksumSize = dict[str, Tuple[str, int]]

# Map of node -> (path -> (arbitrary dictionary))
NodeConfigOverridesT = dict[ClusterNode, dict[str, Any]]

# The endpoint info for the azurite (Azure ABS emulator )container that
# is used when running tests in a docker environment.
AZURITE_HOSTNAME = "azurite"
AZURITE_PORT = 10000

DEFAULT_LOG_ALLOW_LIST: list[CompiledLogAllowElem] = [
    # Tests may be run on workstations that do not have XFS filesystem volumes
    # for containers.
    # Pre-23.2 version of the message
    re.compile("not on XFS. This is a non-supported setup."),
    # >= 23.2 version of the message
    re.compile("not on XFS or ext4. This is a non-supported"),
    # >= 23.2.22, 23.3 version of the message
    re.compile("not XFS or ext4. This is a unsupported"),
    # This is expected when tests are intentionally run on low memory configurations
    re.compile(r"Memory: '\d+' below recommended"),
    # A client disconnecting is not bad behaviour on redpanda's part
    re.compile(r"kafka rpc protocol.*(Connection reset by peer|Broken pipe)"),
    # Sometines we're getting 'Internal Server Error' from S3 on CDT and it doesn't
    # lead to any test failure because the error is transient (AWS weather).
    re.compile(r"unexpected REST API error \"Internal Server Error\" detected"),
    # Redpanda upgrade tests will use the default location of rpk (/usr/bin/rpk)
    # which is not present in typical CI runs, which results in the following
    # error message from the debug bundle service
    re.compile(r"Current specified RPK location"),
    # Tests that use Iceberg REST catalogs may log error messages coming from
    # the catalog that have "Assert" in them. These are typically benign and
    # just indicate a race in committing to Iceberg.
    re.compile(r"UpdateRequirement.*Assert"),
    re.compile("assert-ref-snapshot-id"),
    re.compile("assert-current-schema-id"),
    re.compile("assert-last-assigned-partition-id"),
    re.compile("assert-default-spec-id"),
    # Temporary: https://redpandadata.atlassian.net/browse/CORE-9897
    # there is an ongoing investigation into s3_client receiving 400 Bad Request. For now, stop the CI bleed
    re.compile(
        r"S3 PUT request failed with error for key .*: code: _unknown_error_code_, message: http status: Bad Request, error body: 400 Bad Request, request_id: , resource:"
    ),
    re.compile(
        r"Accessing .*, unexpected REST API error \"http status: Bad Request, error body: 400 Bad Request\" detected, code: _unknown_error_code_, request_id: , resource:"
    ),
    re.compile(r"Configuring topic .* with id .*SEGV.*"),
]

# Log errors that are expected in tests that restart nodes mid-test
RESTART_LOG_ALLOW_LIST = [
    re.compile(
        "(raft|rpc) - .*(disconnected_endpoint|Broken pipe|Connection reset by peer)"
    ),
    re.compile("raft - .*recovery append entries error.*client_request_timeout"),
    # cluster - rm_stm.cc:550 - Error "raft::errc:19" on replicating pid:{producer_identity: id=1, epoch=0} commit batch
    # raft::errc:19 is the shutdown error code, the transaction subsystem encounters this and logs at error level
    re.compile('Error "raft::errc:19" on replicating'),
]

# Log errors that are expected in chaos-style tests that e.g.
# stop redpanda nodes uncleanly
CHAOS_LOG_ALLOW_LIST = [
    # Unclean connection shutdown
    re.compile(
        "(raft|rpc) - .*(client_request_timeout|disconnected_endpoint|Broken pipe|Connection reset by peer)"
    ),
    # Failure to progress STMs promptly
    re.compile("raft::offset_monitor::wait_timed_out"),
    # storage - log_manager.cc:415 - Leftover staging file found, removing: /var/lib/redpanda/data/kafka/__consumer_offsets/15_320/0-1-v1.log.staging
    re.compile("storage - .*Leftover staging file"),
    # e.g. cluster - controller_backend.cc:466 - exception while executing partition operation: {type: update_finished, ntp: {kafka/test-topic-1944-1639161306808363/1}, offset: 413, new_assignment: { id: 1, group_id: 65, replicas: {{node_id: 3, shard: 2}, {node_id: 4, shard: 2}, {node_id: 1, shard: 0}} }, previous_assignment: {nullopt}} - std::__1::__fs::filesystem::filesystem_error (error system:39, filesystem error: remove failed: Directory not empty [/var/lib/redpanda/data/kafka/test-topic-1944-1639161306808363])
    re.compile("cluster - .*Directory not empty"),
    re.compile("r/heartbeat - .*cannot find consensus group"),
    re.compile(
        r"cluster - .*exception while executing partition operation:.*std::exception \(std::exception\)"
    ),
    # Failure to handle an internal RPC because the RPC server already handles connections but doesn't yet handle this method. This can happen while the node is still starting up/restarting.
    # e.g. "admin_api_server - server.cc:655 - [_anonymous] exception intercepted - url: [http://ip-172-31-9-208:9644/v1/brokers/7/decommission] http_return_status[500] reason - seastar::httpd::server_error_exception (Unexpected error: rpc::errc::method_not_found)"
    re.compile("admin_api_server - .*Unexpected error: rpc::errc::method_not_found"),
]

# Log errors emitted by refresh credentials system when cloud storage is enabled with IAM roles
# without a corresponding mock service set up to return credentials
IAM_ROLES_API_CALL_ALLOW_LIST = [
    re.compile(r"cloud_roles - .*api request failed"),
    re.compile(r"cloud_roles - .*Failed to get IMDSv2 token"),
    re.compile(r"cloud_roles - .*failed during IAM credentials refresh:"),
]

# Log errors are used in node_operation_fuzzy_test and partition_movement_test
PREV_VERSION_LOG_ALLOW_LIST = [
    # e.g. cluster - controller_backend.cc:400 - Error while reconciling topics - seastar::abort_requested_exception (abort requested)
    "cluster - .*Error while reconciling topic.*",
    # Typo fixed in recent versions.
    # e.g.  raft - [follower: {id: {1}, revision: {10}}] [group_id:3, {kafka/topic/2}] - recovery_stm.cc:422 - recovery append entries error: raft group does not exists on target broker
    "raft - .*raft group does not exists on target broker",
    # e.g. rpc - Service handler thrown an exception - seastar::gate_closed_exception (gate closed)
    "rpc - .*gate_closed_exception.*",
    # Tests on mixed versions will start out with an unclean restart before
    # starting a workload.
    "(raft|rpc) - .*(disconnected_endpoint|Broken pipe|Connection reset by peer)",
    # e.g.  raft - [group_id:3, {kafka/topic/2}] consensus.cc:2317 - unable to replicate updated configuration: raft::errc::replicated_entry_truncated
    "raft - .*unable to replicate updated configuration: .*",
    # e.g. recovery_stm.cc:432 - recovery append entries error: rpc::errc::client_request_timeout"
    "raft - .*recovery append entries error.*client_request_timeout",
    # Pre v23.2 Redpanda's don't know how to interact with HNS Storage Accounts correctly
    "abs - .*FeatureNotYetSupportedForHierarchicalNamespaceAccounts",
    # We added a condition to log these storage parser errors at `DEBUG` during
    # recovery here: https://github.com/redpanda-data/redpanda/pull/27287
    # However, old versions will still log at `ERROR` when there are not enough bytes
    # left due to unclean shutdown in a segment being recovered. Ignore these
    # in a mixed version test.
    "storage - .*parser::consume_records error: parser_errc::input_stream_not_enough_bytes .* storage::checksumming_consumer",
    # Failure to handle Schema Registry requests due to Redpanda being shutdown (fix is in https://github.com/redpanda-data/redpanda/pull/26909)
    "schemaregistry - .* - exception_reply: .*seastar::sleep_aborted",
]

AUDIT_LOG_ALLOW_LIST = RESTART_LOG_ALLOW_LIST + [
    re.compile(".*Failed to audit authentication.*"),
    re.compile(".*Failed to append authz event to audit log.*"),
    re.compile(".*Failed to append authentication event to audit log.*"),
    re.compile(".*Failed to audit authorization request for endpoint.*"),
    re.compile(
        ".*Request to authorize user to modify or view cluster configuration was not audited.*"
    ),
]

# Path to the LSAN suppressions file
LSAN_SUPPRESSIONS_FILE = "/opt/lsan_suppressions.txt"

# Path to the UBSAN suppressions file
UBSAN_SUPPRESSIONS_FILE = "/opt/ubsan_suppressions.txt"

FAILURE_INJECTION_LOG_ALLOW_LIST = [
    re.compile(
        "Assert failure: .* filesystem error: Injected Failure: Input/output error"
    ),
    re.compile("assert - Backtrace:"),
    re.compile("finject - .* flush called concurrently with other operations"),
    re.compile("crash reason to crash file.*(vassert)"),
]

# Log errors that are acceptable for tests that hit the OIDC endpoint but don't
# necessarily test OIDC functionality
OIDC_ALLOW_LIST = [
    re.compile("security - .* - Error updating"),
]

CLOUD_TOPICS_CONFIG_STR = "cloud_topics_enabled"


class RemoteClusterNode(Protocol):
    account: RemoteAccount

    @property
    def name(self) -> str: ...


@dataclass
class MetricSamples:
    samples: list[MetricSample]

    def label_filter(self, labels: Mapping[str, str]) -> "MetricSamples":
        def f(sample: MetricSample) -> bool:
            for key, value in labels.items():
                assert key in sample.labels
                return sample.labels[key] == value
            # TODO: above logic looks wrong, it only matches the first label in labels
            assert False, "unreachable"

        return MetricSamples([s for s in filter(f, self.samples)])


class MetricsEndpoint(Enum):
    METRICS = "metrics"
    PUBLIC_METRICS = "public_metrics"


# for this type value[0] is CloudStorageType and value[1] is url style
# but it needs to be a list rather than a tuple because injected parameters
# must be "json compatible" which means no tuples
CloudStorageTypeAndUrlStyle = list[CloudStorageType | Literal["virtual_host", "path"]]


def prepare_allow_list(allow_list: LogAllowList) -> CompiledLogAllowList:
    def maybe_compile(a: LogAllowListElem) -> CompiledLogAllowElem:
        return re.compile(a) if isinstance(a, str) else a

    return DEFAULT_LOG_ALLOW_LIST + list(map(maybe_compile, allow_list))


def one_or_many(value: Any) -> Any:
    """
    Helper for reading `one_or_many_property` configs when
    we only care about getting one value out
    """
    if isinstance(value, list):
        return cast(Any, value[0])
    else:
        return value


def get_cloud_provider() -> str:
    """
    Returns the cloud provider in use.  If one is not set then return 'docker'
    """
    return os.getenv("CLOUD_PROVIDER", "docker")


def get_cloud_storage_type(
    applies_only_on: list[CloudStorageType] | None = None,
    docker_use_arbitrary: bool = False,
) -> list[CloudStorageType]:
    """
    Returns a list[CloudStorageType] based on the "CLOUD_PROVIDER"
    environment variable. For example:
    CLOUD_PROVIDER=docker => returns: [CloudStorageType.S3, CloudStorageType.ABS]
    CLOUD_PROVIDER=aws => returns: [CloudStorageType.S3]

    :env "CLOUD_PROVIDER": one of "aws", "gcp", "azure" or "docker"
    :param applies_only_on: optional list(CloudStorageType)
    that is the allow-list of the cloud storage type for a
    test.
    If it's set the function will return the inresection
    of:
        * <cloud_storage_type>: discovered based on the CLOUD_PROVIDER env
        * <applies_only_on>: param provided
    :param docker_use_arbitrary: optional bool to use arbitrary backend when
    the cloud provider is docker.
    """

    if applies_only_on is None:
        applies_only_on = []

    cloud_provider = get_cloud_provider()
    if cloud_provider == "docker":
        if docker_use_arbitrary:
            cloud_storage_type = [CloudStorageType.S3]
        else:
            cloud_storage_type = [CloudStorageType.S3, CloudStorageType.ABS]
    elif cloud_provider in ("aws", "gcp"):
        cloud_storage_type = [CloudStorageType.S3]
    elif cloud_provider == "azure":
        cloud_storage_type = [CloudStorageType.ABS]
    else:
        raise RuntimeError(f"bad cloud provider: {cloud_provider}")

    if applies_only_on:
        cloud_storage_type = list(set(applies_only_on).intersection(cloud_storage_type))
    return cloud_storage_type


def get_cloud_storage_url_style(
    cloud_storage_type: CloudStorageType | None = None,
) -> list[Literal["virtual_host", "path"]]:
    if cloud_storage_type is None:
        return ["virtual_host", "path"]

    if cloud_storage_type == CloudStorageType.S3:
        return ["virtual_host", "path"]
    else:
        return ["virtual_host"]


def get_cloud_storage_type_and_url_style() -> List[CloudStorageTypeAndUrlStyle]:
    """
    Returns a list of compatible cloud storage types and url styles.
    I.e, Returns [[CloudStorageType.S3, 'virtual_host'],
                  [CloudStorageType.S3, 'path'],
                  [CloudStorageType.ABS, 'virtual_host']]
    """

    def get_style(t: CloudStorageType) -> List[CloudStorageTypeAndUrlStyle]:
        return [[t, us] for us in get_cloud_storage_url_style(t)]

    return [
        tus
        for tus_list in map(
            get_style,
            get_cloud_storage_type(),
        )
        for tus in tus_list
    ]


def is_redpanda_cloud(context: TestContext) -> bool:
    """
    Returns True if we are running againt a Redpanda Cloud cluster,
    False otherwise."""

    # we use the presence of a non-empty cloud_cluster key in the config as our
    # global signal that it's a cloud run
    return bool(context.globals.get(RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG))


class ResourceSettings:
    """
    Control CPU+memory footprint of Redpanda instances.  Pass one
    of these into your RedpandaTest constructor if you want to e.g.
    create low-memory situations.

    This class also contains defaults for redpanda CPU and memory
    sizing in tests.  If `dedicated_node` is true, even these limits
    are ignored, and Redpanda is allowed to behave in its default
    way of taking all the CPU and memory on the machine.  The
    `dedicated_node` mode is appropriate when using e.g. whole EC2 instances
    as test nodes.
    """

    DEFAULT_NUM_CPUS = 2
    DEFAULT_MEMORY_MB = 2048

    def __init__(
        self,
        *,
        num_cpus: int | None = None,
        memory_mb: int | None = None,
        bypass_fsync: bool | None = None,
        nfiles: int | None = None,
        reactor_stall_threshold: int | None = None,
        core_dump_limit: str | None = "unlimited",
    ) -> None:
        self._num_cpus = num_cpus
        self._memory_mb = memory_mb

        if bypass_fsync is None:
            self._bypass_fsync = False
        else:
            self._bypass_fsync = bypass_fsync

        self._nfiles = nfiles
        self._reactor_stall_threshold = reactor_stall_threshold
        self._core_dump_limit = core_dump_limit

    @property
    def memory_mb(self) -> int | None:
        return self._memory_mb

    @property
    def num_cpus(self) -> int | None:
        return self._num_cpus

    def to_cli(self, *, dedicated_node: bool) -> Tuple[str, str]:
        """

        Generate Redpanda CLI flags based on the settings passed in at construction
        time.

        :return: 2 tuple of strings, first goes before the binary, second goes after it
        """
        preamble = f"ulimit -Sc {self._core_dump_limit}"
        preamble += f" -Sn {self._nfiles}; " if self._nfiles else "; "

        if self._num_cpus is None and not dedicated_node:
            num_cpus = self.DEFAULT_NUM_CPUS
        else:
            num_cpus = self._num_cpus

        if self._memory_mb is None and not dedicated_node:
            memory_mb = self.DEFAULT_MEMORY_MB
        else:
            memory_mb = self._memory_mb

        args: list[str] = []
        if not dedicated_node:
            args.extend(
                [
                    "--kernel-page-cache=true",
                    "--overprovisioned ",
                    "--reserve-memory=0M",
                ]
            )

        if self._reactor_stall_threshold is not None:
            args.append(f"--blocked-reactor-notify-ms={self._reactor_stall_threshold}")

        if num_cpus is not None:
            args.append(f"--smp={num_cpus}")
        if memory_mb is not None:
            args.append(f"--memory={memory_mb}M")

        args.append(f"--unsafe-bypass-fsync={'1' if self._bypass_fsync else '0'}")

        return preamble, " ".join(args)


class SISettings:
    """
    Settings for shadow indexing stuff.
    The defaults are for use with the default minio docker container,
    but if the test was parametrised with 'cloud_storage_type==CloudStorageType.ABS',
    then the resulting settings will be for use with Azurite.

    These settings are altered in RedpandaTest if running on AWS.
    """

    GLOBAL_CLOUD_STORAGE_CRED_SOURCE_KEY = "cloud_store_cred_source"
    GLOBAL_S3_BUCKET = "s3_bucket"
    GLOBAL_S3_ACCESS_KEY = "s3_access_key"
    GLOBAL_S3_SECRET_KEY = "s3_secret_key"
    GLOBAL_S3_REGION_KEY = "s3_region"
    GLOBAL_USE_FIPS_S3_ENDPOINT = "use_fips_s3_endpoint"
    DEFAULT_USE_FIPS_S3_ENDPOINT = "OFF"

    GLOBAL_ABS_STORAGE_ACCOUNT = "abs_storage_account"
    GLOBAL_ABS_SHARED_KEY = "abs_shared_key"
    GLOBAL_AZURE_CLIENT_ID = "azure_client_id"
    GLOBAL_AZURE_CLIENT_SECRET = "azure_client_secret"
    GLOBAL_AZURE_TENANT_ID = "azure_tenant_id"

    GLOBAL_CLOUD_PROVIDER = "cloud_provider"

    # The account and key to use with local Azurite testing.
    # These are the default Azurite (Azure emulator) storage account and shared key.
    # Both are readily available in the docs.
    ABS_AZURITE_ACCOUNT = "devstoreaccount1"
    ABS_AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    def __init__(
        self,
        test_context: TestContext,
        *,
        log_segment_size: int = 16 * 1000000,
        log_segment_size_min: int | None = None,
        cloud_storage_cache_chunk_size: int | None = None,
        cloud_storage_credentials_source: str = "config_file",
        cloud_storage_access_key: str = "panda-user",
        cloud_storage_secret_key: str = "panda-secret",
        cloud_storage_region: str = "panda-region",
        cloud_storage_api_endpoint: str | None = None,
        cloud_storage_api_endpoint_port: int = 9000,
        cloud_storage_url_style: str = "virtual_host",
        cloud_storage_cache_size: int | None = None,
        cloud_storage_cache_max_objects: int | None = None,
        cloud_storage_enable_remote_read: bool = True,
        cloud_storage_enable_remote_write: bool = True,
        cloud_storage_max_connections: int | None = None,
        cloud_storage_disable_tls: bool = True,
        cloud_storage_segment_max_upload_interval_sec: int | None = None,
        cloud_storage_manifest_max_upload_interval_sec: int | None = None,
        cloud_storage_readreplica_manifest_sync_timeout_ms: int | None = None,
        bypass_bucket_creation: bool = False,
        use_bucket_cleanup_policy: bool = True,
        cloud_storage_housekeeping_interval_ms: int | None = None,
        cloud_storage_spillover_manifest_max_segments: int | None = None,
        fast_uploads: bool = False,
        retention_local_strict: bool = True,
        cloud_storage_max_throughput_per_shard: int | None = None,
        cloud_storage_signature_version: str = "s3v4",
        before_call_headers: Callable[[], dict[str, str]] | None = None,
        skip_end_of_test_scrubbing: bool = False,
        addressing_style: S3AddressingStyle = S3AddressingStyle.PATH,
    ) -> None:
        """
        :param fast_uploads: if true, set low upload intervals to help tests run
                             quickly when they wait for uploads to complete.
        """

        self._context = test_context
        self.cloud_storage_type = get_cloud_storage_type()[0]
        if (
            hasattr(test_context, "injected_args")
            and test_context.injected_args is not None
        ):
            if "cloud_storage_type" in test_context.injected_args:
                self.cloud_storage_type = cast(
                    CloudStorageType, test_context.injected_args["cloud_storage_type"]
                )
            elif "cloud_storage_type_and_url_style" in test_context.injected_args:
                self.cloud_storage_type = cast(
                    CloudStorageType,
                    test_context.injected_args["cloud_storage_type_and_url_style"][0],
                )

        # For most cloud storage backends, we clean up small buckets always.
        # In some cases, we will modify this strategy in the block below.
        self.cloud_storage_cleanup_strategy = (
            CloudStorageCleanupStrategy.ALWAYS_SMALL_BUCKETS_ONLY
        )

        self.skip_load_context = False

        if self.cloud_storage_type == CloudStorageType.S3:
            self.cloud_storage_credentials_source = cloud_storage_credentials_source
            self.cloud_storage_access_key = cloud_storage_access_key
            self.cloud_storage_secret_key = cloud_storage_secret_key
            self.cloud_storage_region = cloud_storage_region
            self._cloud_storage_bucket = f"panda-bucket-{uuid.uuid1()}"
            self.cloud_storage_url_style = cloud_storage_url_style

            if cloud_storage_api_endpoint is not None:
                self.skip_load_context = True
                self.cloud_storage_api_endpoint = cloud_storage_api_endpoint
                test_context.logger.debug(
                    "using custom cloud storage api endpoint: %s",
                    cloud_storage_api_endpoint,
                )
            elif test_context.globals.get(self.GLOBAL_CLOUD_PROVIDER, "aws") == "gcp":
                self.cloud_storage_api_endpoint = "storage.googleapis.com"
                # For GCP, we currently use S3 compat API over boto3, which does not support batch deletes
                # This makes cleanup slow, even for small-ish buckets.
                # Therefore, we maintain logic of skipping bucket cleaning completely, if we are using lifecycle rule.
                self.cloud_storage_cleanup_strategy = (
                    CloudStorageCleanupStrategy.IF_NOT_USING_LIFECYCLE_RULE
                )
            else:
                # Endpoint defaults to minio-s3
                self.cloud_storage_api_endpoint = "minio-s3"
            self.cloud_storage_api_endpoint_port = cloud_storage_api_endpoint_port

            if (
                hasattr(test_context, "injected_args")
                and test_context.injected_args is not None
            ):
                if "cloud_storage_url_style" in test_context.injected_args:
                    self.cloud_storage_url_style = test_context.injected_args[
                        "cloud_storage_url_style"
                    ]
                elif "cloud_storage_type_and_url_style" in test_context.injected_args:
                    self.cloud_storage_url_style = test_context.injected_args[
                        "cloud_storage_type_and_url_style"
                    ][1]

        elif self.cloud_storage_type == CloudStorageType.ABS:
            self.cloud_storage_azure_shared_key = self.ABS_AZURITE_KEY
            self.cloud_storage_azure_storage_account = self.ABS_AZURITE_ACCOUNT

            self._cloud_storage_azure_container = f"panda-container-{uuid.uuid1()}"
            self.cloud_storage_api_endpoint = (
                f"{self.cloud_storage_azure_storage_account}.blob.localhost"
            )
            self.cloud_storage_api_endpoint_port = AZURITE_PORT
        else:
            assert False, (
                f"Unexpected value provided for 'cloud_storage_type' injected arg: {self.cloud_storage_type}"
            )

        self.log_segment_size = log_segment_size
        self.log_segment_size_min = log_segment_size_min
        self.cloud_storage_cache_chunk_size = cloud_storage_cache_chunk_size
        self.cloud_storage_cache_size = cloud_storage_cache_size
        self.cloud_storage_cache_max_objects = cloud_storage_cache_max_objects
        self.cloud_storage_enable_remote_read = cloud_storage_enable_remote_read
        self.cloud_storage_enable_remote_write = cloud_storage_enable_remote_write
        self.cloud_storage_max_connections = cloud_storage_max_connections
        self.cloud_storage_disable_tls = cloud_storage_disable_tls
        self.cloud_storage_segment_max_upload_interval_sec = (
            cloud_storage_segment_max_upload_interval_sec
        )
        self.cloud_storage_manifest_max_upload_interval_sec = (
            cloud_storage_manifest_max_upload_interval_sec
        )
        self.cloud_storage_readreplica_manifest_sync_timeout_ms = (
            cloud_storage_readreplica_manifest_sync_timeout_ms
        )
        self.endpoint_url = f"http://{self.cloud_storage_api_endpoint}:{self.cloud_storage_api_endpoint_port}"
        self.bypass_bucket_creation = bypass_bucket_creation
        self.use_bucket_cleanup_policy = use_bucket_cleanup_policy

        self.cloud_storage_housekeeping_interval_ms = (
            cloud_storage_housekeeping_interval_ms
        )
        self.cloud_storage_spillover_manifest_max_segments = (
            cloud_storage_spillover_manifest_max_segments
        )
        self.retention_local_strict = retention_local_strict
        self.cloud_storage_max_throughput_per_shard = (
            cloud_storage_max_throughput_per_shard
        )
        self.cloud_storage_signature_version = cloud_storage_signature_version
        self.before_call_headers = before_call_headers
        self.addressing_style = addressing_style

        self.cloud_storage_segment_upload_timeout_ms = None
        self.cloud_storage_manifest_upload_timeout_ms = None

        # Allow disabling end of test scrubbing.
        # It takes a long time with lots of segments i.e. as created in scale
        # tests. Should figure out how to re-enable it, or consider using
        # redpanda's built-in scrubbing capabilities.
        self.skip_end_of_test_scrubbing = skip_end_of_test_scrubbing

        if fast_uploads:
            self.cloud_storage_segment_max_upload_interval_sec = 10
            self.cloud_storage_manifest_max_upload_interval_sec = 1
            # with fast uploads enabled, it's better to fail a problematic
            # segment upload or download quickly so we can try again
            self.cloud_storage_segment_upload_timeout_ms = 15000

        self._expected_damage_types: set[str] = set()

    def get_use_fips_s3_endpoint(self) -> bool:
        use_fips_option = self._context.globals.get(
            self.GLOBAL_USE_FIPS_S3_ENDPOINT, self.DEFAULT_USE_FIPS_S3_ENDPOINT
        )
        if use_fips_option == "ON":
            return True
        elif use_fips_option == "OFF":
            return False

        raise RuntimeError(
            f"{self.GLOBAL_USE_FIPS_S3_ENDPOINT} should be 'ON', or 'OFF'"
        )

    def use_fips_endpoint(self) -> bool:
        """
        Returns whether or not to use the FIPS S3 endpoints.  Only true if:
        * Cloud provider is AWS, and
        * Cloud storage type is S3, and
        * Either
          * we are in a FIPS environment or,
          * the global variable 'use_fips_s3_endpoint' is 'ON'
        """
        return (
            get_cloud_provider() == "aws"
            and self.cloud_storage_type == CloudStorageType.S3
            and (self.get_use_fips_s3_endpoint() or in_fips_environment())
        )

    def load_context(self, logger: Logger, test_context: TestContext) -> None:
        if self.skip_load_context:
            logger.info("Skipping SISettings.load_context")
            return

        if self.cloud_storage_type == CloudStorageType.S3:
            self._load_s3_context(logger, test_context)
        elif self.cloud_storage_type == CloudStorageType.ABS:
            self._load_abs_context(logger, test_context)

        cloud_storage_url_style = test_context.globals.get(
            "cloud_storage_url_style", None
        )
        if cloud_storage_url_style is not None:
            if cloud_storage_url_style not in ("path", "virtual_host"):
                raise ValueError(
                    f"Invalid cloud_storage_url_style: {cloud_storage_url_style!r}, "
                    f"expected 'path' or 'virtual_host'"
                )
            logger.info(
                f"Overriding cloud_storage_url_style from globals: "
                f"{cloud_storage_url_style}"
            )
            self.cloud_storage_url_style = cloud_storage_url_style
            if cloud_storage_url_style == "path":
                self.addressing_style = S3AddressingStyle.PATH
            else:
                self.addressing_style = S3AddressingStyle.VIRTUAL

    def _load_abs_context(self, logger: Logger, test_context: TestContext) -> None:
        storage_account = test_context.globals.get(
            self.GLOBAL_ABS_STORAGE_ACCOUNT, None
        )
        shared_key = test_context.globals.get(self.GLOBAL_ABS_SHARED_KEY, None)

        if storage_account and shared_key:
            logger.info("Running on Azure, setting credentials from env")
            self.cloud_storage_azure_storage_account = storage_account
            self.cloud_storage_azure_shared_key = shared_key

            self.endpoint_url = None
            self.cloud_storage_disable_tls = False
            self.cloud_storage_api_endpoint_port = 443
        else:
            logger.debug(
                "Running in Dockerised env against Azurite. "
                "Using Azurite defualt credentials."
            )

    def _load_s3_context(self, logger: Logger, test_context: TestContext) -> None:
        """
        Update based on the test context, to e.g. consume AWS access keys in
        the globals dictionary.
        """
        cloud_storage_credentials_source = test_context.globals.get(
            self.GLOBAL_CLOUD_STORAGE_CRED_SOURCE_KEY, "config_file"
        )
        cloud_storage_bucket = test_context.globals.get(self.GLOBAL_S3_BUCKET, None)
        cloud_storage_access_key = test_context.globals.get(
            self.GLOBAL_S3_ACCESS_KEY, None
        )
        cloud_storage_secret_key = test_context.globals.get(
            self.GLOBAL_S3_SECRET_KEY, None
        )
        cloud_storage_region = test_context.globals.get(self.GLOBAL_S3_REGION_KEY, None)

        # Enable S3 if AWS creds were given at globals
        if (
            cloud_storage_credentials_source == "aws_instance_metadata"
            or cloud_storage_credentials_source == "gcp_instance_metadata"
        ):
            logger.info("Running on AWS S3, setting IAM roles")
            self.cloud_storage_credentials_source = cloud_storage_credentials_source
            self.cloud_storage_access_key = None
            self.cloud_storage_secret_key = None
            self.endpoint_url = None  # None so boto auto-gens the endpoint url
            if test_context.globals.get(self.GLOBAL_CLOUD_PROVIDER, "aws") == "gcp":
                self._gcp_context = GCPContext.from_context(test_context)

                self.endpoint_url = "https://storage.googleapis.com"
                self.cloud_storage_signature_version = "unsigned"
                self.before_call_headers = lambda: {
                    "Authorization": f"Bearer {self._gcp_context.fetch_iam_token()}",
                    "x-goog-project-id": str(self._gcp_context.project_id),
                }
            self.cloud_storage_disable_tls = (
                False  # SI will fail to create archivers if tls is disabled
            )
            self.cloud_storage_region = cloud_storage_region
            self.cloud_storage_api_endpoint_port = 443
            self.addressing_style = S3AddressingStyle.VIRTUAL
        elif (
            cloud_storage_credentials_source == "config_file"
            and cloud_storage_access_key
            and cloud_storage_secret_key
        ):
            # `config_file`` source allows developers to run ducktape tests from
            # non-AWS hardware but targeting a real S3 backend.
            logger.info("Running on AWS S3, setting credentials")
            self.cloud_storage_access_key = cloud_storage_access_key
            self.cloud_storage_secret_key = cloud_storage_secret_key
            self.endpoint_url = None  # None so boto auto-gens the endpoint url
            if test_context.globals.get(self.GLOBAL_CLOUD_PROVIDER, "aws") == "gcp":
                self.endpoint_url = "https://storage.googleapis.com"
            self.cloud_storage_disable_tls = (
                False  # SI will fail to create archivers if tls is disabled
            )
            self.cloud_storage_region = cloud_storage_region
            self.cloud_storage_api_endpoint_port = 443
            self.addressing_style = S3AddressingStyle.VIRTUAL

            if cloud_storage_bucket:
                logger.info(f"Using bucket name from globals: {cloud_storage_bucket}")
                self.bypass_bucket_creation = True
                self._cloud_storage_bucket = cloud_storage_bucket
        else:
            logger.info("No AWS credentials supplied, assuming minio defaults")

    @property
    def cloud_storage_bucket(self) -> str:
        if self.cloud_storage_type == CloudStorageType.S3:
            return self._cloud_storage_bucket
        else:
            assert self.cloud_storage_type == CloudStorageType.ABS
            return self._cloud_storage_azure_container

    def reset_cloud_storage_bucket(self, new_bucket_name: str) -> None:
        if self.cloud_storage_type == CloudStorageType.S3:
            self._cloud_storage_bucket = new_bucket_name
        elif self.cloud_storage_type == CloudStorageType.ABS:
            self._cloud_storage_azure_container = new_bucket_name

    # Call this to update the extra_rp_conf
    def update_rp_conf(self, conf: dict[str, Any]) -> dict[str, Any]:
        if self.cloud_storage_type == CloudStorageType.S3:
            conf["cloud_storage_credentials_source"] = (
                self.cloud_storage_credentials_source
            )
            conf["cloud_storage_access_key"] = self.cloud_storage_access_key
            conf["cloud_storage_secret_key"] = self.cloud_storage_secret_key
            conf["cloud_storage_region"] = self.cloud_storage_region
            conf["cloud_storage_bucket"] = self._cloud_storage_bucket
            conf["cloud_storage_url_style"] = self.cloud_storage_url_style
        elif self.cloud_storage_type == CloudStorageType.ABS:
            conf["cloud_storage_azure_storage_account"] = (
                self.cloud_storage_azure_storage_account
            )
            conf["cloud_storage_azure_container"] = self._cloud_storage_azure_container
            conf["cloud_storage_azure_shared_key"] = self.cloud_storage_azure_shared_key

        conf["log_segment_size"] = self.log_segment_size
        if self.log_segment_size_min is not None:
            conf["log_segment_size_min"] = self.log_segment_size_min

        if self.cloud_storage_cache_chunk_size:
            conf["cloud_storage_cache_chunk_size"] = self.cloud_storage_cache_chunk_size

        conf["cloud_storage_enabled"] = True
        if self.cloud_storage_cache_size is None:
            # Default cache size for testing: large enough to enable streaming throughput up to 100MB/s, but no
            # larger, so that a test doing any significant amount of throughput will exercise trimming.
            conf["cloud_storage_cache_size"] = SISettings.cache_size_for_throughput(
                1024 * 1024 * 100
            )
        else:
            conf["cloud_storage_cache_size"] = self.cloud_storage_cache_size

        if self.cloud_storage_cache_max_objects is not None:
            conf["cloud_storage_cache_max_objects"] = (
                self.cloud_storage_cache_max_objects
            )

        conf["cloud_storage_enable_remote_read"] = self.cloud_storage_enable_remote_read
        conf["cloud_storage_enable_remote_write"] = (
            self.cloud_storage_enable_remote_write
        )

        if self.endpoint_url is not None:
            conf["cloud_storage_api_endpoint"] = self.cloud_storage_api_endpoint
            conf["cloud_storage_api_endpoint_port"] = (
                self.cloud_storage_api_endpoint_port
            )

        if self.cloud_storage_disable_tls:
            conf["cloud_storage_disable_tls"] = self.cloud_storage_disable_tls

        if self.cloud_storage_max_connections:
            conf["cloud_storage_max_connections"] = self.cloud_storage_max_connections
        if self.cloud_storage_readreplica_manifest_sync_timeout_ms:
            conf["cloud_storage_readreplica_manifest_sync_timeout_ms"] = (
                self.cloud_storage_readreplica_manifest_sync_timeout_ms
            )
        if self.cloud_storage_segment_max_upload_interval_sec:
            conf["cloud_storage_segment_max_upload_interval_sec"] = (
                self.cloud_storage_segment_max_upload_interval_sec
            )
        if self.cloud_storage_manifest_max_upload_interval_sec:
            conf["cloud_storage_manifest_max_upload_interval_sec"] = (
                self.cloud_storage_manifest_max_upload_interval_sec
            )
        if self.cloud_storage_housekeeping_interval_ms:
            conf["cloud_storage_housekeeping_interval_ms"] = (
                self.cloud_storage_housekeeping_interval_ms
            )
        if self.cloud_storage_spillover_manifest_max_segments:
            conf["cloud_storage_spillover_manifest_max_segments"] = (
                self.cloud_storage_spillover_manifest_max_segments
            )

        conf["retention_local_strict"] = self.retention_local_strict

        if self.cloud_storage_max_throughput_per_shard:
            conf["cloud_storage_max_throughput_per_shard"] = (
                self.cloud_storage_max_throughput_per_shard
            )

        if self.cloud_storage_segment_upload_timeout_ms is not None:
            conf["cloud_storage_segment_upload_timeout_ms"] = (
                self.cloud_storage_segment_upload_timeout_ms
            )

        if self.cloud_storage_manifest_upload_timeout_ms is not None:
            conf["cloud_storage_manifest_upload_timeout_ms"] = (
                self.cloud_storage_manifest_upload_timeout_ms
            )

        # Enable scrubbing in testing unless it was explicitly disabled.
        if "cloud_storage_enable_scrubbing" not in conf:
            conf["cloud_storage_enable_scrubbing"] = True

        return conf

    def set_expected_damage(self, damage_types: set[str]) -> None:
        """
        ***This is NOT for making a racy test pass.  This is ONLY for tests
           that intentionally damage data by e.g. deleting a segment out
           of band ***

        For tests which intentionally damage data in the object storage
        bucket, they may advertise that here in order to waive the default
        bucket consistency checks at end of test.

        :param damage_type: which categories of damage to ignore, from the
                            types known by the tool invoked in
                            RedpandaService.stop_and_scrub_object_storage, e.g.
                            'missing_segments'
        """
        self._expected_damage_types = damage_types

    def is_damage_expected(self, damage_types: set[str]) -> bool:
        return (damage_types & self._expected_damage_types) == damage_types

    def get_expected_damage(self) -> set[str]:
        return self._expected_damage_types

    @classmethod
    def cache_size_for_throughput(cls, throughput_bytes: int) -> int:
        """
        Calculate the cache size required to accomodate a particular
        streaming throughput for consumers.
        """
        # - Cache trim interval is 5 seconds.
        # - Cache trim low watermark is 80%.
        # Effective streaming bandwidth is 20% of cache size every trim period
        return int((throughput_bytes * 5) / 0.2)


class TLSProvider:
    """
    Interface that RedpandaService uses to obtain TLS certificates.
    """

    @property
    def ca(self) -> tls.CertificateAuthority:
        raise NotImplementedError("ca")

    def create_broker_cert(
        self, service: Service, node: ClusterNode
    ) -> tls.Certificate:
        """
        Create a certificate for a broker.
        """
        raise NotImplementedError("create_broker_cert")

    def create_service_client_cert(
        self, service: Service, name: str
    ) -> tls.Certificate:
        """
        Create a certificate for an internal service client.
        """
        raise NotImplementedError("create_service_client_cert")

    def use_pkcs12_file(self) -> bool:
        """
        Use the generated PKCS#12 file instead of the key/cert
        """
        return False

    def p12_password(self, node: ClusterNode) -> str:
        """
        Get the PKCS#12 file password for the node
        """
        raise NotImplementedError("p12_password")


class SaslMechanismOverride(TypedDict):
    listener: str
    sasl_mechanisms: list[str]


class SecurityConfig:
    # the system currently has a single principal mapping rule. this is
    # sufficient to get our first mTLS tests put together, but isn't general
    # enough to cover all the cases. one awkward thing that came up was: admin
    # clients used by the service and clients used by tests both may need to
    # have mappings. the internal admin client can use something fixed, but
    # tests may want to use a variety of rules. currently it is hard to combine
    # the rules, so instead we use a fixed mapping and arrange for certs to use
    # a similar format. this will change when we get closer to GA and the
    # configuration becomes more general.
    __DEFAULT_PRINCIPAL_MAPPING_RULES = "RULE:^O=Redpanda,CN=(.*?)$/$1/L, DEFAULT"

    def __init__(self) -> None:
        self.enable_sasl = False
        self.kafka_enable_authorization: bool | None = None
        self.sasl_mechanisms: list[str] | None = None
        self.sasl_mechanisms_overrides: list[SaslMechanismOverride] | None = None
        self.http_authentication: list[str] | None = None
        self.endpoint_authn_method: str | None = None
        self.tls_provider: TLSProvider | None = None
        self.require_client_auth: bool = True
        self.auto_auth: bool | None = None

        # The rules to extract principal from mtls
        self.principal_mapping_rules = self.__DEFAULT_PRINCIPAL_MAPPING_RULES

    # sasl is required
    def sasl_enabled(self) -> bool:
        return (
            self.kafka_enable_authorization is None
            and self.enable_sasl
            and self.endpoint_authn_method is None
        ) or self.endpoint_authn_method == "sasl"

    # principal is extracted from mtls distinguished name
    def mtls_identity_enabled(self) -> bool:
        return self.endpoint_authn_method == "mtls_identity"


class LoggingConfig:
    # A dictionary that maps logger names to the Redpanda version in which they
    # were introduced. If a logger is not present in this dictionary, it is
    # assumed it was always supported/does not require special handling.
    LOGGER_GENESIS: dict[str, RedpandaVersionTriple] = {
        "datalake": (24, 3, 1),
        "cloud_topics_compaction": (26, 1, 1),
    }

    def __init__(self, default_level: str, logger_levels: dict[str, str] = {}) -> None:
        self.default_level = default_level
        self.logger_levels = logger_levels

    def enable_finject_logging(self) -> None:
        self.logger_levels["finject"] = "trace"

    def to_args(self, redpanda_version: RedpandaVersionTriple | None = None) -> str:
        """
        Generate redpanda CLI arguments for this logging config

        To support running tests with mixed versions of redpanda, we need to
        be able to generate the correct CLI arguments for the version of
        redpanda we are running against.

        If no version information is provided, we assume that we run against
        dev and all loggers are supported.

        If a logger is not present in the `LOGGER_GENESIS` dictionary, it
        is assumed it was always supported/does not require special handling.

        :return: string
        """
        args = f"--default-log-level {self.default_level}"
        if self.logger_levels:
            levels_arg = ":".join(
                [
                    f"{k}={v}"
                    for k, v in self.logger_levels.items()
                    if not redpanda_version
                    or redpanda_version >= self.LOGGER_GENESIS.get(k, (0, 0, 0))
                ]
            )
            args += f" --logger-log-level={levels_arg}"

        return args


class AuthConfig:
    def __init__(self) -> None:
        self.authn_method: str | None = None


class TlsConfig(AuthConfig):
    def __init__(self) -> None:
        super(TlsConfig, self).__init__()
        self.server_key: str | None = None
        self.server_crt: str | None = None
        self.truststore_file: str | None = None
        self.crl_file: str | None = None
        self.client_key: str | None = None
        self.client_crt: str | None = None
        self.require_client_auth: bool = True
        self.enable_broker_tls: bool = True

    def maybe_write_client_certs(
        self,
        node: ClusterNode,
        logger: Logger,
        tls_client_key_file: str,
        tls_client_crt_file: str,
    ) -> None:
        if self.client_key is not None:
            logger.info(f"Writing client tls key file: {tls_client_key_file}")
            logger.debug(open(self.client_key, "r").read())
            node.account.mkdirs(os.path.dirname(tls_client_key_file))
            node.account.copy_to(self.client_key, tls_client_key_file)

        if self.client_crt is not None:
            logger.info(f"Writing client tls crt file: {tls_client_crt_file}")
            logger.debug(open(self.client_crt, "r").read())
            node.account.mkdirs(os.path.dirname(tls_client_crt_file))
            node.account.copy_to(self.client_crt, tls_client_crt_file)


class PandaproxyConfig(TlsConfig):
    PP_TLS_CLIENT_KEY_FILE = "/etc/redpanda/pp_client.key"
    PP_TLS_CLIENT_CRT_FILE = "/etc/redpanda/pp_client.crt"

    def __init__(self) -> None:
        super(PandaproxyConfig, self).__init__()
        self.cache_keep_alive_ms: int | None = 300000
        self.cache_max_size: int | None = 10
        self.advertised_api_host: str | None = None


class SchemaRegistryConfig(TlsConfig):
    SR_TLS_CLIENT_KEY_FILE = "/etc/redpanda/sr_client.key"
    SR_TLS_CLIENT_CRT_FILE = "/etc/redpanda/sr_client.crt"

    mode_mutability = False

    def __init__(self) -> None:
        super(SchemaRegistryConfig, self).__init__()


class AuditLogConfig(TlsConfig):
    AUDIT_LOG_TLS_CLIENT_KEY_FILE = "/etc/redpanda/audit_log_client.key"
    AUDIT_LOG_TLS_CLIENT_CRT_FILE = "/etc/redpanda/audit_log_client.crt"

    def __init__(
        self,
        listener_port: int | None = None,
        listener_authn_method: str | None = None,
    ) -> None:
        super(AuditLogConfig, self).__init__()
        self.listener_port = listener_port
        self.listener_authn_method = listener_authn_method


class RpkNodeConfig:
    def __init__(self) -> None:
        self.ca_file: str | None = None


class RedpandaServiceConstants:
    SUPERUSER_CREDENTIALS: SaslCredentials = SaslCredentials(
        "admin", "admin1234567890", "SCRAM-SHA-256"
    )


class RedpandaServiceABC(ABC, RedpandaServiceConstants):
    """A base class for all Redpanda services. This lowest-common denominator
    class has both implementation and abstract methods, only for methods which
    can be implemented by all services. Any methods which the service should
    implement should be @abstractmethod in order to ensure they are implemented
    by base classes.

    If a method can be implemented by more than one service implementation, but
    not all of them, it does NOT belong here.
    """

    context: TestContext

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._usage_stats = UsageStats()
        self._max_workers: int | None = None

    @property
    @abstractmethod
    def logger(self) -> Logger:
        pass

    @property
    def usage_stats(self) -> UsageStats:
        """
        Returns usage statistics for Redpanda. The statistics contains
        information about Redpanda service resource usage. f.e. total amount of
        bytes read/written during the test.
        """
        return self._usage_stats

    @property
    def usage_stats_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self._usage_stats)

    @abstractmethod
    def all_up(self) -> bool:
        pass

    @abstractmethod
    def raise_on_bad_logs(
        self, allow_list: LogAllowList = (), test_start_time: float = 0
    ) -> None:
        pass

    @abstractmethod
    def all_nodes_abc(self) -> list[ClusterNode] | list[CloudBroker]:
        """Return a list of all nodes in the cluster, as ClusterNode or CloudBroker.
        This is mostly useful within the ABC itself in order to implement methods
        which pass though all nodes to implementation methods when the user does
        not specify a node."""
        pass

    @abstractmethod
    def kafka_client_security(self) -> KafkaClientSecurity:
        """Return a KafkaClientSecurity object suitable for connecting to the Kafka API
        on this broker."""
        pass

    def export_cluster_config(self) -> None:
        self.logger.debug("export_cluster_config not implemented for this service")

    def wait_until(
        self,
        fn: Callable[[], Any],
        timeout_sec: float,
        backoff_sec: float,
        err_msg: str | Callable[[], str] = "",
        retry_on_exc: bool = False,
    ) -> None:
        """
        Cluster-aware variant of wait_until, which will fail out
        early if a node dies.

        This is useful for long waits, which would otherwise not notice
        a test failure until the end of the timeout, even if redpanda
        already crashed.
        """

        t_initial = time.time()
        # How long to delay doing redpanda liveness checks, to make short waits more efficient
        grace_period = 15

        def wrapped():
            r = fn()
            if not r and time.time() > t_initial + grace_period:
                # Check the cluster is up before waiting + retrying
                assert (
                    self.all_up()
                    or getattr(self, "_tolerate_crashes", False)
                    or getattr(self, "tolerate_not_running", 0) > 0
                )
            return r

        wait_until(
            wrapped,
            timeout_sec=timeout_sec,
            backoff_sec=backoff_sec,
            err_msg=err_msg,
            retry_on_exc=retry_on_exc,
        )

    def wait_until_with_progress_check(
        self,
        check: Callable[[], Any],
        condition: Callable[[], Any],
        timeout_sec: float,
        progress_sec: float,
        backoff_sec: float,
        err_msg: str | None = None,
        logger: Logger | None = None,
    ) -> None:
        """
        Cluster-aware variant of wait_until_with_progress_check, which will
        fail out early if a node dies.

        This is useful for long waits, which would otherwise not notice
        a test failure until the end of the timeout, even if redpanda
        already crashed.
        """
        t_initial = time.time()
        # How long to delay doing redpanda liveness checks, to make short waits more efficient
        grace_period = 15

        def wrapped():
            r = condition()
            if not r and time.time() > t_initial + grace_period:
                # Check the cluster is up before waiting + retrying
                assert (
                    self.all_up()
                    or getattr(self, "_tolerate_crashes", False)
                    or getattr(self, "tolerate_not_running", 0) > 0
                )
            return r

        wait_until_with_progress_check(
            check,
            wrapped,
            timeout_sec,
            progress_sec,
            backoff_sec,
            err_msg=err_msg,
            logger=logger,
        )

    def for_nodes(self, nodes: Collection[U], cb: Callable[[U], T]) -> list[T]:
        if len(nodes) > 0:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_workers
            ) as executor:
                # The list() wrapper is to cause futures to be evaluated here+now
                # (including throwing any exceptions) and not just spawned in background.
                return list(executor.map(cb, nodes))
        else:
            return []

    def _extract_samples(
        self, metrics: list[Metric], sample_pattern: str, node: Any
    ) -> list[MetricSample]:
        """Extract metrics samples given a sample pattern. Embed the node in which it came from."""
        found_sample = None
        sample_values: list[MetricSample] = []

        for family in metrics:
            for sample in family.samples:
                if sample_pattern not in sample.name:
                    continue
                if not found_sample:
                    found_sample = (family.name, sample.name)
                if found_sample != (family.name, sample.name):
                    raise Exception(
                        f"More than one metric matched '{sample_pattern}'. Found {found_sample} and {(family.name, sample.name)}"
                    )
                sample_values.append(
                    MetricSample(sample.name, node, sample.value, sample.labels)
                )

        return sample_values

    @abstractmethod
    def metrics(
        self,
        node: ClusterNode | CloudBroker,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        name: str | None = None,
    ) -> list[Metric]:
        """Query and return all metrics from the given node's metrics endpoint.

        :name: If not None, only return metrics matching this exact name."""
        pass

    def metrics_sample(
        self,
        sample_pattern: str = "",
        nodes: list[ClusterNode] | list[CloudBroker] | None = None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        *,
        name: str = "",
    ) -> MetricSamples | None:
        """Query the given metrics endpoint for a single metric name. Returns
        all series for this metric (i.e., all label combinations, on every node).

        :param sample_pattern: Substring to match against metric names. Fuzzy
            matching is used; any metric whose name contains this string will
            be considered a match.
            Exactly one of ``name`` or ``sample_pattern`` must be provided.
        :param nodes: List of nodes to query. If ``None``, all nodes in the
            cluster are queried.
        :param metrics_endpoint: The metrics endpoint to query.
        :param name: If provided, performs exact matching for the metric name.
            If not provided, fuzzy matching is used via ``sample_pattern``.
            Exactly one of ``name`` or ``sample_pattern`` must be provided.

        :returns: MetricSamples for the matched metric, or ``None`` if no
            metric matches the pattern. If ``name`` is provided and no metric
            is found, an exception is raised.
        :rtype: MetricSamples | None

        This either uses fuzzy matching (if sample_pattern is used) or
        exact matching (if name is used) to find the metric.

        Fuzzy matching matches any metric whose name contains the given
        sample_pattern (which is a plain string, not a regex or glob).

        This interface matches the sample pattern against metric names, and
        requires that exactly one metric name match the query (though this name
        may be associated with multiple metric values with varying labels). All
        series for this name are returned, no aggregation is performed.

        None will be returned if no metric matches the pattern. An
        exception will be raised if two or more metric names match the pattern.

        For example, the query:

            redpanda.metrics_sample("under_replicated")

        will return an array containing MetricSample instances for each node and
        core/shard in the cluster. Each entry will correspond to the sum of
        the "vectorized_cluster_partition_under_replicated_replicas" summed across
        all shards.
        """

        # check that name/sample_pattern usage is correct
        if not name and not sample_pattern:
            raise ValueError(
                "Either 'name' or 'sample_pattern' must be provided (neither were)"
            )

        if name and sample_pattern:
            raise ValueError(
                "Either 'name' or 'sample_pattern' must be provided (both were)"
            )

        key = name if name else sample_pattern
        names, sample_patterns = ([name], []) if name else ([], [sample_pattern])

        # delegate to metrics_samples for implementation
        samples = self.metrics_samples(
            sample_patterns, nodes, metrics_endpoint, names=names
        )

        if len(samples) == 0:
            return None
        else:
            assert len(samples) == 1 and key in samples, f"{samples}"
            return samples[key]

    def metrics_samples(
        self,
        sample_patterns: Iterable[str] = (),
        nodes: list[ClusterNode] | list[CloudBroker] | None = None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        *,
        names: Iterable[str] = (),
    ) -> dict[str, MetricSamples]:
        """Implement this method to iterate over nodes to query multiple sample patterns.

        Query metrics for multiple sample names using fuzzy matching.
        Similar to metrics_sample(), but works with multiple patterns.
        """

        # check that name/sample_pattern usage is correct
        if not names and not sample_patterns:
            raise ValueError(
                "Either 'names' or 'sample_patterns' must be provided (neither were)"
            )

        if names and sample_patterns:
            raise ValueError(
                "Either 'names' or 'sample_patterns' must be provided (both were)"
            )

        patterns_or_name = sample_patterns if sample_patterns else names

        sample_values_per_pattern: dict[str, list[MetricSample]] = {
            pattern: [] for pattern in patterns_or_name
        }

        if nodes is None:
            nodes = self.all_nodes_abc()

        def fetch_node_samples(
            n: ClusterNode | CloudBroker,
        ) -> dict[str, list[MetricSample]]:
            node_samples: dict[str, list[MetricSample]] = {
                pattern: [] for pattern in patterns_or_name
            }
            # if exact names are provided, we use RP-side filtering support to query
            # only for those metrics, which is much faster especially when many
            # metrics are involved
            if names:
                for name in names:
                    metrics = self.metrics(
                        n,
                        metrics_endpoint=metrics_endpoint,
                        name=name,
                    )
                    node_samples[name] = self._extract_samples(metrics, name, n)
            else:
                metrics = self.metrics(n, metrics_endpoint)
                for pattern in sample_patterns:
                    node_samples[pattern] = self._extract_samples(metrics, pattern, n)
            return node_samples

        for node_samples in self.for_nodes(nodes, fetch_node_samples):
            for pattern, samples in node_samples.items():
                sample_values_per_pattern[pattern] += samples

        return {
            pattern: MetricSamples(values)
            for pattern, values in sample_values_per_pattern.items()
            if values
        }

    @staticmethod
    def _adjust_metric_name(name: str, endpoint: MetricsEndpoint) -> str:
        """Adjust the metric name to be used in the __name__ filter."""

        # do a pre-check of the expected prefix to catch user errors early
        prefix = {
            MetricsEndpoint.METRICS: "vectorized_",
            MetricsEndpoint.PUBLIC_METRICS: "redpanda_",
        }[endpoint]

        assert name.startswith(prefix), (
            f"All {endpoint} names must start with {prefix}: {name}"
        )
        # we need to strip the prefix, because seastar __name__ filtering
        # works on the metric name without the prefix
        return name.removeprefix(prefix)

    @staticmethod
    def _metric_basename(name: str) -> str:
        """The prom text client has an annoying behavior for counter metrics:
        changes either the family or sample name by appending or removing _total,
        depending on if it is present.

        That is, ONLY for TYPE: counter metrics if the scaped metric name is foo, then
        in the Metric object m, m.family = foo, m.samples[].name = foo_total. If
        the metric name was instead foo_total, the result is actually the same! So
        you cannot tell what the _true_ metric name was from the Metric object.

        This interferes with matching metrics by name, since the __name__= filter
        on seastar needs the true metric name. So our policy: always strip _total
        when preparing the the name for querying and then check equality on family.name,
        which also doesn't include total. This means both metrics variations above can
        be successfully queried with foo or foo_total.

        This is *further* complicated by the fact that we have non-counters, like gauges
        which also end in total, e.g., redpanda_application_uptime_seconds_total which
        is a gauge but ends in total. These don't get the above treatment: the family
        and sample name will be the true metric names. So when matching we also strip
        _total from the returned family name to handle this case (essentially we always
        compare the base (stripped) names on both sides, even when that isn't necessary
        because the metric is not a counter - but we don't know it's a counter until
        the query has been returned and the filter applied).

        That's not necessarily desirable, but it's the best we can do without changing
        the prometheus text parser (i.e., using our own).

        See: prometheus_client/parser.py.text_fd_to_metric_families.build_metric"""
        return name.removesuffix("_total")

    def metric_sum(
        self,
        metric_name: str,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        namespace: str | None = None,
        topic: str | None = None,
        nodes: list[ClusterNode] | list[CloudBroker] | None = None,
        expect_metric: bool = False,
    ) -> float:
        """Pings the 'metrics_endpoint' of each node and returns the summed values
        of the given metric, optionally filtering by namespace and topic.
        """

        if nodes is None:
            nodes = self.all_nodes_abc()

        basename = self._metric_basename(metric_name)

        @dataclass
        class Result:
            value: float
            metric_seen: bool
            matched_families: set[str]

        def sum_for_node(n: ClusterNode | CloudBroker) -> Result:
            value = 0.0
            metric_seen = False
            matched_families: set[str] = set()
            metrics = self.metrics(
                n, metrics_endpoint=metrics_endpoint, name=basename + "*"
            )
            for family in metrics:
                if self._metric_basename(family.name) != basename:
                    continue
                matched_families.add(family.name)
                for sample in family.samples:
                    labels = sample.labels
                    if namespace:
                        assert (
                            "redpanda_namespace" in labels or "namespace" in labels
                        ), f"Missing namespace label: {sample}"
                        if (
                            labels.get("redpanda_namespace", labels.get("namespace"))
                            != namespace
                        ):
                            continue
                    if topic:
                        assert "redpanda_topic" in labels or "topic" in labels, (
                            f"Missing topic label: {sample}"
                        )
                        if labels.get("redpanda_topic", labels.get("topic")) != topic:
                            continue
                    metric_seen = True
                    value += sample.value
            return Result(value, metric_seen, matched_families)

        value = 0.0
        metric_seen = False
        matched_families: set[str] = set()
        for r in self.for_nodes(nodes, sum_for_node):
            value += r.value
            metric_seen |= r.metric_seen
            matched_families |= r.matched_families

        # catch any weirdness, like if two metrics foo_total and foo both exist, which would
        # be ambiguous
        assert len(matched_families) <= 1, (
            f"More than one family matched: {matched_families}"
        )

        if expect_metric and not matched_families:
            assert metric_seen, f"Metric {metric_name} was not observed"

        return value


class KubeServiceMixin(ABC):
    @property
    @abstractmethod
    def kubectl(self) -> KubectlTool:
        pass

    def get_node_memory_mb(self) -> float:
        line = self.kubectl.exec("cat /proc/meminfo | grep MemTotal")
        memory_kb = int(line.strip().split()[1])
        return memory_kb / 1024

    def get_node_cpu_count(self) -> int:
        core_count_str = self.kubectl.exec(
            "cat /proc/cpuinfo | grep ^processor | wc -l"
        )
        return int(core_count_str.strip())

    def get_node_disk_free(self) -> int:
        if self.kubectl.exists(RedpandaService.PERSISTENT_ROOT):
            df_path = RedpandaService.PERSISTENT_ROOT
        else:
            # If dir doesn't exist yet, use the parent.
            df_path = os.path.dirname(RedpandaService.PERSISTENT_ROOT)
        df_out = self.kubectl.exec(f"df --output=avail {df_path}")
        avail_kb = int(df_out.strip().split("\n")[1].strip())
        return avail_kb * 1024


class CorruptedClusterError(Exception):
    """Throw to indicate a cluster is an unhealthy or otherwise unsuitable state to
    continue the remainder of the tests."""

    pass


class BrokerNotStartedError(Exception):
    """Thrown in cases asks about or tries to perform an operation on a
    broker that it is not started, as such an operation is bound to fail."""

    pass


class RedpandaServiceCloud(KubeServiceMixin, RedpandaServiceABC):
    """
    Service class for running tests against Redpanda Cloud.

    Use the `make_redpanda_service` factory function to instantiate. Set
    the GLOBAL_CLOUD_* values in the global json file to have the factory
    use `RedpandaServiceCloud`.
    """

    GLOBAL_CLOUD_CLUSTER_CONFIG = "cloud_cluster"

    def __init__(
        self,
        context: TestContext,
        *,
        config_profile_name: str,
        skip_if_no_redpanda_log: bool | None = False,
        min_brokers: int = 3,
        **kwargs: Any,
    ) -> None:
        """Initialize a RedpandaServiceCloud object.

        :param context: test context object
        :param num_brokers: ignored because Redpanda Cloud will launch the number of brokers necessary to satisfy the product needs
        :param superuser:  if None, then create SUPERUSER_CREDENTIALS with full acls
        :param tier_name:  the redpanda cloud tier name to create
        :param min_brokers: the minimum number of brokers the consumer of the cluster (a test, typically) requires
                            if the cloud cluster does not have this many brokers an exception is thrown
        """

        # we save the test context under both names since RedpandaService and Service
        # save them under these two names, respetively
        self.context = self._context = context

        super().__init__()

        self.config_profile_name = config_profile_name
        self._min_brokers = min_brokers
        self._superuser = RedpandaService.SUPERUSER_CREDENTIALS

        self._trim_logs = False

        # Prepare values from globals.json to serialize
        # later to dataclass
        self._cc_config = context.globals[self.GLOBAL_CLOUD_CLUSTER_CONFIG]

        self._provider_config: dict[str, str | None] = {}
        match context.globals.get("cloud_provider"):
            case "aws" | "gcp":
                self._provider_config.update({
                    'access_key':
                        context.globals.get(SISettings.GLOBAL_S3_ACCESS_KEY, None),
                    'secret_key':
                        context.globals.get(SISettings.GLOBAL_S3_SECRET_KEY, None),
                    'region':
                        context.globals.get(SISettings.GLOBAL_S3_REGION_KEY, None)
                })  # yapf: disable
            case "azure":
                self._provider_config.update({
                    'azure_client_id':
                        context.globals.get(SISettings.GLOBAL_AZURE_CLIENT_ID, None),
                    'azure_client_secret':
                        context.globals.get(SISettings.GLOBAL_AZURE_CLIENT_SECRET, None),
                    'azure_tenant_id':
                        context.globals.get(SISettings.GLOBAL_AZURE_TENANT_ID, None)
                })  # yapf: disable
            case _:
                pass

        # log cloud cluster id
        self.logger.debug(f"initial cluster_id: {self._cc_config['id']}")

        # Create cluster class
        self._cloud_cluster = CloudCluster(
            context, self.logger, self._cc_config, provider_config=self._provider_config
        )
        # Prepare kubectl
        self.__kubectl = None

        # Backward compatibility with RedpandaService
        # Fake out sasl_enabled callable
        self.sasl_enabled = lambda: True
        # Always true for Cloud Cluster
        self._dedicated_nodes = True
        self.logger.info(
            "ResourceSettings: setting dedicated_nodes=True because serving from redpanda cloud"
        )

        self._is_serverless_cluster = False
        if self._cc_config["type"] == "serverless":
            self._is_serverless_cluster = True
        cluster_id = self._cloud_cluster.create(
            superuser=self._superuser, is_serverless_cluster=self._is_serverless_cluster
        )
        remote_uri = f"redpanda@{cluster_id}-agent"
        if self._is_serverless_cluster:
            # if cluster is a serverless cluster it will be treated as a black box, there will be no kubectl or
            # checking of pods
            return

        self.__kubectl = KubectlTool(
            self,
            remote_uri=remote_uri,
            cluster_id=cluster_id,
            cluster_provider=self._cloud_cluster.config.provider,
            tp_proxy=self._cloud_cluster.config.teleport_auth_server,
            tp_token=self._cloud_cluster.config.teleport_bot_token,
        )

        self.rebuild_pods_classes()
        self._initial_node_count = len(self.pods)

        assert self._min_brokers <= self._initial_node_count, (
            f"Not enough brokers: test needs {self._min_brokers} but cluster has {self._initial_node_count}"
        )

    @property
    def logger(self) -> Logger:
        return self._context.logger

    @property
    def kubectl(self) -> KubectlTool:
        assert self.__kubectl, "kubectl accessed before cluster was started?"
        return self.__kubectl

    def all_nodes_abc(self) -> list[CloudBroker]:
        return self.pods

    def who_am_i(self) -> str:
        return not_none(self._cloud_cluster.cluster_id)

    def security_config(self) -> dict[str, Any]:
        return dict(
            security_protocol="SASL_SSL",
            sasl_mechanism=self._superuser.algorithm,
            sasl_plain_username=self._superuser.username,
            sasl_plain_password=self._superuser.password,
            enable_tls=True,
        )

    def kafka_client_security(self) -> KafkaClientSecurity:
        return KafkaClientSecurity(self._superuser, True)

    def rebuild_pods_classes(self) -> None:
        """Querry pods and create Classes fresh"""

        def make_cloud_broker(p: dict[str, Any]) -> CloudBroker:
            return CloudBroker(p, self.kubectl, self.logger)

        self.pods = self.for_nodes(self.get_redpanda_pods(), make_cloud_broker)

    def start(self) -> None:
        """Does nothing, do not call."""
        # everything here was moved into __init__
        pass

    def format_pod_status(self, pods: list[dict[str, Any]]) -> str:
        statuses_list = [(p["metadata"]["name"], p["status"]["phase"]) for p in pods]
        if len(statuses_list) < 1:
            return "none"
        else:
            return ", ".join([f"{n}: {s}" for n, s in statuses_list])

    def get_redpanda_pods_filtered(self, status: str) -> list[dict[str, Any]]:
        return [
            p
            for p in self.get_redpanda_pods()
            if p["status"]["phase"].lower() == status
        ]

    def get_redpanda_pods_presorted(
        self,
    ) -> Tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Method gets all pods and separates them into active and inactive

        return: active_pods, inactive_pods, unknown lists
        """
        # Pod lifecycle is: Pending, Running, Succeeded, Failed, Unknown
        active_phases = ["running"]
        inactive_phases = ["pending", "succeeded", "failed"]

        all_pods = self.get_redpanda_pods()
        # Sort pods into bins
        active_rp_pods: list[dict[str, Any]] = []
        inactive_rp_pods: list[dict[str, Any]] = []
        unknown_rp_pods: list[dict[str, Any]] = []
        for pod in all_pods:
            _status = pod["status"]["phase"].lower()
            if _status in active_phases:
                active_rp_pods.append(pod)
            elif _status in inactive_phases:
                inactive_rp_pods.append(pod)
            else:
                # Phase unknown and others
                unknown_rp_pods.append(pod)

        # Log pod names and statuses
        # Example: Current Redpanda pods: rp-cnplksdb0t6f2b421c20-0: Running, rp-cnplksdb0t6f2b421c20-1: Running, rp-cnplksdb0t6f2b421c20-2: Running'
        self.logger.debug(
            f"Active RP cluster pods: {self.format_pod_status(active_rp_pods)}"
        )
        self.logger.debug(
            f"Inactive RP cluster pods: {self.format_pod_status(inactive_rp_pods)}"
        )
        self.logger.debug(
            f"Other RP cluster pods: {self.format_pod_status(unknown_rp_pods)}"
        )

        return active_rp_pods, inactive_rp_pods, unknown_rp_pods

    def get_redpanda_statefulset(self) -> dict[str, Any]:
        """Get the statefulset for redpanda brokers"""
        assert self.is_operator_v2_cluster(), "statefulset called on op v1 cluster"
        return json.loads(
            self.kubectl.cmd("get statefulset -n redpanda redpanda-broker -o json")
        )

    def get_redpanda_pods(self) -> list[dict[str, Any]]:
        """Get the current list of redpanda pods as k8s API objects."""
        pods = json.loads(self.kubectl.cmd("get pods -n redpanda -o json"))

        return [p for p in pods["items"] if is_redpanda_pod(p, self.cluster_id)]

    @property
    def cluster_id(self) -> str:
        cid = self._cloud_cluster.cluster_id
        assert cid, cid
        return cid

    def get_node_by_id(self, id: int) -> CloudBroker | None:
        for p in self.pods:
            if p.slot_id == id:
                return p
        return None

    @cached_property
    def config_profile(self) -> dict[str, Any]:
        profiles: dict[str, Any] = self.get_install_pack()["config_profiles"]
        if self.config_profile_name not in profiles:
            # throw user friendly error
            raise RuntimeError(
                f"'{self.config_profile_name}' not found among config profiles: {profiles.keys()}"
            )
        return profiles[self.config_profile_name]

    def restart_pod(self, pod_name: str, timeout: int = 180) -> None:
        """Restart a pod by name

        Using kubectl delete to gracefully stop the pod,
        the pod will automatically be restarted by the cluster.
        Block until pod container is ready.
        The list of pod names can be found in self.pods property.

        :param pod_name: string of pod name, e.g. 'rp-clo88krkqkrfamptsst0-5'
        :param timeout: seconds to wait until pod is ready after kubectl delete
        """
        self.logger.info(f"restarting pod {pod_name}")

        def pod_container_ready(pod_name: str):
            # kubectl get pod rp-clo88krkqkrfamptsst0-0 -n=redpanda -o=jsonpath='{.status.containerStatuses[0].ready}'
            return self.kubectl.cmd(
                [
                    "get",
                    "pod",
                    pod_name,
                    "-n=redpanda",
                    "-o=jsonpath='{.status.containerStatuses[0].ready}'",
                ]
            )

        delete_cmd = ["delete", "pod", pod_name, "-n=redpanda"]
        self.logger.info(f"deleting pod {pod_name} so the cluster can recreate it")
        # kubectl delete pod rp-clo88krkqkrfamptsst0-0 -n=redpanda'
        self.kubectl.cmd(delete_cmd)

        self.logger.info(
            f"waiting for pod {pod_name} container status ready with timeout {timeout}"
        )
        wait_until(
            lambda: pod_container_ready(pod_name) == "true",
            timeout_sec=timeout,
            backoff_sec=1,
            err_msg=f"pod {pod_name} container status not ready",
        )
        self.logger.info(f"pod {pod_name} container status ready")

        # Call to rebuild metadata for all cloud brokers
        self.rebuild_pods_classes()

    def is_operator_v2_cluster(self) -> bool:
        # Below will fail if there is no 'redpanda' resource type. This is the
        # case on operator v1 but exists on operator v2.
        try:
            self.kubectl.cmd(["get", "redpanda", "-n=redpanda", "--ignore-not-found"])
        except subprocess.CalledProcessError as e:
            if 'have a resource type "redpanda"' in e.stderr:
                return False

        return True

    def rolling_restart_pods(self, pod_timeout: int = 180) -> None:
        """Restart all pods in the cluster one at a time.

        Restart a pod in the cluster and does not restart another
        until the previous one has finished.
        Block until cluster is

        :param pod_timeout: seconds to wait for each pod to be ready after restart
        """

        pod_names = [p.name for p in self.pods]
        self.logger.info(f"rolling restart on pods: {pod_names}")

        for pod_name in pod_names:
            self.restart_pod(pod_name, pod_timeout)
            cluster_name = f"rp-{self._cloud_cluster.cluster_id}"
            expected_replicas = self.cluster_desired_replicas(cluster_name)
            # Check cluster readiness after pod restart
            self.check_cluster_readiness(cluster_name, expected_replicas, pod_timeout)

    def concurrent_restart_pods(self, pod_timeout: int) -> None:
        """
        Restart all pods in the cluster concurrently and wait
        for the entire cluster to be ready.
        """

        cluster_name = f"rp-{self._cloud_cluster.cluster_id}"
        pod_names = [p.name for p in self.pods]
        self.logger.info(f"Starting concurrent restart on pods: {pod_names}")

        threads: list[threading.Thread] = []
        for pod_name in pod_names:
            thread = threading.Thread(target=self.restart_pod, args=(pod_name,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()  # Wait for all threads to complete

        self.logger.info("All pods have been restarted (deleted) concurrently.")

        # kubectl get cluster rp-clo88krkqkrfamptsst0 -n=redpanda -o=jsonpath='{.status.replicas}'
        expected_replicas = int(
            self.kubectl.cmd(
                [
                    "get",
                    "cluster",
                    cluster_name,
                    "-n=redpanda",
                    "-o=jsonpath='{.status.replicas}'",
                ]
            )
        )

        # Check cluster readiness after restart of all pods
        self.check_cluster_readiness(cluster_name, expected_replicas, pod_timeout)

    def check_cluster_readiness(
        self, cluster_name: str, expected_replicas: int, pod_timeout: int
    ):
        """Checks if the cluster has the expected number of ready replicas."""
        self.logger.info(
            f"Waiting for cluster {cluster_name} to have readyReplicas {expected_replicas} with timeout {pod_timeout}"
        )

        wait_until(
            lambda: self.cluster_ready_replicas(cluster_name) == expected_replicas,
            timeout_sec=pod_timeout,
            backoff_sec=1,
            err_msg=f"Cluster {cluster_name} failed to arrive at readyReplicas {expected_replicas}",
        )

        self.logger.info(
            f"Cluster {cluster_name} arrived at readyReplicas {expected_replicas}"
        )

    def cluster_desired_replicas(self, cluster_name: str = "") -> int:
        """Return cluster desired replica count."""
        if self.is_operator_v2_cluster():
            rp_statefulset = self.get_redpanda_statefulset()
            return int(rp_statefulset["status"]["replicas"])
        expected_replicas = int(
            self.kubectl.cmd(
                [
                    "get",
                    "cluster",
                    cluster_name,
                    "-n=redpanda",
                    "-o=jsonpath='{.status.replicas}'",
                ]
            )
        )
        return expected_replicas

    def cluster_ready_replicas(self, cluster_name: str = "") -> int:
        """Retrieves the number of ready replicas for the given cluster."""
        if self.is_operator_v2_cluster():
            rp_statefulset = self.get_redpanda_statefulset()
            return int(rp_statefulset["status"]["readyReplicas"])
        ret = self.kubectl.cmd(
            [
                "get",
                "cluster",
                cluster_name,
                "-n=redpanda",
                "-o=jsonpath='{.status.readyReplicas}'",
            ]
        )
        return int(0 if not ret else ret)

    def verify_basic_produce_consume(self, producer: Any, consumer: Any) -> None:
        self.logger.info("Checking basic producer functions")
        current_sent = producer.produce_status.sent
        produce_count = 100
        consume_count = 100

        def producer_complete():
            number_left = (current_sent + produce_count) - producer.produce_status.sent
            self.logger.info(f"{number_left} messages still need to be sent.")
            return number_left <= 0

        wait_until(producer_complete, timeout_sec=120, backoff_sec=1)

        self.logger.info("Checking basic consumer functions")
        current_sent = producer.produce_status.sent

        consumer.start()
        wait_until(
            lambda: consumer.message_count >= consume_count,
            timeout_sec=120,
            backoff_sec=1,
            err_msg=f"Could not consume {consume_count} msgs in 120 seconds",
        )

        consumer.stop()
        consumer.free()
        self.logger.info(
            f"Successfully verified basic produce/consume with {produce_count}/"
            f"{consume_count} messages."
        )

    def stop(self, **kwargs: Any) -> None:
        if self._cloud_cluster.config.delete_cluster:
            self._cloud_cluster.delete()
        else:
            self.logger.info(
                f"skipping delete of cluster {self._cloud_cluster.cluster_id}"
            )

    def brokers(self) -> str:
        if self._is_serverless_cluster:
            return self._cloud_cluster.get_serverless_broker_address()
        return self._cloud_cluster.get_broker_address()

    def brokers_list(self) -> list[str]:
        return self.brokers().split(",")

    def install_pack_version(self) -> str:
        if self._is_serverless_cluster:
            # if serverless cluster we do not have an install pack version
            return "unknown_version"
        return self._cloud_cluster.get_install_pack_version()

    def sockets_clear(self, node: RemoteClusterNode) -> bool:
        return True

    def all_up(self) -> bool:
        return self.cluster_healthy()

    def metrics(
        self,
        node: Any,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.PUBLIC_METRICS,
        name: str | None = None,
    ):
        """Parse the prometheus text format metric from a given pod."""
        if metrics_endpoint == MetricsEndpoint.PUBLIC_METRICS:
            text = self._cloud_cluster.get_public_metrics()
        else:
            # operator V2 clusters use HTTPS for all the things
            p = "-k https" if self.is_operator_v2_cluster() else "http"
            if name:
                name = self._adjust_metric_name(name, metrics_endpoint)
                query_string = f"?__name__={name}"
            else:
                query_string = ""
            text = self.kubectl.exec(
                f"curl -f -s -S {p}://localhost:9644/metrics{query_string}", node.name
            )
        return list(text_string_to_metric_families(text))

    @staticmethod
    def get_cloud_globals(globals: dict[str, Any]) -> dict[str, Any]:
        # Load needed config values from cloud section
        # of globals prior to actual cluster creation
        return globals.get(RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG, {})

    def get_tier(self):
        """Get product information.

        Returns dict with info of product, including advertised limits.
        Returns none if product info for the tier is not found.
        """
        return self._cloud_cluster.get_tier()

    def get_scaled_tier(self) -> ThroughputTierInfo | None:
        """Get tier limits scaled for the actual number of nodes.

        Returns tier info with limits adjusted proportionally based on
        the ratio of actual nodes to the default tier node count.
        Returns None if base tier info is not found.
        """
        tier = self.get_tier()
        if tier is None:
            return None

        # Global limits
        global_partition_limit = 112500

        default_nodes = int(self.config_profile["nodes_count"])
        actual_nodes = len(self.pods)

        if actual_nodes == default_nodes:
            return tier

        scale_factor = actual_nodes / default_nodes

        return ThroughputTierInfo(
            max_ingress=int(tier.max_ingress * scale_factor),
            max_egress=int(tier.max_egress * scale_factor),
            max_connections_count=int(tier.max_connections_count * scale_factor),
            max_partition_count=min(
                int(tier.max_partition_count * scale_factor), global_partition_limit
            ),
        )

    def get_install_pack(self):
        install_pack_client = InstallPackClient(
            self._cloud_cluster.config.install_pack_url_template,
            self._cloud_cluster.config.install_pack_auth_type,
            self._cloud_cluster.config.install_pack_auth,
        )
        install_pack_version = self._cloud_cluster.config.install_pack_ver

        # Load install pack and check profile
        return install_pack_client.getInstallPack(install_pack_version)

    def cloud_agent_ssh(self, remote_cmd: list[str]):
        """Run the given command on the redpanda agent node of the cluster.

        :param remote_cmd: The command to run on the agent node.
        """
        return self.kubectl._ssh_cmd(remote_cmd)

    def scale_cluster(self, nodes_count: int) -> Any:
        """Scale out/in cluster to specified number of nodes."""
        return self._cloud_cluster.scale_cluster(nodes_count)

    def set_cluster_config_overrides(
        self, cluster_id: str, config_values: list[dict[str, str]]
    ) -> Any:
        """
        Set configuration overrides for a specific
        Redpanda cloud cluster using Admin API
        """
        return self._cloud_cluster.set_cluster_config_overrides(
            cluster_id, config_values
        )

    def clean_cluster(self):
        """Cleans state from a running cluster to make it seem like it was newly provisioned.

        Call this function at the end of a test case so the next test case has a clean cluster.
        For safety, this function must not be called concurrently.
        """
        assert self.context.session_context.max_parallel < 2, (
            "unsafe to clean cluster if ducktape run with parallelism"
        )

        # assuming topics beginning with '_' are system topics that should not be deleted
        rpk = RpkTool(self)
        topics = rpk.list_topics()
        deletable = [x for x in topics if not x.startswith("_")]
        self.logger.debug(f"found topics to delete ({len(deletable)}): {deletable}")
        for topic in deletable:
            rpk.delete_topic(topic)

    def assert_cluster_is_reusable(self) -> None:
        """Tries to assess wether the cluster is reusable for subsequent tests. This means that the
        cluster is healthy and has its original shape and configuration (currently we don't check
        the configuration aspect).

        Throws an CorruptedClusterError if the cluster is not re-usable, otherwise does nothing."""

        uh_reason = self.cluster_unhealthy_reason()
        if uh_reason is not None:
            raise CorruptedClusterError(uh_reason)

        self._cloud_cluster._ensure_cluster_health()

        expected_nodes = self._initial_node_count
        active, _, _ = self.get_redpanda_pods_presorted()
        failed = self.get_redpanda_pods_filtered("failed")
        active_count = len(active)
        failed_count = len(failed)
        assert expected_nodes == active_count, (
            f"Expected {expected_nodes} nodes (initial count) but found {active_count} active pods"
        )
        assert failed_count == 0, f"Expected no failed pods, found {failed_count}"

        brokers = self._cloud_cluster.get_brokers()
        broker_count = len(brokers)
        assert expected_nodes == broker_count, (
            f"Expected {expected_nodes} nodes (initial count) but there "
            f"were only {broker_count} brokers: {brokers}"
        )

    def raise_on_crash(self, log_allow_list: LogAllowList = ()) -> None:
        """Function checks if active RP pods has restart counter changed since last check"""

        # Can't remove log_allow_list as it is present in the metadataaddeer call
        # Checking logs in case of crash is useless for pods as they are auto-restarted anyway
        def _get_stored_pod(uuid: str):
            """Shortcut to getting proper stored Broker class"""
            for pod in self.pods:
                if uuid == pod.uuid:
                    return pod
            return None

        def _get_container_id(p: dict[str, Any]):
            # Shortcut to getting containerID
            return p["containerStatuses"][0]["containerID"]

        def _get_restart_count(p: dict[str, Any]):
            # Shortcut to getting restart counter
            return p["containerStatuses"][0]["restartCount"]

        if self._is_serverless_cluster:
            # if serverless cluster test treats it like a black box, no checking of pods
            return

        # Not checking active count vs expected nodes
        active, _, _ = self.get_redpanda_pods_presorted()
        for pod in active:
            _name = pod["metadata"]["name"]

            # Check if stored pod and loaded one is the same
            _stored_pod = _get_stored_pod(pod["metadata"]["uid"])
            if _stored_pod is None:
                raise NodeCrash([(_name, "Pod not found among prior stored ones")])

            # Check if container inside pod stayed the same
            container_id = _get_container_id(pod["status"])
            if _get_container_id(_stored_pod._status) != container_id:
                raise NodeCrash(
                    [(_name, "Pod container mismatch with prior stored one")]
                )

            # Check that restart count is the same
            restart_count = _get_restart_count(pod["status"])
            if _get_restart_count(_stored_pod._status) != restart_count:
                raise NodeCrash(
                    [(_name, "Pod has been restarted due to possible crash")]
                )

        # Worth to note that rebuilding stored broker classes
        # can be skipped in this case since nothing changed now
        # and should not be changed. But if some more sophisticated
        # checks will be introduced, it might be needed to call
        # self.rebuild_pods_classes() at the and
        return

    def cluster_unhealthy_reason(self) -> str | None:
        """Check if cluster is healthy, using rpk cluster health. Note that this will return
        true if all currently configured brokers are up and healthy, including in the case brokers
        have been added or removed from the cluster, even though the cluster is not in its original
        form in that case."""

        # kubectl exec rp-clo88krkqkrfamptsst0-0 -n=redpanda -c=redpanda -- rpk cluster health
        ret = self.kubectl.exec("rpk cluster health")

        # bash$ rpk cluster health
        # CLUSTER HEALTH OVERVIEW
        # =======================
        # Healthy:                          true
        # Unhealthy reasons:                []
        # Controller ID:                    0
        # All nodes:                        [0 1 2]
        # Nodes down:                       []
        # Leaderless partitions (0):        []
        # Under-replicated partitions (0):  []

        lines = ret.splitlines()
        self.logger.debug(f"rpk cluster health lines: {lines}")
        unhealthy_reasons = "no line found"
        for line in lines:
            part = line.partition(":")
            heading = part[0].strip()
            if heading == "Healthy":
                if part[2].strip() == "true":
                    return None
            elif heading == "Unhealthy reasons":
                unhealthy_reasons = part[2].strip()

        return unhealthy_reasons

    def cluster_healthy(self) -> bool:
        return self.cluster_unhealthy_reason is not None

    def raise_on_bad_logs(
        self, allow_list: LogAllowList = (), test_start_time: float = 0
    ) -> None:
        """
        Raise a BadLogLines exception if any nodes' logs contain errors
        not permitted by `allow_list`

        :param allow_list: list of compiled regexes, or None for default
        :return: None
        """
        if self._is_serverless_cluster:
            # if serverless cluster test treats it as a black box, no checking of logs
            return

        allow_list = prepare_allow_list(allow_list)

        lsearcher = LogSearchCloud(
            self._context,
            allow_list,
            self.logger,
            self.kubectl,
            test_start_time=test_start_time,
        )
        lsearcher.search_logs([(None, pod) for pod in self.pods])

    def copy_cloud_logs(self, test_start_time: float) -> dict[str, Any]:
        """Method makes sure that agent and cloud logs is copied after the test"""

        def create_dest_path(service_name: str):
            # Create directory into which service logs will be copied
            dest = os.path.join(
                TestContext.results_dir(self._context, self._context.test_index),
                service_name,
            )
            if not os.path.isdir(dest):
                mkdir_p(dest)

            return dest

        def copy_from_agent(since: str):
            service_name = f"{self._cloud_cluster.cluster_id}-agent"
            # Example path:
            # '/home/ubuntu/redpanda/tests/results/2024-04-11--019/SelfRedpandaCloudTest/test_healthy/2/coc12bfs0etj2dg9a5ig-agent'
            dest = create_dest_path(service_name)

            logfile = os.path.join(dest, "agent.log")
            # Query journalctl to copy logs from
            with open(logfile, "wb") as lfile:
                for line in self.kubectl._ssh_cmd(
                    f"journalctl -u redpanda-agent -S '{since}'".split(), capture=True
                ):
                    lfile.writelines([line])
            return

        def copy_from_pod(params: dict[str, Any]):
            """Function copies logs from agent and all RP pods"""
            pod = params["pod"]
            test_start_time = params["s_time"]
            dest = create_dest_path(pod.name)
            try:
                remote_path = os.path.join("/tmp", "pod_log_extract.sh")
                logfile = os.path.join(dest, f"{pod.name}.log")
                with open(logfile, "wb") as lfile:
                    for line in pod.nodeshell(
                        f"bash {remote_path} '{pod.name}' '{test_start_time}'".split(),
                        capture=True,
                    ):
                        lfile.writelines([line])
            except Exception as e:
                self.logger.warning(f"Error getting logs for {pod.name}: {e}")
            return pod.name

        if self._is_serverless_cluster:
            # if serverless cluster test treats it as a black box, no checking of pods
            return {}

        # Prepare time for different occasions
        t_start_time = time.gmtime(test_start_time)

        # Do not include seconds on purpose to improve chances
        time_format = "%Y-%m-%d %H:%M"
        f_start_time = time.strftime(time_format, t_start_time)

        # Collect-agent-logs
        self._context.logger.debug("Copying cloud agent logs...")
        sw = Stopwatch()
        sw.start()
        copy_from_agent(f_start_time)
        sw.split()
        self.logger.info(sw.elapsedf("# Done log copy from agent"))

        # Collect pod logs
        # Use CloudBrokers as a source of metadata and the rest
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        params: list[dict[str, Any]] = []
        for pod in self.pods:
            params.append({"pod": pod, "s_time": f_start_time})
        sw.start()
        for name in pool.map(copy_from_pod, params):
            # Calculate time for this node
            self.logger.info(sw.elapsedf(f"# Done log copy for {name} (interim)"))
        return {}


class RedpandaService(Service, RedpandaServiceABC):
    PERSISTENT_ROOT = "/var/lib/redpanda"
    TRIM_LOGS_KEY = "trim_logs"
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    CRASH_REPORTS = os.path.join(DATA_DIR, "crash_reports")
    NODE_CONFIG_FILE = "/etc/redpanda/redpanda.yaml"
    RPK_CONFIG_FILE = "/root/.config/rpk/rpk.yaml"
    CLUSTER_BOOTSTRAP_CONFIG_FILE = "/etc/redpanda/.bootstrap.yaml"
    TLS_SERVER_KEY_FILE = "/etc/redpanda/server.key"
    TLS_SERVER_CRT_FILE = "/etc/redpanda/server.crt"
    TLS_SERVER_P12_FILE = "/etc/redpanda/server.p12"
    TLS_CA_CRT_FILE = "/etc/redpanda/ca.crt"
    TLS_CA_CRL_FILE = "/etc/redpanda/ca.crl"
    SYSTEM_TLS_CA_CRT_FILE = "/usr/local/share/ca-certificates/ca.crt"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.log")
    BACKTRACE_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda_backtrace.log")
    COVERAGE_PROFRAW_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.profraw")
    TEMP_OSSL_CONFIG_FILE = "/etc/openssl.cnf"
    DEFAULT_NODE_READY_TIMEOUT_SEC = 40
    NODE_READY_TIMEOUT_MIN_SEC_KEY = "node_ready_timeout_min_sec"
    DEFAULT_CLOUD_STORAGE_SCRUB_TIMEOUT_SEC = 60
    DEDICATED_NODE_KEY = "dedicated_nodes"
    RAISE_ON_ERRORS_KEY = "raise_on_error"
    LOG_LEVEL_KEY = "redpanda_log_level"
    DEFAULT_LOG_LEVEL = "info"
    COV_KEY = "enable_cov"
    DEFAULT_COV_OPT = "OFF"

    ENTERPRISE_LICENSE_NAG = "A Redpanda Enterprise Edition license is required"

    # Where we put a compressed binary if saving it after failure
    EXECUTABLE_SAVE_PATH = "/tmp/redpanda.gz"

    FAILURE_INJECTION_CONFIG_PATH = "/etc/redpanda/failure_injection_config.json"

    OPENSSL_CONFIG_FILE_BASE = "openssl/openssl.cnf"
    OPENSSL_MODULES_PATH_BASE = "lib/ossl-modules/"

    # When configuring multiple listeners for testing, a secondary port to use
    # instead of the default.
    KAFKA_ALTERNATE_PORT = 9093
    KAFKA_KERBEROS_PORT = 9094
    ADMIN_ALTERNATE_PORT = 9647

    GLOBAL_USE_STRESS_FIBER = "enable_stress_fiber"
    GLOBAL_NUM_STRESS_FIBERS = "num_stress_fibers"
    GLOBAL_STRESS_FIBER_MIN_MS = "stress_fiber_min_ms"
    GLOBAL_STRESS_FIBER_MAX_MS = "stress_fiber_max_ms"
    DEFAULT_USE_STRESS_FIBER = "OFF"
    DEFAULT_NUM_STRESS_FIBERS = 1
    DEFAULT_STRESS_FIBER_MIN_MS = 100
    DEFAULT_STRESS_FIBER_MAX_MS = 200

    CLUSTER_CONFIG_DEFAULTS: dict[str, Any] = {
        "join_retry_timeout_ms": 200,
        "default_topic_partitions": 4,
        "enable_metrics_reporter": False,
        "superusers": [RedpandaServiceConstants.SUPERUSER_CREDENTIALS[0]],
        # Disable segment size jitter to make tests more deterministic if they rely on
        # inspecting storage internals (e.g. number of segments after writing a certain
        # amount of data).
        "log_segment_size_jitter_percent": 0,
        # This is high enough not to interfere with the logic in any tests, while also
        # providing some background coverage of the connection limit code (i.e. that it
        # doesn't crash, it doesn't limit when it shouldn't)
        "kafka_connections_max": 2048,
        "kafka_connections_max_per_ip": 1024,
        "kafka_connections_max_overrides": ["1.2.3.4:5"],
        # configure shutdown watchdog timeout to 20 seconds to give it a chance
        # to fire before Redpanda node that doesn't stopped is killed
        "partition_manager_shutdown_watchdog_timeout": 20000,
    }

    logs = {
        "redpanda_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True,
        },
        "code_coverage_profraw_file": {
            "path": COVERAGE_PROFRAW_CAPTURE,
            "collect_default": True,
        },
        "executable": {"path": EXECUTABLE_SAVE_PATH, "collect_default": False},
        "backtraces": {"path": BACKTRACE_CAPTURE, "collect_default": True},
        "crash_reports": {"path": CRASH_REPORTS, "collect_default": True},
    }

    # Thread name of shards to be used with redpanda_tid()
    SHARD_0_THREAD_NAME = "redpanda"
    SHARD_1_THREAD_NAME = "reactor-1"

    nodes: list[ClusterNode]

    def __init__(
        self,
        context: TestContext,
        num_brokers: int,
        *,
        cluster_spec: Any = None,
        extra_rp_conf: dict[str, Any] | None = None,
        extra_node_conf: dict[str, Any] | None = None,
        resource_settings: ResourceSettings | None = None,
        si_settings: SISettings | None = None,
        log_level: str | None = None,
        log_config: LoggingConfig | None = None,
        environment: dict[str, str] | None = None,
        security: SecurityConfig = SecurityConfig(),
        node_ready_timeout_s: int | None = None,
        superuser: SaslCredentials | None = None,
        skip_if_no_redpanda_log: bool = False,
        pandaproxy_config: PandaproxyConfig | None = None,
        schema_registry_config: SchemaRegistryConfig | None = None,
        audit_log_config: AuditLogConfig | None = None,
        disable_cloud_storage_diagnostics: bool = False,
        cloud_storage_scrub_timeout_s: int | None = None,
        rpk_node_config: RpkNodeConfig | None = None,
    ) -> None:
        super().__init__(context, num_nodes=num_brokers, cluster_spec=cluster_spec)

        # def __init__(
        #     self,
        #     context: TestContext,
        #     num_brokers: int,
        #     *,
        #     cluster_spec=None,
        #     extra_rp_conf=None,
        #     resource_settings: Optional[ResourceSettings] = None,
        #     si_settings: Optional[SISettings] = None,
        #     superuser: Optional[SaslCredentials] = None,
        #     skip_if_no_redpanda_log: Optional[bool] = False,
        #     disable_cloud_storage_diagnostics=True,
        # ):

        # BEGIN

        self._context = context
        self._extra_rp_conf = extra_rp_conf or dict()

        if si_settings is not None:
            self.set_si_settings(si_settings)
        else:
            self._si_settings = None

        if superuser is None:
            superuser = self.SUPERUSER_CREDENTIALS
            self._skip_create_superuser = False
        else:
            # When we are passed explicit superuser credentials, presume that the caller
            # is taking care of user creation themselves (e.g. when testing credential bootstrap)
            self._skip_create_superuser = True

        self._superuser = superuser

        self._admin = Admin(
            self, auth=(self._superuser.username, self._superuser.password)
        )

        if resource_settings is None:
            resource_settings = ResourceSettings()
        self._resource_settings = resource_settings

        # Disable saving cloud storage diagnostics. This may be useful for
        # tests that generate millions of objecst, as collecting diagnostics
        # may take a significant amount of time.
        self._disable_cloud_storage_diagnostics = disable_cloud_storage_diagnostics

        self._trim_logs = self._context.globals.get(self.TRIM_LOGS_KEY, True)

        self._node_id_by_idx: dict[int, int] = {}
        self._security_config: dict[str, str | int] = {}

        self._skip_if_no_redpanda_log = skip_if_no_redpanda_log

        self._dedicated_nodes: bool = self._context.globals.get(
            self.DEDICATED_NODE_KEY, False
        )

        self.logger.info(f"ResourceSettings: dedicated_nodes={self._dedicated_nodes}")

        # END
        self._security = security
        self._installer: RedpandaInstaller = RedpandaInstaller(self)
        self._pandaproxy_config = pandaproxy_config
        self._schema_registry_config = schema_registry_config
        self._audit_log_config = audit_log_config
        self._failure_injection_enabled = False
        # Tolerate redpanda nodes not running during health checks. This is
        # useful when running i.e. with node_operations.FailureInjectorBackgroundThread
        # which can kill redpanda nodes.
        # This is a number to allow multiple callers to set it.
        self.tolerate_not_running = 0
        self._tolerate_crashes = False
        self._rpk_node_config = rpk_node_config

        if node_ready_timeout_s is None:
            node_ready_timeout_s = RedpandaService.DEFAULT_NODE_READY_TIMEOUT_SEC
        # apply min timeout rule. some tests may override this with larger
        # timeouts, so take the maximum.
        node_ready_timeout_min_s = self._context.globals.get(
            self.NODE_READY_TIMEOUT_MIN_SEC_KEY, node_ready_timeout_s
        )
        node_ready_timeout_s = max(node_ready_timeout_s, node_ready_timeout_min_s)
        self.node_ready_timeout_s = node_ready_timeout_s

        if cloud_storage_scrub_timeout_s is None:
            cloud_storage_scrub_timeout_s = (
                RedpandaService.DEFAULT_CLOUD_STORAGE_SCRUB_TIMEOUT_SEC
            )
        self.cloud_storage_scrub_timeout_s = cloud_storage_scrub_timeout_s

        self._extra_node_conf: dict[ClusterNode, dict[str, Any]] = {}
        for node in self.nodes:
            self._extra_node_conf[node] = extra_node_conf or dict()

        if log_config is not None:
            self._log_config = log_config
        else:
            if log_level is None:
                self._log_level = self._context.globals.get(
                    self.LOG_LEVEL_KEY, self.DEFAULT_LOG_LEVEL
                )
            else:
                self._log_level = log_level
            self._log_config = LoggingConfig(
                self._log_level,
                {
                    "exception": "info",
                    "io": "debug",
                    "seastar_memory": "debug",
                    "dns_resolver": "info",
                },
            )

        self._started: Set[ClusterNode] = set()

        self._raise_on_errors = self._context.globals.get(
            self.RAISE_ON_ERRORS_KEY, True
        )

        self._cloud_storage_client: S3Client | ABSClient | None = None

        # enable asan abort / core dumps by default
        self._environment = dict(
            ASAN_OPTIONS="abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1"
        )

        # If lsan_suppressions.txt exists, then include it
        if os.path.exists(LSAN_SUPPRESSIONS_FILE):
            self.logger.debug(f"{LSAN_SUPPRESSIONS_FILE} exists")
            self._environment["LSAN_OPTIONS"] = f"suppressions={LSAN_SUPPRESSIONS_FILE}"
        else:
            self.logger.debug(f"{LSAN_SUPPRESSIONS_FILE} does not exist")

        # ubsan, halt at first violation and include a stack trace
        # in the logs.
        ubsan_opts = "print_stacktrace=1:halt_on_error=1:abort_on_error=1"
        if os.path.exists(UBSAN_SUPPRESSIONS_FILE):
            ubsan_opts += f":suppressions={UBSAN_SUPPRESSIONS_FILE}"

        self._environment["UBSAN_OPTIONS"] = ubsan_opts

        if environment is not None:
            self._environment.update(environment)

        self.config_file_lock = threading.Lock()

        self._saved_executable = False

        self._tls_cert = None
        self._init_tls()

        # Each time we start a node and write out its node_config (redpanda.yaml),
        # stash a copy here so that we can quickly look up e.g. addresses later.
        self._node_configs: dict[ClusterNode, dict[str, Any]] = {}

        self._seed_servers = self.nodes

        self._expect_max_controller_records = 1000

    def restart_nodes(
        self,
        nodes: ClusterNode | list[ClusterNode],
        override_cfg_params: dict[str, Any] | None = None,
        start_timeout: int | None = None,
        stop_timeout: int | None = None,
        auto_assign_node_id: bool = False,
        omit_seeds_on_idx_one: bool = True,
        extra_cli: list[str] = [],
    ) -> None:
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            # The list() wrapper is to cause futures to be evaluated here+now
            # (including throwing any exceptions) and not just spawned in background.
            def stop_with_timeout(n: ClusterNode) -> None:
                self.stop_node(n, timeout=stop_timeout)

            def start_with_params(n: ClusterNode) -> None:
                self.start_node(
                    n,
                    override_cfg_params=override_cfg_params,
                    timeout=start_timeout,
                    auto_assign_node_id=auto_assign_node_id,
                    omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                    extra_cli=extra_cli,
                )

            list(executor.map(stop_with_timeout, nodes))
            list(executor.map(start_with_params, nodes))

    def set_extra_rp_conf(self, conf: dict[str, Any]):
        self._extra_rp_conf = conf
        if self._si_settings is not None:
            self._extra_rp_conf = self._si_settings.update_rp_conf(self._extra_rp_conf)

    def set_si_settings(self, si_settings: SISettings):
        si_settings.load_context(self.logger, self._context)
        self._si_settings = si_settings
        self._extra_rp_conf = self._si_settings.update_rp_conf(self._extra_rp_conf)

    def use_stress_fiber(self) -> bool:
        """Return true if the test should run with the stress fiber."""
        use_stress_fiber = self._context.globals.get(
            self.GLOBAL_USE_STRESS_FIBER, self.DEFAULT_USE_STRESS_FIBER
        )
        if use_stress_fiber == "ON":
            return True
        elif use_stress_fiber == "OFF":
            return False

        self.logger.warning(f"{self.GLOBAL_USE_STRESS_FIBER} should be 'ON', or 'OFF'")
        return False

    def get_stress_fiber_params(self) -> Tuple[int, int, int]:
        fibers = int(
            self._context.globals.get(
                self.GLOBAL_NUM_STRESS_FIBERS, self.DEFAULT_NUM_STRESS_FIBERS
            )
        )
        min_ms = int(
            self._context.globals.get(
                self.GLOBAL_STRESS_FIBER_MIN_MS, self.DEFAULT_STRESS_FIBER_MIN_MS
            )
        )
        max_ms = int(
            self._context.globals.get(
                self.GLOBAL_STRESS_FIBER_MAX_MS, self.DEFAULT_STRESS_FIBER_MAX_MS
            )
        )
        return (fibers, min_ms, max_ms)

    def add_extra_rp_conf(self, conf: dict[str, Any]):
        self._extra_rp_conf = {**self._extra_rp_conf, **conf}

    def all_nodes_abc(self) -> list[ClusterNode]:
        return self.nodes

    def healthy(self):
        """
        A primitive health check on all the nodes which returns True when all
        nodes report that no under replicated partitions exist. This should
        later be replaced by a proper / official start-up probe type check on
        the health of a node after a restart.
        """

        def check_node(node: ClusterNode) -> tuple[int, bool]:
            """Returns (count, had_error)."""
            try:
                metrics = self.metrics(node)
            except Exception:
                return (0, True)
            count = 0
            for family in metrics:
                for sample in family.samples:
                    if (
                        sample.name
                        == "vectorized_cluster_partition_under_replicated_replicas"
                    ):
                        count += int(sample.value)
            return (count, False)

        results = self.for_nodes(self.nodes, check_node)
        return all(not had_error and count == 0 for count, had_error in results)

    def rolling_restart_nodes(
        self,
        nodes: ClusterNode | list[ClusterNode],
        override_cfg_params: dict[str, Any] | None = None,
        start_timeout: int | None = None,
        stop_timeout: int | None = None,
        use_maintenance_mode: bool = True,
        omit_seeds_on_idx_one: bool = True,
        auto_assign_node_id: bool = False,
    ):
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        restarter = RollingRestarter(self)
        restarter.restart_nodes(
            nodes,
            override_cfg_params=override_cfg_params,
            start_timeout=start_timeout,
            stop_timeout=stop_timeout,
            use_maintenance_mode=use_maintenance_mode,
            omit_seeds_on_idx_one=omit_seeds_on_idx_one,
            auto_assign_node_id=auto_assign_node_id,
        )

    def set_resource_settings(self, rs: ResourceSettings):
        self._resource_settings = rs

    @property
    def si_settings(self) -> SISettings:
        """Return the SISettings object associated with this redpanda service,
        containing the cloud storage associated settings. Throws if si settings
        were not configured for this service."""
        assert self._si_settings, (
            "si_settings were None, probably because they were not specified during redpanda service creation"
        )
        return self._si_settings

    def trim_logs(self):
        if not self._trim_logs:
            return

        # Excessive logging may cause disks to fill up quickly.
        # Call this method to removes TRACE and DEBUG log lines from redpanda logs
        # Ensure this is only done on tests that have passed
        def prune(node: ClusterNode):
            node.account.ssh(
                f"sed -i -E -e '/TRACE|DEBUG/d' {RedpandaService.STDOUT_STDERR_CAPTURE} || true"
            )

        self.for_nodes(self.nodes, prune)

    def node_id(
        self, node: ClusterNode, force_refresh: bool = False, timeout_sec: int = 30
    ) -> int:
        """
        Returns the node ID (redpanda broker ID) of a given node. Uses a cached
        value if present unless 'force_refresh' is set to True.

        :param: force_refresh if True, always queries for the node ID from the node's
        API, never uses cached data.

        :param: timeout_sec number of seconds to try to find the node before giving up,
        which is relevant in the case there is no cached index <-> node info info and
        we are not able to contact some some. The full timeout may apply for each node
        in the cluster so the before this method returns may be up to timeout_sec *
        node_count. timeout_sec=0 means do not retry failed network calls.

        This throws BrokerNotStarted if the specified node is not started.

        NOTE: this is not thread-safe.
        """
        idx = self.idx(node)
        if not force_refresh:
            if idx in self._node_id_by_idx:
                return self._node_id_by_idx[idx]

        # fail immediately if node is not started, since we cannot possibly
        # fetch its ID in that case
        if node not in self._started:
            raise BrokerNotStartedError(f"node {node.name} not started")

        self.logger.debug(
            f"Fetching node ID (broker ID) for {node.name} (force_refresh={force_refresh})"
        )

        def _try_get_node_id():
            try:
                node_cfg = self._admin.get_node_config(node)
            except Exception as e:
                self.logger.debug(f"error in get_node_config for {node.name}: {e}")
                return (False, -1)
            return (True, node_cfg["node_id"])

        node_id = wait_until_result(
            _try_get_node_id,
            timeout_sec=timeout_sec,
            err_msg=f"couldn't reach admin endpoint for {node.account.hostname}",
        )
        self.logger.info(f"Got node ID for {node.account.hostname}: {node_id}")
        self._node_id_by_idx[idx] = node_id
        return node_id

    def kafka_client_security(self):
        if self._security_config:

            def get_str(key: str):
                v = self._security_config[key]
                assert isinstance(v, str)
                return v

            creds = SaslCredentials(
                username=get_str("sasl_plain_username"),
                password=get_str("sasl_plain_password"),
                algorithm=get_str("sasl_mechanism"),
            )
        else:
            creds = None

        return KafkaClientSecurity(creds, tls_enabled=False)

    def set_skip_if_no_redpanda_log(self, v: bool):
        self._skip_if_no_redpanda_log = v

    def raise_on_bad_logs(
        self, allow_list: LogAllowList = (), test_start_time: float = 0
    ):
        """
        Raise a BadLogLines exception if any nodes' logs contain errors not
        permitted by `allow_list`

        :param allow_list: LogAllowList of additional lines to ignore (default
            ignores are always included)
        """

        allow_list = prepare_allow_list(allow_list)

        def check_node(node: ClusterNode) -> tuple[str | None, ClusterNode] | None:
            if self._skip_if_no_redpanda_log and not node.account.exists(
                RedpandaService.STDOUT_STDERR_CAPTURE
            ):
                self.logger.info(
                    f"{RedpandaService.STDOUT_STDERR_CAPTURE} not found on {node.account.hostname}. Skipping log scan."
                )
                return None
            return (self.get_version_if_not_head(node), node)

        _searchable_nodes = [
            r for r in self.for_nodes(self.nodes, check_node) if r is not None
        ]

        lsearcher = LogSearchLocal(
            self._context,
            allow_list,
            self.logger,
            RedpandaService.STDOUT_STDERR_CAPTURE,
        )
        lsearcher.search_logs(_searchable_nodes)

    @property
    def dedicated_nodes(self):
        """
        If true, the nodes are dedicated linux servers, e.g. EC2 instances.

        If false, the nodes are containers that share CPUs and memory with
        one another.
        :return:
        """
        return self._dedicated_nodes

    def redpanda_env_preamble(self):
        # Pass environment variables via FOO=BAR shell expressions
        return " ".join([f"{k}={v}" for (k, v) in self._environment.items()])

    def set_seed_servers(self, node_list: list[ClusterNode]):
        assert len(node_list) > 0
        self._seed_servers = node_list

    def set_environment(self, environment: dict[str, str]):
        self._environment.update(environment)

    def unset_environment(self, keys: list[str]):
        for k in keys:
            try:
                del self._environment[k]
            except KeyError:
                pass

    def set_extra_node_conf(self, node: ClusterNode, conf: dict[str, Any]):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        self._extra_node_conf[node] = conf

    def add_extra_node_conf(self, node: ClusterNode, conf: dict[str, Any]):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        self._extra_node_conf[node] = {**self._extra_node_conf[node], **conf}

    def set_security_settings(self, settings: SecurityConfig):
        self._security = settings
        self._init_tls()

    def set_pandaproxy_settings(self, settings: PandaproxyConfig):
        self._pandaproxy_config = settings

    def set_schema_registry_settings(self, settings: SchemaRegistryConfig):
        self._schema_registry_config = settings

    def set_audit_log_settings(self, settings: AuditLogConfig):
        self._audit_log_config = settings

    def _init_tls(self):
        """
        Call this if tls setting may have changed.
        """
        if self._security.tls_provider:
            # build a cert for clients used internally to the service
            self._tls_cert = self._security.tls_provider.create_service_client_cert(
                self, "redpanda.service.admin"
            )

    def sasl_enabled(self):
        return self._security.sasl_enabled()

    def mtls_identity_enabled(self):
        return self._security.mtls_identity_enabled()

    def endpoint_authn_method(self):
        return self._security.endpoint_authn_method

    def require_client_auth(self):
        return self._security.require_client_auth

    def get_node_memory_mb(self) -> int:
        if self._resource_settings.memory_mb is not None:
            self.logger.info("get_node_memory_mb: got from ResourceSettings")
            return self._resource_settings.memory_mb
        elif self._dedicated_nodes is False:
            self.logger.info("get_node_memory_mb: using ResourceSettings default")
            return self._resource_settings.DEFAULT_MEMORY_MB
        else:
            self.logger.info("get_node_memory_mb: fetching from node")
            # Assume nodes are symmetric, so we can just ask one
            # how much memory it has.
            node = self.nodes[0]
            line = node.account.ssh_output(
                "cat /proc/meminfo | grep MemTotal", timeout_sec=10
            )
            # Output line is like "MemTotal:       32552236 kB"
            memory_kb = int(line.strip().split()[1])
            return memory_kb // 1024

    def get_node_cpu_count(self) -> int:
        if self._resource_settings.num_cpus is not None:
            self.logger.info("get_node_cpu_count: got from ResourceSettings")
            return self._resource_settings.num_cpus
        elif self._dedicated_nodes is False:
            self.logger.info("get_node_cpu_count: using ResourceSettings default")
            return self._resource_settings.DEFAULT_NUM_CPUS
        else:
            self.logger.info("get_node_cpu_count: fetching from node")

            # Assume nodes are symmetric, so we can just ask one
            node = self.nodes[0]
            core_count_str = node.account.ssh_output(
                "cat /proc/cpuinfo | grep ^processor | wc -l", timeout_sec=10
            )
            return int(core_count_str.strip())

    def get_node_disk_free(self):
        # Assume nodes are symmetric, so we can just ask one
        node = self.nodes[0]

        if node.account.exists(self.PERSISTENT_ROOT):
            df_path = self.PERSISTENT_ROOT
        else:
            # If dir doesn't exist yet, use the parent.
            df_path = os.path.dirname(self.PERSISTENT_ROOT)

        df_out = node.account.ssh_output(f"df --output=avail {df_path}", timeout_sec=10)

        avail_kb = int(df_out.strip().split(b"\n")[1].strip())

        if not self.dedicated_nodes:
            # Assume docker images share a filesystem.  This may not
            # be the truth (e.g. in CI they get indepdendent XFS
            # filesystems), but it's the safe assumption on e.g.
            # a workstation.
            avail_kb = int(avail_kb / len(self.nodes))

        return avail_kb * 1024

    def get_node_disk_usage(self, node: ClusterNode):
        """
        get disk usage for the redpanda volume on a particular node
        """

        for line in node.account.ssh_capture(
            f"df --block-size 1 {self.PERSISTENT_ROOT}", timeout_sec=10
        ):
            self.logger.debug(line.strip())
            if self.PERSISTENT_ROOT in line:
                return int(line.split()[2])
        assert False, "couldn't parse df output"

    def _startup_poll_interval(self, first_start: bool):
        """
        During startup, our eagerness depends on whether it's the first
        start, where we expect a redpanda node to start up very quickly,
        or a subsequent start where it may be more sedate as data replay
        takes place.
        """
        return 0.2 if first_start else 1.0

    def wait_for_membership(self, first_start: bool, timeout_sec: int = 30):
        self.logger.info("Waiting for all brokers to join cluster")

        wait_until(
            lambda: {n for n in self._started if self.registered(n)} == self._started,
            timeout_sec=timeout_sec,
            backoff_sec=self._startup_poll_interval(first_start),
            err_msg="Cluster membership did not stabilize",
        )

    def setup_azurite_dns(self):
        """
        Azure API relies on <container>.something DNS.  Doing DNS configuration
        with docker/podman is unstable, as it isn't consistent across operating
        systems.  Instead do it more crudely but robustly, but editing /etc/hosts.
        """
        azurite_ip = socket.gethostbyname(AZURITE_HOSTNAME)
        azurite_dns = (
            f"{self.si_settings.cloud_storage_azure_storage_account}.blob.localhost"
        )

        def update_hosts_file(node_name: str, path: str):
            ducktape_hosts = open(path, "r").read()
            if azurite_dns not in ducktape_hosts:
                ducktape_hosts += f"\n{azurite_ip}   {azurite_dns}\n"
                self.logger.info(
                    f"Adding Azurite entry to {path} for node {node_name}, new content:"
                )
                self.logger.info(ducktape_hosts)
                with open(path, "w") as f:
                    f.write(ducktape_hosts)
            else:
                self.logger.debug(
                    f"Azurite /etc/hosts entry already present on {node_name}:"
                )
                self.logger.debug(ducktape_hosts)

        # Edit /etc/hosts on the node where ducktape is running
        update_hosts_file("ducktape", "/etc/hosts")

        def setup_node_dns(node: ClusterNode):
            tmpfile = f"/tmp/{node.name}_hosts"
            node.account.copy_from("/etc/hosts", tmpfile)
            update_hosts_file(node.name, tmpfile)
            node.account.copy_to(tmpfile, "/etc/hosts")

        # Edit /etc/hosts on Redpanda nodes
        self.for_nodes(self.nodes, setup_node_dns)

    def start(
        self,
        nodes: list[ClusterNode] | None = None,
        clean_nodes: bool = True,
        start_si: bool = True,
        expect_fail: bool = False,
        auto_assign_node_id: bool = False,
        omit_seeds_on_idx_one: bool = True,
        node_config_overrides: NodeConfigOverridesT = {},
        skip_storage_init_check: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Start the service on all nodes.

        :param expect_fail: if true, expect redpanda nodes to terminate shortly
                            after starting.  Raise exception if they don't.
        """

        assert not kwargs, f"unexpected keyword args: {kwargs}"

        # Callers sometimes forget that this must be keyed by ClusterNode.
        for k in node_config_overrides:
            assert type(k) is ClusterNode, (
                f"Expected ClusterNode as key in node_config_overrides, got {type(k)}"
            )

        to_start = nodes if nodes is not None else self.nodes
        assert all((node in self.nodes for node in to_start))
        self.logger.info("%s: starting service" % self.who_am_i())

        first_start = self._start_time < 0
        if first_start:
            self._start_time = time.time()

            if (
                self._si_settings
                and self._si_settings.cloud_storage_type is CloudStorageType.ABS
                and self._si_settings.cloud_storage_azure_storage_account
                == SISettings.ABS_AZURITE_ACCOUNT
            ):
                self.setup_azurite_dns()

        self.logger.debug(
            self.who_am_i()
            + ": killing processes and attempting to clean up before starting"
        )

        def clean_one(node: ClusterNode):
            try:
                self.stop_node(node)
            except Exception:
                pass

            try:
                if clean_nodes:
                    # Expected usage is that we may install new binaries before
                    # starting the cluster, and installation-cleaning happened
                    # when we started the installer.
                    self.clean_node(node, preserve_current_install=True)
                else:
                    self.logger.debug("%s: skip cleaning node" % self.who_am_i(node))
            except Exception:
                self.logger.exception(f"Error cleaning node {node.account.hostname}:")
                raise

        if first_start:
            # Clean all nodes on the first start because the test can choose to initialize
            # the cluster with a smaller node subset and start other nodes later with
            # redpanda.start_node() (which doesn't invoke clean_node)
            self.for_nodes(self.nodes, clean_one)
        else:
            self.for_nodes(to_start, clean_one)

        if first_start:
            self.write_tls_certs()
            self.write_bootstrap_cluster_config()

        if start_si and self._si_settings is not None:
            self.start_si()

        def start_one(node: ClusterNode):
            node_overrides = (
                node_config_overrides[node] if node in node_config_overrides else {}
            )
            self.logger.debug("%s: starting node" % self.who_am_i(node))
            self.start_node(
                node,
                first_start=first_start,
                expect_fail=expect_fail,
                auto_assign_node_id=auto_assign_node_id,
                omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                override_cfg_params=node_overrides,
            )

        try:
            self.for_nodes(to_start, start_one)
        except TimeoutError as e:
            if expect_fail:
                raise e
            if "failed to start within" in str(e):
                self.logger.debug(f"Checking for crashes after start-up error: {e}")
                self.raise_on_crash()
            raise e

        if expect_fail:
            # If we got here without an exception, it means we failed as expected
            return

        if self._start_duration_seconds < 0:
            self._start_duration_seconds = time.time() - self._start_time

        if not self._skip_create_superuser:
            self._admin.create_user(*self._superuser, await_exists=True)

        self.wait_for_membership(first_start=first_start)

        if not skip_storage_init_check:
            self.logger.info("Verifying storage is in expected state")

            expected = {
                "redpanda": {"controller", "kvstore"},
                "kafka": {
                    "_redpanda.audit_log",
                    "_redpanda.transform_logs",
                },
                "kafka_internal": {"ct_l1_domain"},
            }
            expected["l1_staging"] = set()  # make type deduction happy

            storage = self.storage(nodes=to_start)
            for node in storage.nodes:
                unexpected_ns = set(node.ns) - set(expected.keys())
                if unexpected_ns:
                    for ns in unexpected_ns:
                        self.logger.error(
                            f"node {node.name}: unexpected namespace: {ns}, "
                            f"topics: {set(node.ns[ns].topics)}"
                        )
                    raise RuntimeError("Unexpected files in data directory")

                for ns in node.ns:
                    unexpected_topics = set(node.ns[ns].topics) - expected[ns]
                    if unexpected_topics:
                        self.logger.error(
                            f"node {node.name}: unexpected topics in {ns} namespace: "
                            f"{unexpected_topics}"
                        )
                        raise RuntimeError("Unexpected files in data directory")

        if self.sasl_enabled():
            username, password, algorithm = self._superuser
            self._security_config = dict(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism=algorithm,
                sasl_plain_username=username,
                sasl_plain_password=password,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=3000,
            )

        # Start stress fiber if requested
        if self.use_stress_fiber():

            def start_stress_fiber(node: ClusterNode):
                count, min_ms, max_ms = self.get_stress_fiber_params()
                self.start_stress_fiber(node, count, min_ms, max_ms)

            if first_start:
                self.logger.info(f"Starting stress fiber for {len(to_start)} nodes")
                self.for_nodes(to_start, start_stress_fiber)
            else:
                self.logger.info(f"Starting stress fiber for {len(self.nodes)} nodes")
                self.for_nodes(self.nodes, start_stress_fiber)

    def write_crl_file(self, node: ClusterNode, ca: tls.CertificateAuthority) -> None:
        assert ca.crl is not None, "CRL file is required"
        self.logger.info(
            f"Writing Redpanda node tls ca CRL file: {RedpandaService.TLS_CA_CRL_FILE}"
        )
        self.logger.debug(open(ca.crl, "r").read())
        node.account.mkdirs(os.path.dirname(RedpandaService.TLS_CA_CRL_FILE))
        node.account.copy_to(ca.crl, RedpandaService.TLS_CA_CRL_FILE)
        node.account.ssh(f"chmod 755 {RedpandaService.TLS_CA_CRL_FILE}")

    def write_tls_certs(self):
        if not self._security.tls_provider:
            return

        ca = self._security.tls_provider.ca
        for node in self.nodes:
            cert = self._security.tls_provider.create_broker_cert(self, node)

            self.logger.info(
                f"Writing Redpanda node tls key file: {RedpandaService.TLS_SERVER_KEY_FILE}"
            )
            self.logger.debug(open(cert.key, "r").read())
            node.account.mkdirs(os.path.dirname(RedpandaService.TLS_SERVER_KEY_FILE))
            node.account.copy_to(cert.key, RedpandaService.TLS_SERVER_KEY_FILE)

            self.logger.info(
                f"Writing Redpanda node tls cert file: {RedpandaService.TLS_SERVER_CRT_FILE}"
            )
            self.logger.debug(open(cert.crt, "r").read())
            node.account.mkdirs(os.path.dirname(RedpandaService.TLS_SERVER_CRT_FILE))
            node.account.copy_to(cert.crt, RedpandaService.TLS_SERVER_CRT_FILE)

            self.logger.info(
                f"Writing Redpanda node P12 file: {RedpandaService.TLS_SERVER_P12_FILE}"
            )
            self.logger.debug("P12 file is binary encoded")
            node.account.mkdirs(os.path.dirname(RedpandaService.TLS_SERVER_P12_FILE))
            node.account.copy_to(cert.p12_file, RedpandaService.TLS_SERVER_P12_FILE)

            self.logger.info(
                f"Writing Redpanda node tls ca cert file: {RedpandaService.TLS_CA_CRT_FILE}"
            )
            self.logger.debug(open(ca.crt, "r").read())
            node.account.mkdirs(os.path.dirname(RedpandaService.TLS_CA_CRT_FILE))
            node.account.copy_to(ca.crt, RedpandaService.TLS_CA_CRT_FILE)
            node.account.ssh(f"chmod 755 {RedpandaService.TLS_CA_CRT_FILE}")

            assert ca.crl is not None, "Missing CRL file"
            self.write_crl_file(node, ca)

            node.account.copy_to(ca.crt, RedpandaService.SYSTEM_TLS_CA_CRT_FILE)
            node.account.ssh(f"chmod 755 {RedpandaService.SYSTEM_TLS_CA_CRT_FILE}")
            node.account.ssh("update-ca-certificates")

            if self._pandaproxy_config is not None:
                self._pandaproxy_config.maybe_write_client_certs(
                    node,
                    self.logger,
                    PandaproxyConfig.PP_TLS_CLIENT_KEY_FILE,
                    PandaproxyConfig.PP_TLS_CLIENT_CRT_FILE,
                )
                self._pandaproxy_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._pandaproxy_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._pandaproxy_config.truststore_file = (
                    RedpandaService.TLS_CA_CRT_FILE
                )
                self._pandaproxy_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

            if self._schema_registry_config is not None:
                self._schema_registry_config.maybe_write_client_certs(
                    node,
                    self.logger,
                    SchemaRegistryConfig.SR_TLS_CLIENT_KEY_FILE,
                    SchemaRegistryConfig.SR_TLS_CLIENT_CRT_FILE,
                )
                self._schema_registry_config.server_key = (
                    RedpandaService.TLS_SERVER_KEY_FILE
                )
                self._schema_registry_config.server_crt = (
                    RedpandaService.TLS_SERVER_CRT_FILE
                )
                self._schema_registry_config.truststore_file = (
                    RedpandaService.TLS_CA_CRT_FILE
                )
                self._schema_registry_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

            if self._audit_log_config is not None:
                self._audit_log_config.maybe_write_client_certs(
                    node,
                    self.logger,
                    AuditLogConfig.AUDIT_LOG_TLS_CLIENT_KEY_FILE,
                    AuditLogConfig.AUDIT_LOG_TLS_CLIENT_CRT_FILE,
                )
                self._audit_log_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._audit_log_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._audit_log_config.truststore_file = RedpandaService.TLS_CA_CRT_FILE
                self._audit_log_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

    def start_redpanda(self, node: ClusterNode, extra_cli: list[str] = []):
        preamble, res_args = self._resource_settings.to_cli(
            dedicated_node=self._dedicated_nodes
        )

        # each node will create its own copy of the .profraw file
        # since each node creates a redpanda broker.
        if self.cov_enabled():
            self._environment.update(
                dict(LLVM_PROFILE_FILE=f'"{RedpandaService.COVERAGE_PROFRAW_CAPTURE}"')
            )

        env_preamble = self.redpanda_env_preamble()

        cur_ver: RedpandaVersionTriple | None = None
        try:
            cur_ver = self.get_version_int_tuple(node)
        except Exception:  # noqa
            pass

        cmd = (
            f"{preamble} {env_preamble} nohup {self.find_binary('redpanda')}"
            f" --redpanda-cfg {RedpandaService.NODE_CONFIG_FILE}"
            f" {self._log_config.to_args(cur_ver)} "
            " --abort-on-seastar-bad-alloc "
            " --dump-memory-diagnostics-on-alloc-failure-kind=all "
            f" {res_args} "
            f" {' '.join(extra_cli)}"
            f" >> {RedpandaService.STDOUT_STDERR_CAPTURE} 2>&1 &"
        )

        node.account.ssh(cmd)

    def check_node(self, node: ClusterNode):
        pid = self.redpanda_pid(node)
        if not pid:
            self.logger.warning(f"No redpanda PIDs found on {node.name}")
            return False

        if not node.account.exists(f"/proc/{pid}"):
            self.logger.warning(f"PID {pid} (node {node.name}) dead")
            return False

        # fall through
        return True

    def start_stress_fiber(
        self, node: ClusterNode, count: int, min_ms: int, max_ms: int
    ):
        """Start stress fiber"""
        admin = Admin(self)
        admin.stress_fiber_start(
            node=node,
            num_fibers=count,
            min_ms_per_scheduling_point=min_ms,
            max_ms_per_scheduling_point=max_ms,
        )

    def all_up(self):
        def check_node(node: ClusterNode):
            pid = self.redpanda_pid(node)
            if not pid:
                self.logger.warning(f"No redpanda PIDs found on {node.name}")
                return False

            if not node.account.exists(f"/proc/{pid}"):
                self.logger.warning(f"PID {pid} (node {node.name}) dead")
                return False

            # fall through
            return True

        return all(self.for_nodes(list(self._started), check_node))

    def signal_redpanda(
        self,
        node: ClusterNode,
        signal: Signals = signal.SIGKILL,
        idempotent: bool = False,
        thread: str | None = None,
    ) -> None:
        """
        :param idempotent: if true, then kill-like signals are ignored if
                           the process is already gone.
        :param thread: if set, then the signal is sent to the given thread
        """
        if thread is None:
            pid = self.redpanda_pid(node)
            if pid is None:
                if idempotent and signal in {SIGKILL, SIGTERM}:
                    return
                else:
                    raise RuntimeError(
                        f"Can't signal redpanda on node {node.name}, it isn't running"
                    )

            node.account.signal(pid, signal, allow_fail=False)
        else:
            tgid, tid = self.redpanda_tid(node, thread)
            script_path = inject_remote_script(node, "tgkill.py")
            cmd = shlex.join(
                ["python3", script_path, str(tgid), str(tid), str(signal.value)]
            )
            node.account.ssh(cmd, allow_fail=False)

    @contextmanager
    def paused_node(self, node: ClusterNode):
        """Context manager to pause redpanda on a node by sending SIGSTOP"""
        # rpk requires all nodes up to operate, so remove from started nodes
        self.remove_from_started_nodes(node, "paused_node")
        self.signal_redpanda(node, signal=signal.SIGSTOP)
        try:
            yield
        finally:
            self.signal_redpanda(node, signal=signal.SIGCONT)
            self.add_to_started_nodes(node)

    def sockets_clear(self, node: RemoteClusterNode):
        """
        Check that high-numbered redpanda ports (in practice, just the internal
        RPC port) are clear on the node, to avoid TIME_WAIT sockets from previous
        tests interfering with redpanda startup.

        In principle, redpanda should not have a problem with TIME_WAIT sockets
        on its port (redpanda binds with SO_REUSEADDR), but in practice we have
        seen "Address in use" errors:
        https://github.com/redpanda-data/redpanda/pull/3754
        """
        for line in node.account.ssh_capture("netstat -ant", timeout_sec=10):
            self.logger.debug(f"node={node.name} {line.strip()}")

            # Parse output line
            tokens = line.strip().split()
            if len(tokens) != 6:
                # Header, skip
                continue
            _, _, _, src, dst, state = tokens

            if src.endswith(":33145"):
                self.logger.info(
                    f"Port collision on node {node.name}: {src}->{dst} {state}"
                )
                return False

        # Fall through: no problematic lines found
        return True

    def lsof_node(self, node: ClusterNode, filter: str | None = None):
        """
        Get the list of open files for a running node

        :param filter: If given, this is a grep regex that will filter the files we list

        :return: yields strings
        """
        first = True
        cmd = f"lsof -nP -p {self.redpanda_pid(node)}"
        if filter is not None:
            cmd += f" | grep {filter}"
        for line in node.account.ssh_capture(cmd, timeout_sec=60):
            if first and not filter:
                # First line is a header, skip it
                first = False
                continue
            try:
                filename = line.split()[-1]
            except IndexError:
                # Malformed line
                pass
            else:
                yield filename

    def is_node_ready(self, node: ClusterNode):
        """
        Calls Admin API's v1/status/ready endpoint to verify if the node
        is ready
        """
        status = None
        try:
            status = Admin.ready(node).get("status")
        except requests.exceptions.ConnectionError:
            self.logger.debug(f"node {node.name} not yet accepting connections")
            return False
        except:
            self.logger.exception(
                f"error on getting status from {node.account.hostname}"
            )
            raise
        if status != "ready":
            self.logger.debug(
                f"status of {node.account.hostname} isn't ready: {status}"
            )
            return False
        return True

    def start_node(
        self,
        node: ClusterNode,
        override_cfg_params: dict[str, Any] | None = None,
        timeout: int | None = None,
        write_config: bool = True,
        first_start: bool = False,
        expect_fail: bool = False,
        auto_assign_node_id: bool = False,
        omit_seeds_on_idx_one: bool = True,
        skip_readiness_check: bool = False,
        node_id_override: int | None = None,
        extra_cli: list[str] = [],
        **kwargs: Any,
    ) -> None:
        """
        Start a single instance of redpanda. This function will not return until
        redpanda appears to have started successfully. If redpanda does not
        start within a timeout period the service will fail to start. Thus this
        function also acts as an implicit test that redpanda starts quickly.
        """

        assert not kwargs, "kwargs not empty"
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        self.write_openssl_config_file(node)

        if write_config:
            self.write_node_conf_file(
                node,
                override_cfg_params,
                auto_assign_node_id=auto_assign_node_id,
                omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                node_id_override=node_id_override,
            )

        if timeout is None:
            timeout = self.node_ready_timeout_s

        if self.dedicated_nodes:
            # When running on dedicated nodes, we should always be running on XFS.  If we
            # aren't, it's probably an accident that can easily cause spurious failures
            # and confusion, so be helpful and fail out early.
            fs = node.account.ssh_output(
                f"stat -f -c %T {self.PERSISTENT_ROOT}", timeout_sec=10
            ).strip()
            if fs != b"xfs":
                raise RuntimeError(
                    f"Non-XFS filesystem {fs} at {self.PERSISTENT_ROOT} on {node.name}"
                )

        def start_rp():
            self.start_redpanda(node, extra_cli=extra_cli)

            if expect_fail:
                wait_until(
                    lambda: self.redpanda_pid(node) is None,
                    timeout_sec=timeout,
                    backoff_sec=0.2,
                    err_msg=f"Redpanda processes did not terminate on {node.name} during startup as expected in {timeout} sec",
                )
            elif not skip_readiness_check:
                wait_until(
                    lambda: self.is_node_ready(node),
                    timeout_sec=timeout,
                    backoff_sec=self._startup_poll_interval(first_start),
                    err_msg=f"Redpanda service {node.account.hostname} failed to start within {timeout} sec",
                    retry_on_exc=True,
                )

        self.logger.debug("Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        if not expect_fail:
            self.add_to_started_nodes(node)

    def start_node_with_rpk(
        self, node: ClusterNode, additional_args: str = "", clean_node: bool = True
    ):
        """
        Start a single instance of redpanda using rpk. similar to start_node,
        this function will not return until redpanda appears to have started
        successfully.
        """
        self.logger.debug(
            self.who_am_i()
            + ": killing processes and attempting to clean up before starting"
        )
        try:
            self.stop_node(node)
        except Exception:
            pass

        if clean_node:
            self.clean_node(node, preserve_current_install=True)
        else:
            self.logger.debug("%s: skip cleaning node" % self.who_am_i(node))
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        env_vars = " ".join([f"{k}={v}" for (k, v) in self._environment.items()])
        rpk = RpkRemoteTool(self, node)

        _, args = self._resource_settings.to_cli(dedicated_node=self._dedicated_nodes)
        additional_args += " " + args

        def start_rp():
            rpk.redpanda_start(
                RedpandaService.STDOUT_STDERR_CAPTURE, additional_args, env_vars
            )

            wait_until(
                lambda: self.is_node_ready(node),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Redpanda service {node.account.hostname} failed to start within 60 sec using rpk",
                retry_on_exc=True,
            )

        self.logger.debug("Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        self.add_to_started_nodes(node)

        # We need to manually read the config from the file and add it
        # to _node_configs since we use rpk to write the file instead of
        # the write_node_conf_file method.
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)
            with open(os.path.join(d, "redpanda.yaml")) as f:
                actual_config = yaml.full_load(f.read())
                self._node_configs[node] = actual_config

    def _log_node_shutdown_analysis(self, node: ClusterNode):
        """
        Analyze a node's failure to shutdown within the allocated time to try
        to diagnose the reason for shutdown hang.
        """
        self.logger.debug(f"Gathering logs to analyze from {node.name}...")
        expr = '"application.*Stopping"'
        cmd = f"grep {expr} {RedpandaService.STDOUT_STDERR_CAPTURE} || true"

        other_stopping: list[str] = []
        last_next_to_shutdown = None
        for line in node.account.ssh_capture(cmd):
            if "next to shutdown" in line:
                last_next_to_shutdown = line
            else:
                other_stopping.append(line)

        if last_next_to_shutdown is None:
            self.logger.debug(
                "No structured shutdown messages found. Hang may have occurred in early shutdown"
            )
            return

        self.logger.debug(f"Last structured shutdown line: {last_next_to_shutdown}")
        next_service = last_next_to_shutdown.split()[-1]

        for line in other_stopping:
            if f"Stopping {next_service}" in line:
                self.logger.debug(f"Found stopping message for last service {line}")
                return

        self.logger.debug(f"Did not find stopping message for service {next_service}")

    def _log_node_process_state(self, node: ClusterNode):
        """
        For debugging issues around starting and stopping processes: log
        which processes are running and which ports are in use.
        """

        self.logger.debug(
            f"Gathering process and port usage information on {node.name}..."
        )

        # Capture general process information
        process_lines: list[str] = []
        for line in node.account.ssh_capture("ps aux --sort=-%mem", timeout_sec=30):
            process_lines.append(line.strip())

        output_str = "\n".join(process_lines)
        self.logger.debug(f"{node.name}: ps aux output:\n{output_str}")

        # Capture network information
        netstat_lines = [
            line.strip()
            for line in node.account.ssh_capture("netstat -panelot", timeout_sec=30)
        ]

        output_str = "\n".join(netstat_lines)
        self.logger.debug(f"{node.name}: netstat -panelot output:\n{output_str}")

    def _log_process_status(self, node: ClusterNode, pid: int):
        """
        Log the status of a process from /proc/[pid]/status
        """
        self.logger.debug(f"{node.name}: Gathering /proc/{pid}/status for node...")
        cmd = f"cat /proc/{pid}/status"
        lines: list[str] = []
        for line in node.account.ssh_capture(cmd, allow_fail=True, timeout_sec=10):
            if re.search(r"CoreDumping:\s*1", line):
                self.logger.warning(
                    f"{node.name}: Detected core dumping in process {pid} status."
                )
            lines.append(line.strip())

        output_str = "\n".join(lines)
        self.logger.debug(f"{node.name}: /proc/{pid}/status:\n{output_str}")

    def start_service(self, node: ClusterNode, start: Callable[[], None]) -> None:
        # Maybe the service collides with something that wasn't cleaned up
        # properly: let's peek at what's going on on the node before starting it.
        self._log_node_process_state(node)

        try:
            start()
        except:
            # In case our failure to start is something like an "address in use", we
            # would like to know what else is going on on this node.
            self.logger.warning(
                f"Failed to start on {node.name}, gathering node ps and netstat..."
            )
            self._log_node_process_state(node)
            raise

    def start_si(self):
        if self.si_settings.cloud_storage_type == CloudStorageType.S3:
            self._cloud_storage_client = S3Client(
                region=self.si_settings.cloud_storage_region,
                access_key=self.si_settings.cloud_storage_access_key,
                secret_key=self.si_settings.cloud_storage_secret_key,
                endpoint=self.si_settings.endpoint_url,
                logger=self.logger,
                signature_version=self.si_settings.cloud_storage_signature_version,
                before_call_headers=self.si_settings.before_call_headers,
                use_fips_endpoint=self.si_settings.use_fips_endpoint(),
                addressing_style=self.si_settings.addressing_style,
            )

            self.logger.debug(
                f"Creating S3 bucket: {self.si_settings.cloud_storage_bucket}"
            )
        elif self.si_settings.cloud_storage_type == CloudStorageType.ABS:
            # Make sure that use_bucket_cleanup_policy if False for ABS:
            # 1) We don't implement it.
            # 2) It's not needed because ABS buckets can be deleted without emptying.
            self.si_settings.use_bucket_cleanup_policy = False
            self._cloud_storage_client = ABSClient(
                logger=self.logger,
                storage_account=self.si_settings.cloud_storage_azure_storage_account,
                shared_key=self.si_settings.cloud_storage_azure_shared_key,
                endpoint=self.si_settings.endpoint_url,
            )
            self.logger.debug(
                f"Creating ABS container: {self.si_settings.cloud_storage_bucket}"
            )
        else:
            raise RuntimeError(
                f"Unsupported cloud_storage_type: {self.si_settings.cloud_storage_type}"
            )

        if not self.si_settings.bypass_bucket_creation:
            assert self.si_settings.cloud_storage_bucket, "No SI bucket configured"
            self.cloud_storage_client.create_bucket(
                self.si_settings.cloud_storage_bucket
            )

        # If the test has requested to use a bucket cleanup policy then we
        # attempt to create one which will remove everything from the bucket
        # after one day.
        # This is a time optimization to avoid waiting hours cleaning up tiny
        # objects created by scale tests.
        if self.si_settings.use_bucket_cleanup_policy:
            self.cloud_storage_client.create_expiration_policy(
                bucket=self.si_settings.cloud_storage_bucket, days=1
            )

    @property
    def cloud_storage_client(self):
        assert self._cloud_storage_client, (
            "cloud storage client not available - did you call start_si()?"
        )
        return self._cloud_storage_client

    def delete_bucket_from_si(self):
        self.logger.info(
            f"cloud_storage_cleanup_strategy = {self.si_settings.cloud_storage_cleanup_strategy}"
        )

        if (
            self.si_settings.cloud_storage_cleanup_strategy
            == CloudStorageCleanupStrategy.ALWAYS_SMALL_BUCKETS_ONLY
        ):
            if self.si_settings.cloud_storage_type == CloudStorageType.ABS:
                # ABS buckets can be deleted without emptying so no need to check size.
                # Also leaving buckets around when using local instance of Azurite causes
                # performance issues and test flakiness.
                self.logger.info(
                    "Always deleting ABS buckets as they don't have to be emptied first."
                )
            else:
                bucket_is_small = True
                max_object_count = 3000

                # See if the bucket is small enough
                t = time.time()
                for i, _ in enumerate(
                    self.cloud_storage_client.list_objects(
                        self.si_settings.cloud_storage_bucket
                    )
                ):
                    if i >= max_object_count:
                        bucket_is_small = False
                        break
                self.logger.info(
                    f"Determining bucket count for {self.si_settings.cloud_storage_bucket} up to {max_object_count} objects took {time.time() - t}s"
                )
                if bucket_is_small:
                    # Log grep hint: "a small bucket"
                    self.logger.info(
                        f"Bucket {self.si_settings.cloud_storage_bucket} is a small bucket (deleting it)"
                    )
                else:
                    self.logger.info(
                        f"Bucket {self.si_settings.cloud_storage_bucket} is NOT a small bucket (NOT deleting it)"
                    )
                    return

        elif (
            self.si_settings.cloud_storage_cleanup_strategy
            == CloudStorageCleanupStrategy.IF_NOT_USING_LIFECYCLE_RULE
        ):
            if self.si_settings.use_bucket_cleanup_policy:
                self.logger.info(
                    f"Skipping deletion of bucket/container: {self.si_settings.cloud_storage_bucket}. "
                    "Using a cleanup policy instead."
                )
                return
        else:
            raise ValueError(
                f"Unimplemented cloud storage cleanup strategy {self.si_settings.cloud_storage_cleanup_strategy}"
            )

        self.logger.debug(
            f"Deleting bucket/container: {self.si_settings.cloud_storage_bucket}"
        )

        assert self.si_settings.cloud_storage_bucket, (
            f"missing bucket : {self.si_settings.cloud_storage_bucket}"
        )
        t = time.time()
        self.cloud_storage_client.empty_and_delete_bucket(
            self.si_settings.cloud_storage_bucket, parallel=self.dedicated_nodes
        )

        self.logger.info(
            f"Emptying and deleting bucket {self.si_settings.cloud_storage_bucket} took {time.time() - t}s"
        )

    def get_objects_from_si(self):
        assert (
            self.cloud_storage_client
            and self._si_settings
            and self._si_settings.cloud_storage_bucket
        ), (
            f"bad si config {self.cloud_storage_client} : {self._si_settings.cloud_storage_bucket if self._si_settings else self._si_settings}"
        )
        return self.cloud_storage_client.list_objects(
            self._si_settings.cloud_storage_bucket
        )

    def partitions(self, topic_name: str | None = None) -> list[Partition]:
        """
        Return partition metadata for the topic.
        """
        kc = KafkaCat(self)
        md = kc.metadata()

        result: list[Partition] = []

        def make_partition(topic_name: str, p: dict[str, Any]):
            index = p["partition"]
            leader_id = p["leader"]
            leader = None if leader_id == -1 else self.get_node_by_id(leader_id)
            replicas = [self.node_by_id(r["id"]) for r in p["replicas"]]
            return Partition(topic_name, index, leader, replicas)

        for topic in md["topics"]:
            if topic["topic"] == topic_name or topic_name is None:
                result.extend(
                    make_partition(topic["topic"], p) for p in topic["partitions"]
                )

        return result

    def set_cluster_config_to_null(self, name: str, **kwargs: Any) -> None:
        self.set_cluster_config(values={name: None}, **kwargs)

    def set_cluster_config(
        self,
        values: dict[str, Any],
        expect_restart: bool = False,
        admin_client: Admin | None = None,
        timeout: int = 10,
        tolerate_stopped_nodes: bool = False,
    ):
        """
        Update cluster configuration and wait for all nodes to report that they
        have seen the new config.

        :param values: dict of property name to value.
        :param expect_restart: set to true if you wish to permit a node restart for needs_restart=yes properties.
                               If you set such a property without this flag, an assertion error will be raised.
        """
        if admin_client is None:
            admin_client = self._admin

        patch_result = admin_client.patch_cluster_config(upsert=values, remove=[])
        new_version = patch_result["config_version"]

        self._wait_for_config_version(
            new_version,
            expect_restart,
            timeout,
            admin_client=admin_client,
            tolerate_stopped_nodes=tolerate_stopped_nodes,
        )

    def enable_development_feature_support(self, key: int | None = None):
        """
        Enable experimental feature support.

        The key must be equal to the current broker time expressed as unix epoch
        in seconds, and be within 1 hour.
        """
        key = int(time.time()) if key is None else key
        self.set_cluster_config(
            dict(
                enable_developmental_unrecoverable_data_corrupting_features=key,
            )
        )

    def _wait_for_config_version(
        self,
        config_version: int,
        expect_restart: bool,
        timeout: int,
        admin_client: Admin | None = None,
        tolerate_stopped_nodes: bool = False,
    ):
        admin_client = admin_client or self._admin
        if tolerate_stopped_nodes:
            started_node_ids = {self.node_id(n) for n in self.started_nodes()}
        else:
            started_node_ids = {}

        def is_ready():
            status = admin_client.get_cluster_config_status(node=self.controller())
            ready = all(
                [
                    n["config_version"] >= config_version
                    for n in status
                    if not tolerate_stopped_nodes or n["node_id"] in started_node_ids
                ]
            )

            return ready, status

        # The version check is >= to permit other config writes to happen in
        # the background, including the write to cluster_id that happens
        # early in the cluster's lifetime
        config_status = wait_until_result(
            is_ready,
            timeout_sec=timeout,
            backoff_sec=0.5,
            err_msg=f"Config status versions did not converge on {config_version}",
        )

        any_restarts = any(n["restart"] for n in config_status)
        if any_restarts and expect_restart:
            self.restart_nodes(self.nodes)
            # Having disrupted the cluster with a restart, wait for the controller
            # to be available again before returning to the caller, so that they do
            # not have to worry about subsequent configuration actions failing.
            admin_client.await_stable_leader(
                namespace="redpanda", topic="controller", partition=0
            )
        elif any_restarts:
            raise AssertionError(
                "Nodes report restart required but expect_restart is False"
            )

    def set_feature_active(
        self, feature_name: str, active: bool, *, timeout_sec: int = 15
    ):
        target_state = "active" if active else "disabled"
        cur_state = self.get_feature_state(feature_name)
        if active and cur_state == "unavailable":
            # If we have just restarted after an upgrade, wait for cluster version
            # to progress and for the feature to become available.
            self.await_feature(feature_name, "available", timeout_sec=timeout_sec)
        self._admin.put_feature(feature_name, {"state": target_state})
        self.await_feature(feature_name, target_state, timeout_sec=timeout_sec)

    def get_feature_state(self, feature_name: str, node: ClusterNode | None = None):
        f = self._admin.get_features(node=node)
        by_name = dict((f["name"], f) for f in f["features"])
        try:
            state = by_name[feature_name]["state"]
        except KeyError:
            state = None
        return state

    def await_feature(
        self,
        feature_name: str,
        await_state: str,
        *,
        timeout_sec: int,
        nodes: list[ClusterNode] | None = None,
    ):
        """
        For use during upgrade tests, when after upgrade yo uwould like to block
        until a particular feature's active status updates (e.g. if it does migrations)
        """
        if nodes is None:
            nodes = self.started_nodes()

        def is_awaited_state():
            for n in nodes:
                state = self.get_feature_state(feature_name, node=n)
                if state != await_state:
                    self.logger.info(
                        f"Feature {feature_name} not yet {await_state} on {n.name} (state {state})"
                    )
                    return False

            self.logger.info(f"Feature {feature_name} is now {await_state}")
            return True

        wait_until(is_awaited_state, timeout_sec=timeout_sec, backoff_sec=1)

    def monitor_log(self, node: ClusterNode):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        return node.account.monitor_log(RedpandaService.STDOUT_STDERR_CAPTURE)

    def raise_on_crash(self, log_allow_list: LogAllowList = ()) -> None:
        """
        Check if any redpanda nodes are unexpectedly not running,
        or if any logs contain segfaults or assertions.

        Call this after a test fails, to generate a more useful
        error message, rather than having failures on "timeouts" which
        are actually redpanda crashes.
        """

        allow_list = prepare_allow_list(log_allow_list)

        def is_allowed_log_line(line: str) -> bool:
            for a in allow_list:
                if a.search(line) is not None:
                    return True
            return False

        # We log long encoded AWS/GCP headers that occasionally have 'SEGV' in
        # them by chance
        cloud_header_strings = ["x-amz-id", "x-amz-request", "x-guploader-uploadid"]

        def scan_node_for_crash(node: ClusterNode) -> tuple[ClusterNode, str] | None:
            self.logger.info(f"Scanning node {node.account.hostname} log for errors...")
            # crashes appear near the "end" of the file, so examine only the last
            # 10 MB, to avoid timeouts on large logs
            for line in node.account.ssh_capture(
                f"tail --bytes=10000000 {RedpandaService.STDOUT_STDERR_CAPTURE} "
                "| grep -e SEGV -e Segmentation\\ fault -e [Aa]ssert -e Sanitizer "
                "-e 'Aborting on shard' -e 'crash reason to crash file' || true",
                timeout_sec=30,
            ):
                if "SEGV" in line and any(
                    [h in line.lower() for h in cloud_header_strings]
                ):
                    continue

                if is_allowed_log_line(line):
                    self.logger.info(f"Ignoring allow-listed log line '{line}'")
                    continue

                if "No such file or directory" not in line:
                    return (node, line)
            return None

        crash_results = self.for_nodes(self.nodes, scan_node_for_crash)
        crashes: list[tuple[ClusterNode, str]] = [
            r for r in crash_results if r is not None
        ]

        if not crashes:
            # Even if there is no assertion or segfault, look for unexpectedly
            # not-running processes
            def check_pid(node: ClusterNode) -> tuple[ClusterNode, str] | None:
                if not self.redpanda_pid(node):
                    return (node, "Redpanda process unexpectedly stopped")
                return None

            pid_results = self.for_nodes(list(self._started), check_pid)
            crashes = [r for r in pid_results if r is not None]

        if crashes:
            if self._tolerate_crashes:
                self.logger.info(
                    f"Detected crashes, but RedpandaService is configured to allow them: {crashes}"
                )
            else:
                raise NodeCrash(crashes)

    def raw_metrics(
        self,
        node: ClusterNode,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        name: str | None = None,
    ):
        assert node in self._started, f"Node {node.account.hostname} is not started"

        url = f"http://{node.account.hostname}:9644/{metrics_endpoint.value}"

        params = (
            {"__name__": self._adjust_metric_name(name, metrics_endpoint)}
            if name
            else None
        )
        start_t = time.time()
        resp = None
        try:
            resp = requests.get(url, timeout=10, params=params)
        finally:
            elapsed = time.time() - start_t
            if resp:
                status = resp.status_code
                bytes_len = len(resp.text)
            else:
                status = "<exception>"
                bytes_len = "n/a"
            self.logger.debug(
                f"raw_metrics duration_sec={elapsed:.3f} endpoint={metrics_endpoint.value} host={node.account.hostname} "
                f"status={status} bytes={bytes_len}"
            )
        assert resp.status_code == 200
        return resp.text

    def metrics(
        self,
        node: ClusterNode | CloudBroker,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
        name: str | None = None,
    ):
        """Parse the prometheus text format metric from a given node."""
        assert isinstance(node, ClusterNode)
        text = self.raw_metrics(node, metrics_endpoint, name)
        return list(text_string_to_metric_families(text))

    def cloud_storage_diagnostics(self):
        """
        When a cloud storage test fails, it is often useful to know what
        the state of the S3 bucket was, and what was in the manifest
        JSON files.

        This function lists the contents of the bucket (up to a key count
        limit) into the ducktape log, and writes a zip file into the ducktape
        results directory containing a sample of the manifest.json files.
        """
        if self._disable_cloud_storage_diagnostics:
            self.logger.debug("Skipping cloud diagnostics, disabled")
            return
        if not self._si_settings:
            self.logger.debug("Skipping cloud diagnostics, no SI settings")
            return

        try:
            self._cloud_storage_diagnostics()
        except Exception:
            # We are running during test teardown, so do log the exception
            # instead of propagating: this was a best effort thing
            self.logger.exception("Failed to gather cloud storage diagnostics")

    def _cloud_storage_diagnostics(self):
        # In case it's a big test, do not exhaustively log every object
        # or dump every manifest
        key_dump_limit = 10000
        manifest_dump_limit = 128

        self.logger.info(
            f"Gathering cloud storage diagnostics in bucket {self.si_settings.cloud_storage_bucket}"
        )

        manifests_to_dump: list[str] = []
        for o in self.cloud_storage_client.list_objects(
            self.si_settings.cloud_storage_bucket
        ):
            key = o.key
            if key_dump_limit > 0:
                self.logger.info(f"  {key} {o.content_length}")
                key_dump_limit -= 1

            # Gather manifest.json and topic_manifest.json files
            if (
                "manifest.json" in key
                or "manifest.bin" in key
                and manifest_dump_limit > 0
            ):
                manifests_to_dump.append(key)
                manifest_dump_limit -= 1

            if manifest_dump_limit == 0 and key_dump_limit == 0:
                break

        service_dir = os.path.join(
            TestContext.results_dir(self._context, self._context.test_index),
            self.service_id,
        )

        if not os.path.isdir(service_dir):
            mkdir_p(service_dir)

        archive_basename = "cloud_diagnostics.zip"
        archive_path = os.path.join(service_dir, archive_basename)

        with zipfile.ZipFile(archive_path, mode="w") as archive:
            for m in manifests_to_dump:
                self.logger.info(f"Fetching manifest {m}")
                body = self.cloud_storage_client.get_object_data(
                    self.si_settings.cloud_storage_bucket, m
                )
                filename = m.replace("/", "_")

                with archive.open(filename, "w") as outstr:
                    outstr.write(body)

                # Decode binary manifests for convenience, but don't give up
                # if we fail
                if "/manifest.bin" in m:
                    try:
                        decoded = RpStorageTool(self.logger).decode_partition_manifest(
                            body
                        )
                    except Exception as e:
                        self.logger.warning(f"Failed to decode {m}: {e}")
                    else:
                        json_filename = f"{filename}_decoded.json"
                        json_bytes = json.dumps(decoded, indent=2)
                        self.logger.info(
                            f"Decoded manifest {m} to {len(json_bytes)} of JSON from {len(body)} bytes of serde"
                        )
                        with archive.open(json_filename, "w") as outstr:
                            outstr.write(json_bytes.encode())

    def raise_on_storage_usage_inconsistency(self):
        def tracked(fstat: tuple[pathlib.Path, int]):
            """
            filter out files at the root of redpanda's data directory. these
            are not included right now in the local storage costs returned by
            the admin api. we may want to update this in the future, but
            paying the cost to look at the files which are generally small,
            and non-reclaimable doesn't seem worth the hassle. however, for
            small experiements they are relatively large, so we need to filter
            them out for the purposes of looking at the accuracy of storage
            usage tracking.

            Example files:
                  DIR cloud_storage_cache
                   26 startup_log
                10685 config_cache.yaml
            """
            file, _ = fstat
            if len(file.parents) == 1:
                return False
            if file.parents[-2].name in ["cloud_storage_cache", "debug-bundle"]:
                return False
            if (
                "compaction.staging" in file.name
                or "compaction.compaction_index" in file.name
                or "compaction_index.staging" in file.name
            ):
                # compaction staging files are temporary and are generally
                # cleaned up after compaction finishes, or at next round of
                # compaction if a file was stranded. during shutdown of any
                # generic test we don't have a good opportunity to force this to
                # happen without placing a lot of restrictions on shutdown. for
                # the time being just ignore these.
                return False
            if file.suffix == ".cannotrecover":
                # Unrecoverable segments aren't included in the disk usage report.
                # Since we don't remove them automatically and they don't get
                # cleaned up automatically, we can ignore them here for now.
                return False
            return True

        @dataclass
        class StorageInspectionResult:
            """Result of inspecting storage usage on a node."""

            diff_ratio: float  # Absolute difference ratio between observed and reported
            reclaimable_ratio: float  # Ratio of storage reclaimable by retention
            reported: dict[str, int]  # Reported storage usage by category
            reported_total: int  # Total reported storage usage
            observed: list[tuple[pathlib.Path, int]]  # List of (path, size) tuples
            observed_total: int  # Total observed storage usage

            def should_retry(self) -> bool:
                return self.diff_ratio > 0.05 + self.reclaimable_ratio

        def inspect_node(node: ClusterNode) -> StorageInspectionResult:
            """
            Fetch reported size from admin interface, query the local file
            system, and compute a percentage difference between reported and
            observed disk usage.
            """
            try:
                observed = list(self.data_stat(node))
                reported = self._admin.get_local_storage_usage(node)

                observed_total = sum(s for _, s in filter(tracked, observed))
                reported_total = (
                    reported["data"] + reported["index"] + reported["compaction"]
                )

                diff = observed_total - reported_total
                return StorageInspectionResult(
                    diff_ratio=abs(diff / reported_total),
                    reclaimable_ratio=reported["reclaimable_by_retention"]
                    / reported_total,
                    reported=reported,
                    reported_total=reported_total,
                    observed=observed,
                    observed_total=observed_total,
                )
            except Exception:
                return StorageInspectionResult(
                    diff_ratio=0.0,
                    reclaimable_ratio=0.0,
                    reported={},
                    reported_total=0,
                    observed=[],
                    observed_total=0,
                )

        # inspect the node and check that we fall below a 5% + reclaimabled_by_retention%
        # threshold difference. at this point the test is over, but we allow for a couple
        # retries in case things need to settle.

        def inspect_nodes(nodes_: list[ClusterNode]):
            return self.for_nodes(nodes_, lambda n: (n, inspect_node(n)))

        retries = [r for r in inspect_nodes(self.nodes) if r[1].should_retry()]

        for _ in range(3):
            results = inspect_nodes([node for node, _ in retries])
            retries = [r for r in results if r[1].should_retry()]

            if not retries:
                # all good
                return

            time.sleep(5)

        # if one or more nodes failed the check, then report information about
        # the situation and fail the test by raising an exception.
        node_names: list[str] = []
        max_node, max_diff = retries[0][0], retries[0][1].diff_ratio
        for node, result in retries:
            node_name = f"{self.idx(node)}:{node.account.hostname}"
            node_names.append(node_name)
            if result.diff_ratio > max_diff:
                max_diff = result.diff_ratio
                max_node = node
            diff = result.observed_total - result.reported_total
            for file, size in result.observed:
                self.logger.debug(f"Observed file [{node_name}]: {size:12} {file}")
            for key, value in result.reported.items():
                self.logger.debug(f"Reported [{node_name}]: {key}={value:12}")
            self.logger.warning(
                f"Storage usage [{node_name}]: obs {result.observed_total:12} rep {result.reported_total:12} diff {diff:12} pct {result.diff_ratio} reclaimable_pct {result.reclaimable_ratio}"
            )

        max_node_name = f"{self.idx(max_node)}:{max_node.account.hostname}"
        raise RuntimeError(
            f"Storage usage inconsistency on nodes {node_names}: max difference {max_diff} on node {max_node_name}"
        )

    def decode_backtraces(self, raise_on_failure: bool = False):
        """
        Decodes redpanda backtraces if any of them are present
        :return: None
        """

        def decode_node_backtraces(node: ClusterNode) -> Exception | None:
            if not node.account.exists(RedpandaService.STDOUT_STDERR_CAPTURE):
                # Log may not exist if node never started
                return None

            self.logger.info(f"Decoding backtraces on {node.account.hostname}.")
            cmd = "/opt/scripts/seastar-addr2line"
            cmd += " -a /opt/llvm/llvm-addr2line"
            cmd += f" -e {self.find_raw_binary('redpanda')}"
            cmd += f" -f {RedpandaService.STDOUT_STDERR_CAPTURE}"
            cmd += f" > {RedpandaService.BACKTRACE_CAPTURE} 2>&1"
            cmd += (
                f" && find {RedpandaService.BACKTRACE_CAPTURE} -type f -size 0 -delete"
            )

            try:
                node.account.ssh(cmd)
            except Exception as e:
                # We run during teardown on failures, so if something
                # goes wrong we must not raise, or we would usurp
                # the original exception that caused the failure.
                self.logger.exception("Failed to run seastar-addr2line")
                return e
            return None

        errors = [
            e
            for e in self.for_nodes(self.nodes, decode_node_backtraces)
            if e is not None
        ]
        if raise_on_failure and errors:
            raise errors[0]

    def rp_install_path(self):
        if self._installer._started:
            # The installer sets up binaries to always use /opt/redpanda.
            return "/opt/redpanda"
        return self._context.globals["rp_install_path_root"]

    def find_binary(self, name: str) -> str:
        rp_install_path_root = self.rp_install_path()
        return f"{rp_install_path_root}/bin/{name}"

    def find_raw_binary(self, name: str):
        """
        Like `find_binary`, but find the underlying executable rather tha
        a shell wrapper.
        """
        rp_install_path_root = self.rp_install_path()
        return f"{rp_install_path_root}/libexec/{name}"

    def get_version(self, node: ClusterNode) -> str:
        """
        Returns the redpanda binary version as a string.
        """
        env_preamble = self.redpanda_env_preamble()
        version_cmd = f"{env_preamble} {self.find_binary('redpanda')} --version"
        VERSION_LINE_RE = re.compile(".*(v\\d+\\.\\d+\\.\\d+).*")
        # NOTE: not all versions of Redpanda support the --version field, even
        # though they print out the version.
        version_lines = [
            l
            for l in node.account.ssh_capture(
                version_cmd, allow_fail=True, timeout_sec=10
            )
            if VERSION_LINE_RE.match(l)
        ]
        assert len(version_lines) == 1, version_lines
        return VERSION_LINE_RE.findall(version_lines[0])[0]

    def get_version_int_tuple(self, node: ClusterNode):
        version_str = self.get_version(node)
        return ri_int_tuple(RI_VERSION_RE.findall(version_str)[0])

    def get_version_if_not_head(self, node: ClusterNode) -> str | None:
        """
        Returns the redpanda binary version as a string if it differs from HEAD.
        I.e., if this node is running a previous version of redpanda.
        """
        cur_ver = self._installer.installed_version(node)
        if cur_ver != RedpandaInstaller.HEAD:
            return self.get_version(node)
        return None

    def export_cluster_config(self) -> None:
        """
        Export the cluster configuration of all nodes to the ducktape log
        directory for later inspection.
        """
        self.logger.info("%s: exporting cluster config" % self.who_am_i())

        service_dir = os.path.join(
            TestContext.results_dir(self._context, self._context.test_index),
            self.service_id,
        )
        cluster_config_filename = os.path.join(service_dir, "cluster_config.yaml")
        self.logger.debug(
            "%s: cluster_config_filename %s"
            % (self.who_am_i(), cluster_config_filename)
        )

        if not os.path.isdir(service_dir):
            mkdir_p(service_dir)

        try:
            rpk = RpkTool(self)
            rpk.cluster_config_export(cluster_config_filename, True)
        except Exception as e:
            # Configuration is optional: if redpanda has e.g. crashed, you
            # will not be able to get it from the admin API
            self.logger.info(f"{self.who_am_i()}: error getting config: {e}")

    def stop(self, **kwargs: Any) -> None:
        """
        Override default stop() to execude stop_node in parallel
        """
        self._stop_time = time.time()  # The last time stop is invoked

        self.logger.info("%s: stopping service" % self.who_am_i())

        self.for_nodes(self.nodes, lambda n: self.stop_node(n, **kwargs))

        self._stop_duration_seconds = time.time() - self._stop_time

    def _set_trace_loggers_and_sleep(self, node: ClusterNode, time_sec: int = 10):
        """
        For debugging issues around stopping processes: set the log level to
        trace on all loggers.
        """
        # These tend to be exceptionally chatty, or don't provide much value.
        keep_existing = ["exception", "io", "seastar_memory", "assert"]
        try:
            loggers = self._admin.get_loggers(node)
            for logger in loggers:
                if logger in keep_existing:
                    continue
                self._admin.set_log_level(logger, "trace", time_sec)
            time.sleep(time_sec)
        except Exception as e:
            self.logger.warning(f"Error setting trace loggers: {e}")

    def _update_usage_stats(self, node: ClusterNode):
        if node not in self._started:
            return

        metrics = {
            "vectorized_io_queue_total_read_bytes_total": "disk_bytes_read",
            "vectorized_io_queue_total_write_bytes_total": "disk_bytes_written",
            "vectorized_storage_log_batches_read": "batches_read",
            "vectorized_storage_log_batches_written": "batches_written",
            "vectorized_internal_rpc_sent_bytes": "internal_rpc_bytes_sent",
            "vectorized_internal_rpc_received_bytes": "internal_rpc_bytes_recv",
            "vectorized_cloud_client_total_uploads": "cloud_storage_puts",
            "vectorized_cloud_client_total_downloads": "cloud_storage_gets",
        }

        try:
            metric_samples = self.metrics_samples(
                list(metrics.keys()),
                [node],
            )

            for key, ms in metric_samples.items():
                current = getattr(self._usage_stats, metrics[key])
                setattr(
                    self._usage_stats,
                    metrics[key],
                    current + int(sum(s.value for s in ms.samples)),
                )
        except Exception as e:
            self.logger.warning(f"Cannot check metrics on shutdown - {e}")

    def stop_node(
        self,
        node: ClusterNode,
        timeout: float | None = None,
        forced: bool = False,
        **kwargs: Any,
    ):
        assert not kwargs, f"Unknown args {kwargs}"
        # collect usage stats before the node is stopped, the usage stats
        # accumulate metrics from all the nodes before they are stopped.
        self._update_usage_stats(node)
        # Assume node is stopped once we enter this path. If stopping succeeds
        # it is the obvious thing to do. If stopping fails we can't differentiate
        # between a node that stopped or will eventually stop so for all intents
        # and purposes we consider it stopped not to trip other logic that expects
        # started to contain nodes that _must_ be running. E.g. crash detection
        # at end of test which iterates through "started nodes".
        self.remove_from_started_nodes(node, "stop_node")

        pid = self.redpanda_pid(node)

        if pid is None:
            return

        self.logger.info(f"{node.name}: Stopping redpanda (pid {pid})")

        node.account.signal(
            pid, signal.SIGKILL if forced else signal.SIGTERM, allow_fail=False
        )

        stop_timeout = timeout or 30

        @debounce(0.5)
        def debounced_log_process_status():
            self._log_process_status(node, pid)

        def check_redpanda_process_stopped():
            is_stopped = self.redpanda_pid(node) is None
            if not is_stopped:
                self.logger.debug(
                    f"{node.name}: Redpanda process (pid {pid}) still running."
                )
                debounced_log_process_status()

            return is_stopped

        try:
            wait_until(
                check_redpanda_process_stopped,
                timeout_sec=stop_timeout,
                err_msg=f"Redpanda node {node.account.hostname} failed to stop in {stop_timeout} seconds",
            )

            self.logger.info(f"{node.name}: Redpanda process has exited.")
        except TimeoutError:
            sleep_sec = 10
            self.logger.warning(
                f"Timed out waiting for stop on {node.name}, setting log_level to 'trace' and sleeping for {sleep_sec}s"
            )
            self._set_trace_loggers_and_sleep(node, time_sec=sleep_sec)
            self.logger.warning(f"Node {node.name} status:")
            self._log_node_process_state(node)
            self._log_process_status(node, pid)
            self._log_node_shutdown_analysis(node)
            # Kill the process if it's still running. If redpanda still runs we
            # might fail to collect logs as the file will be modified while we
            # (ducktape) are reading/compressing it.
            # I.e. `tar: redpanda.log: file changed as we read it`
            node.account.signal(pid, signal.SIGKILL, allow_fail=True)
            raise

    def remove_from_started_nodes(self, node: ClusterNode, reason: str = "unknown"):
        if node in self._started:
            self.logger.debug(
                f"Removed {node.name} from started nodes, reason: {reason}"
            )
            self._started.remove(node)

    def add_to_started_nodes(self, node: ClusterNode):
        self.logger.debug(f"Added {node.name} to started nodes")
        self._started.add(node)

    def clean(self, **kwargs: Any):
        super().clean(**kwargs)
        # If we bypassed bucket creation, there is no need to try to delete it.
        if self._si_settings and self._si_settings.bypass_bucket_creation:
            self.logger.info(
                f"Skipping deletion of bucket/container: {self.si_settings.cloud_storage_bucket},"
                "because its creation was bypassed."
            )
            return
        if self._cloud_storage_client:
            try:
                self.delete_bucket_from_si()
            except Exception as e:
                self.logger.error(
                    f"Failed to remove bucket {self.si_settings.cloud_storage_bucket}."
                    f" This may cause running out of quota in the cloud env. Please investigate: {e}"
                )

                raise e

    def clean_node(
        self,
        node: ClusterNode,
        preserve_logs: bool = False,
        preserve_current_install: bool = False,
        **kwargs: Any,
    ):
        assert not kwargs, f"Unknown args {kwargs}"
        # These are allow_fail=True to allow for a race where kill_process finds
        # the PID, but then the process has died before it sends the SIGKILL.  This
        # should be safe against actual failures to of the process to stop, because
        # we're using SIGKILL which does not require the process's cooperation.
        node.account.kill_process("redpanda", clean_shutdown=False, allow_fail=True)
        if node.account.exists(RedpandaService.PERSISTENT_ROOT):
            hidden_cache_folder = f"{RedpandaService.PERSISTENT_ROOT}/.cache"
            self.logger.debug(f"Checking for presence of {hidden_cache_folder}")
            if node.account.exists(hidden_cache_folder):
                self.logger.debug(
                    f"Seeing {hidden_cache_folder}, removing that specifically first"
                )
                node.account.remove(hidden_cache_folder)
            if node.account.sftp_client.listdir(RedpandaService.PERSISTENT_ROOT):
                if not preserve_logs:
                    node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/*")
                else:
                    node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/data/*")
        if node.account.exists(RedpandaService.NODE_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.NODE_CONFIG_FILE}")
        if node.account.exists(RedpandaService.RPK_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.RPK_CONFIG_FILE}")
        if node.account.exists(RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE}")
        if not preserve_logs and node.account.exists(self.EXECUTABLE_SAVE_PATH):
            node.account.remove(self.EXECUTABLE_SAVE_PATH)

        if node.account.exists(RedpandaService.SYSTEM_TLS_CA_CRT_FILE):
            node.account.remove(RedpandaService.SYSTEM_TLS_CA_CRT_FILE)
            node.account.ssh("update-ca-certificates")

        if node.account.exists(RedpandaService.TEMP_OSSL_CONFIG_FILE):
            node.account.remove(RedpandaService.TEMP_OSSL_CONFIG_FILE)

        if not preserve_current_install or not self._installer._started:
            # Reset the binaries to use the original binaries.
            # NOTE: if the installer hasn't been started, there is no
            # installation to preserve!
            self._installer.reset_current_install([node])

        self.clear_cached_broker_metadata(node)

    def remove_local_data(self, node: ClusterNode):
        node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/data/*")
        # clear the cached metadata since Redpanda often comes up with new
        # broker IDs, so the old broker IDs are invalid
        self.clear_cached_broker_metadata(node)

    def clear_cached_broker_metadata(self, node: ClusterNode):
        """Roughly speaking, this clears any internal metadata about the given node, so that
        the data is queried freshly from the node when it is needed. It should be
        called when an operation that would invalidate this cached metadata is performed."""

        # clear cached index -> id map: this could be invalided by changing the cluster shape
        self.logger.debug(f"Clearing cached broker metadata for {node.name}")
        index = self.idx(node)
        assert index > 0
        if index in self._node_id_by_idx:
            del self._node_id_by_idx[index]

    def redpanda_pid(self, node: ClusterNode):
        try:
            cmd = "pgrep --list-full --exact redpanda"
            for line in node.account.ssh_capture(cmd, timeout_sec=10):
                # Ignore SSH commands that lookup the version of redpanda
                # by running `redpanda --version` like in `self.get_version(node)`
                if "--version" in line:
                    continue

                self.logger.debug(f"{node.name}: pgrep output: {line}")

                # The pid is listed first, that's all we need
                return int(line.split()[0])
            return None
        except RemoteCommandError as e:
            # 1 - No processes matched or none of them could be signalled.
            if e.exit_status == 1:
                self.logger.debug(f"{node.name}: redpanda process not found")
                return None

            self.logger.error(f"{node.name}: error checking redpanda pid: {e}")

            raise e

    def redpanda_tid(self, node: ClusterNode, thread: str):
        """Return the thread group ID and thread ID of the given thread"""
        cmd = "ps -C redpanda -T"
        for line in node.account.ssh_capture(cmd, timeout_sec=10):
            # Example line:
            #     PID    SPID TTY          TIME CMD
            # 2662879 2662879 pts/16   00:00:02 redpanda
            self.logger.debug(f"ps output: {line}")
            parts = line.split()
            thread_name = parts[4]
            if thread == thread_name:
                return int(parts[0]), int(parts[1])
        raise RuntimeError(f"Thread {thread} not found in ps output")

    def started_nodes(self) -> List[ClusterNode]:
        return list(self._started)

    def render(self, path: str, **kwargs: Any):
        with self.config_file_lock:
            return super(RedpandaService, self).render(path, **kwargs)

    @staticmethod
    def get_node_fqdn(node: ClusterNode):
        ip = socket.gethostbyname(node.account.hostname)
        hostname = (
            node.account.ssh_output(cmd=f"dig -x {ip} +short", timeout_sec=10)
            .decode("utf-8")
            .split("\n")[0]
            .removesuffix(".")
        )
        fqdn = (
            node.account.ssh_output(cmd=f"host -t A {hostname}", timeout_sec=10)
            .decode("utf-8")
            .split(" ")[0]
        )
        return fqdn

    def write_openssl_config_file(self, node: ClusterNode):
        conf = self.render(
            "openssl.cnf",
            fips_conf_file=os.path.join(
                self.rp_install_path(), "openssl/fipsmodule.cnf"
            ),
        )
        self.logger.debug(
            f"Writing {RedpandaService.TEMP_OSSL_CONFIG_FILE} to {node.name}:\n{conf}"
        )
        node.account.create_file(RedpandaService.TEMP_OSSL_CONFIG_FILE, conf)

    def get_openssl_config_file_path(self) -> str:
        path = os.path.join(self.rp_install_path(), self.OPENSSL_CONFIG_FILE_BASE)
        if self.rp_install_path() != "/opt/redpanda":
            # If we aren't using an 'installed' Redpanda instance, the openssl config file
            # located in the install path will not point to the correct location of the FIPS
            # module config file.  We generate an openssl config file just for this purpose
            # see write_openssl_config_file above
            path = RedpandaService.TEMP_OSSL_CONFIG_FILE

        self.logger.debug(
            f"OpenSSL Config File Path: {path} ({self.rp_install_path()})"
        )
        return path

    def get_openssl_modules_directory(self) -> str:
        path = os.path.join(self.rp_install_path(), self.OPENSSL_MODULES_PATH_BASE)

        self.logger.debug(
            f"OpenSSL Modules Directory: {path} ({self.rp_install_path()})"
        )
        return path

    def write_node_conf_file(
        self,
        node: ClusterNode,
        override_cfg_params: dict[str, Any] | None = None,
        auto_assign_node_id: bool = False,
        omit_seeds_on_idx_one: bool = True,
        node_id_override: int | None = None,
    ):
        """
        Write the node config file for a redpanda node: this is the YAML representation
        of Redpanda's `node_config` class.  Distinct from Redpanda's _cluster_ configuration
        which is written separately.
        """
        node_info = {self.idx(n): n for n in self.nodes}

        include_seed_servers = True
        if node_id_override:
            assert not auto_assign_node_id, (
                "Can not use node id override when auto assigning node ids"
            )
            node_id = node_id_override
        else:
            node_id = self.idx(node)
        if omit_seeds_on_idx_one and node_id == 1:
            include_seed_servers = False

        if auto_assign_node_id:
            # Supply None so it's omitted from the config.
            node_id = None

        # Grab the IP to use it as an alternative listener address, to
        # exercise code paths that deal with multiple listeners
        node_ip = socket.gethostbyname(node.account.hostname)

        # Grab the node's FQDN which is needed for Kerberos name
        # resolution
        fqdn = self.get_node_fqdn(node)

        if self._security.tls_provider and self._rpk_node_config is not None:
            self._rpk_node_config.ca_file = RedpandaService.TLS_CA_CRT_FILE

        conf = self.render(
            "redpanda.yaml",
            node=node,
            data_dir=RedpandaService.DATA_DIR,
            nodes=node_info,
            node_id=node_id,
            include_seed_servers=include_seed_servers,
            seed_servers=self._seed_servers,
            node_ip=node_ip,
            kafka_alternate_port=self.KAFKA_ALTERNATE_PORT,
            kafka_kerberos_port=self.KAFKA_KERBEROS_PORT,
            fqdn=fqdn,
            admin_alternate_port=self.ADMIN_ALTERNATE_PORT,
            pandaproxy_config=self._pandaproxy_config,
            schema_registry_config=self._schema_registry_config,
            audit_log_config=self._audit_log_config,
            superuser=self._superuser,
            sasl_enabled=self.sasl_enabled(),
            endpoint_authn_method=self.endpoint_authn_method(),
            auto_auth=self._security.auto_auth,
            rpk_node_config=self._rpk_node_config,
        )

        def is_fips_capable(node: ClusterNode) -> bool:
            cur_ver = self._installer.installed_version(node)
            return cur_ver == RedpandaInstaller.HEAD or cur_ver >= (24, 2, 1)

        if in_fips_environment() and is_fips_capable(node):
            fips_mode = get_fips_mode().value
            self.logger.info(f"Setting Redpanda to FIPS mode: {fips_mode}")
            doc = yaml.full_load(conf)
            doc["redpanda"].update(
                dict(
                    fips_mode=fips_mode,
                    openssl_config_file=self.get_openssl_config_file_path(),
                    openssl_module_directory=self.get_openssl_modules_directory(),
                )
            )
            conf = yaml.dump(doc)

        if override_cfg_params or node in self._extra_node_conf:
            doc = yaml.full_load(conf)
            doc["redpanda"].update(self._extra_node_conf[node])
            self.logger.debug(
                f"extra_node_conf[{node.name}]: {self._extra_node_conf[node]}"
            )
            if override_cfg_params:
                self.logger.debug(
                    "Setting custom node configuration options: {}".format(
                        override_cfg_params
                    )
                )
                doc["redpanda"].update(override_cfg_params)
            conf = yaml.dump(doc)

        if self._security.tls_provider:
            p12_password = (
                self._security.tls_provider.p12_password(node)
                if self._security.tls_provider.use_pkcs12_file()
                else None
            )
            tls_config = [
                dict(
                    enabled=True,
                    require_client_auth=self.require_client_auth(),
                    name=n,
                    truststore_file=RedpandaService.TLS_CA_CRT_FILE,
                    crl_file=RedpandaService.TLS_CA_CRL_FILE,
                )
                for n in ["dnslistener", "iplistener"]
            ]
            for n in tls_config:
                if p12_password is None:
                    n["cert_file"] = RedpandaService.TLS_SERVER_CRT_FILE
                    n["key_file"] = RedpandaService.TLS_SERVER_KEY_FILE
                else:
                    n["p12_file"] = RedpandaService.TLS_SERVER_P12_FILE
                    n["p12_password"] = p12_password
            doc = yaml.full_load(conf)
            doc["redpanda"].update(dict(kafka_api_tls=tls_config))
            conf = yaml.dump(doc)

        self.logger.info(
            "Writing Redpanda node config file: {}".format(
                RedpandaService.NODE_CONFIG_FILE
            )
        )
        self.logger.debug(conf)
        node.account.create_file(RedpandaService.NODE_CONFIG_FILE, conf)

        self._node_configs[node] = yaml.full_load(conf)

    def find_path_to_rpk(self) -> str:
        return f"{self._context.globals.get('rp_install_path_root', None)}/bin/rpk"

    def write_bootstrap_cluster_config(self):
        conf = copy.deepcopy(self.CLUSTER_CONFIG_DEFAULTS)

        cur_ver = self._installer.installed_version(self.nodes[0])
        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (22, 2, 7):
            # this configuration property was introduced in 22.2, ensure
            # it doesn't appear in older configurations
            del conf["log_segment_size_jitter_percent"]
        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (23, 2, 1):
            # versions prior to 23.2.1 does not support
            # partition_manager_shutdown_watchdog_timeout property
            del conf["partition_manager_shutdown_watchdog_timeout"]
        if self._extra_rp_conf:
            self.logger.debug(
                "Setting custom cluster configuration options: {}".format(
                    self._extra_rp_conf
                )
            )
            conf.update(self._extra_rp_conf)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (24, 2, 1):
            # this configuration property was introduced in 24.2, ensure
            # it doesn't appear in older configurations
            conf.pop("cloud_storage_url_style", None)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (23, 3, 1):
            # this configuration property was introduced in 23.3, ensure
            # it doesn't appear in older configurations
            conf.pop("cloud_storage_enable_scrubbing", None)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (23, 2, 1):
            # this configuration property was introduced in 23.2, ensure
            # it doesn't appear in older configurations
            conf.pop("retention_local_strict", None)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (22, 2, 1):
            # this configuration property was introduced in 22.2.1, ensure
            # it doesn't appear in older configurations
            conf.pop("cloud_storage_credentials_source", None)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (25, 1, 1):
            conf.pop("iceberg_target_lag_ms", None)

        if self._security.enable_sasl:
            self.logger.debug("Enabling SASL in cluster configuration")
            conf.update(dict(enable_sasl=True))
        if self._security.kafka_enable_authorization is not None:
            self.logger.debug(
                f"Setting kafka_enable_authorization: {self._security.kafka_enable_authorization} in cluster configuration"
            )
            conf.update(
                dict(
                    kafka_enable_authorization=self._security.kafka_enable_authorization
                )
            )
        if self._security.sasl_mechanisms is not None:
            self.logger.debug(
                f"Setting sasl_mechanisms: {self._security.sasl_mechanisms} in cluster configuration"
            )
            conf.update(dict(sasl_mechanisms=self._security.sasl_mechanisms))
        if self._security.sasl_mechanisms_overrides is not None:
            self.logger.debug(
                f"Setting sasl_mechanisms_overrides: {self._security.sasl_mechanisms_overrides} in cluster configuration"
            )
            conf.update(
                dict(sasl_mechanisms_overrides=self._security.sasl_mechanisms_overrides)
            )

        if self._security.http_authentication is not None:
            self.logger.debug(
                f"Setting http_authentication: {self._security.http_authentication} in cluster configuration"
            )
            conf.update(dict(http_authentication=self._security.http_authentication))

        # Only override `rpk_path` if not already provided
        if (
            cur_ver == RedpandaInstaller.HEAD or cur_ver >= (24, 3, 1)
        ) and "rpk_path" not in conf:
            # Introduced rpk_path to v24.3
            rpk_path = self.find_path_to_rpk()
            conf.update(dict(rpk_path=rpk_path))

        conf_yaml = yaml.dump(conf)

        def write_config_to_node(node: ClusterNode) -> None:
            self.logger.info(
                "Writing bootstrap cluster config file {}:{}".format(
                    node.name, RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE
                )
            )
            node.account.mkdirs(
                os.path.dirname(RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE)
            )
            node.account.create_file(
                RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE, conf_yaml
            )

        self.for_nodes(self.nodes, write_config_to_node)

    def get_node_by_id(self, node_id: int) -> ClusterNode | None:
        """
        Returns the node that has the requested id or None if node is not found.

        If you expect the node to exist, you may prefer `self.node_by_id(node_id)`,
        which raises an exception if the node is not found.

        This function ignores nodes which are not started (in general it needs to node
        to be up to query its ID).
        """
        for n in self.nodes:
            try:
                if self.node_id(n) == node_id:
                    return n
            except BrokerNotStartedError:
                # skip nodes that are not started (though we may stil obtain
                # their information if it is cached: this exception indicates
                # the the cache was not populated for this node)
                pass

        return None

    def node_by_id(self, node_id: int) -> ClusterNode:
        """
        Returns the node that has requested id or throws NodeNotFoundError
        if not found.
        """
        nid = self.get_node_by_id(node_id)
        if nid is None:
            self.logger.info(
                f"Node with id {node_id} not found, idx_to_id {self._node_id_by_idx}"
            )
            raise NodeNotFoundError(f"Node with id {node_id} not found")
        return nid

    def registered(self, node: ClusterNode):
        """
        Check if a newly added node is fully registered with the cluster, such
        that a kafka metadata request to any node in the cluster will include it.

        We first check the admin API to do a kafka-independent check, and then verify
        that kafka clients see the same thing.
        """
        node_id = self.node_id(node, force_refresh=True)
        self.logger.debug(
            f"registered: checking if broker {node_id} ({node.name}) is registered..."
        )

        # Query all nodes' admin APIs, so that we don't advance during setup until
        # the node is stored in raft0 AND has been replayed on all nodes.  Otherwise
        # a kafka metadata request to the last node to join could return incomplete
        # metadata and cause strange issues within a test.
        for peer in self._started:
            try:
                admin_brokers = self._admin.get_brokers(node=peer)
            except requests.exceptions.RequestException as e:
                # We run during startup, when admin API may not even be listening yet: tolerate
                # API errors but presume that if some APIs are not up yet, then node registration
                # is also not complete.
                self.logger.debug(
                    f"registered: peer {peer.name} admin API not yet available ({e})"
                )
                return False
            found = None
            for b in admin_brokers:
                if b["node_id"] == node_id:
                    found = b
                    break

            if not found:
                self.logger.info(
                    f"registered: node {node.name} not yet found in peer {peer.name}'s broker list ({admin_brokers})"
                )
                return False
            else:
                if not found["is_alive"]:
                    self.logger.info(
                        f"registered: node {node.name} found in {peer.name}'s broker list ({admin_brokers}) but not yet marked as alive"
                    )
                    return False
                self.logger.debug(
                    f"registered: node {node.name} now visible in peer {peer.name}'s broker list ({admin_brokers})"
                )

        if self.brokers():  # Conditional in case Kafka API turned off
            if self.sasl_enabled():
                client = PythonLibrdkafka(
                    self,
                    tls_cert=self._tls_cert,
                    username=self._superuser.username,
                    password=self._superuser.password,
                    algorithm=self._superuser.algorithm,
                )
            else:
                client = PythonLibrdkafka(self, tls_cert=self._tls_cert)
            brokers = client.brokers()
            broker = brokers.get(node_id, None)
            if broker is None:
                # This should never happen, because we already checked via the admin API
                # that the node of interest had become visible to all peers.
                self.logger.error(
                    f"registered: node {node.name} not found in kafka metadata!"
                )
                assert broker is not None

            self.logger.debug(f"registered: found broker info: {broker}")

        return True

    def controller(self):
        """
        :return: the ClusterNode that is currently controller leader, or None if no leader exists
        """
        for node in self.started_nodes():
            try:
                r = requests.request(
                    "get",
                    f"http://{node.account.hostname}:9644/v1/partitions/redpanda/controller/0",
                    timeout=10,
                )
            except requests.exceptions.RequestException:
                continue

            if r.status_code != 200:
                continue
            else:
                resp_leader_id = r.json()["leader_id"]
                if resp_leader_id != -1:
                    return self.get_node_by_id(resp_leader_id)

        return None

    @property
    def cache_dir(self):
        return os.path.join(RedpandaService.DATA_DIR, "cloud_storage_cache")

    def node_storage(
        self,
        node: ClusterNode,
        sizes: bool = False,
        scan_cache: bool = True,
        compaction_footers: bool = False,
    ) -> NodeStorage:
        """
        Retrieve a summary of storage on a node.

        :param sizes: if true, stat each segment file and record its size in the
                      `size` attribute of Segment.
        :param scan_cache: if false, skip scanning the tiered storage cache; use
                           this if you are only interested in raft storage.
        """

        self.logger.debug(f"Starting storage checks for {node.name} sizes={sizes}")
        store = NodeStorage(node.name, RedpandaService.DATA_DIR, self.cache_dir)
        script_path = inject_remote_script(node, "compute_storage.py")
        cmd = ["python3", script_path, f"--data-dir={RedpandaService.DATA_DIR}"]
        if sizes:
            cmd.append("--sizes")
        if compaction_footers:
            cmd.append("--compaction-footers")
        output = node.account.ssh_output(
            shlex.join(cmd), combine_stderr=False, timeout_sec=10
        )
        namespaces = json.loads(output)
        for ns, topics in namespaces.items():
            ns_path = os.path.join(store.data_dir, ns)
            ns = store.add_namespace(ns, ns_path)
            for topic, partitions in topics.items():
                topic_path = os.path.join(ns_path, topic)
                topic = ns.add_topic(topic, topic_path)
                for part, segments in partitions.items():
                    partition_path = os.path.join(topic_path, part)
                    partition = topic.add_partition(part, node, partition_path)
                    partition.add_files(list(segments.keys()))
                    for segment, data in segments.items():
                        if "size" in data:
                            partition.set_segment_size(segment, data["size"])
                        if "compaction_footer" in data:
                            partition.set_segment_compaction_footer(
                                segment, data["compaction_footer"]
                            )

        if (
            scan_cache
            and self._si_settings is not None
            and node.account.exists(store.cache_dir)
        ):
            bytes = int(
                node.account.ssh_output(
                    f'du -s "{store.cache_dir}"', combine_stderr=False
                )
                .strip()
                .split()[0]
            )
            objects = int(
                node.account.ssh_output(
                    f'find "{store.cache_dir}" -type f | wc -l', combine_stderr=False
                ).strip()
            )
            indices = int(
                node.account.ssh_output(
                    f'find "{store.cache_dir}" -type f -name "*.index" | wc -l',
                    combine_stderr=False,
                ).strip()
            )
            store.set_cache_stats(NodeCacheStorage(bytes, objects, indices))

        self.logger.debug(f"Finished storage checks for {node.name} sizes={sizes}")

        return store

    def storage(
        self,
        *,
        nodes: Collection[ClusterNode] | None = None,
        sizes: bool = False,
        scan_cache: bool = True,
    ) -> ClusterStorage:
        """
        :param nodes: if None, only report on started nodes. Otherwise, report
                      on the nodes in the `nodes` list parameter.
        :param sizes: if true, stat each segment file and record its size in the
                      `size` attribute of Segment.
        :param scan_cache: if false, skip scanning the tiered storage cache; use
                           this if you are only interested in raft storage.

        :returns: instances of ClusterStorage
        """
        if nodes is None:
            nodes = self._started
        assert nodes, "Empty node list specified for storage stat collection"

        store = ClusterStorage()
        self.logger.debug(f"Starting storage checks nodes={nodes} sizes={sizes}")

        def compute_node_storage(node: ClusterNode):
            s = self.node_storage(node, sizes=sizes, scan_cache=scan_cache)
            store.add_node(s)

        self.for_nodes(nodes, compute_node_storage)
        self.logger.debug(f"Finished storage checks nodes={nodes} sizes={sizes}")
        return store

    def copy_data(self, dest: str, node: ClusterNode):
        # after copying, move all files up a directory level so the caller does
        # not need to know what the name of the storage directory is.
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.DATA_DIR, d)
            data_dir = os.path.basename(RedpandaService.DATA_DIR)
            data_dir = os.path.join(d, data_dir)
            for fn in os.listdir(data_dir):
                shutil.move(os.path.join(data_dir, fn), dest)

    def data_checksum(self, node: ClusterNode) -> FileToChecksumSize:
        """Run command that computes MD5 hash of every file in redpanda data
        directory. The results of the command are turned into a map from path
        to hash-size tuples."""
        script_path = inject_remote_script(node, "compute_storage.py")
        cmd = f"python3 {script_path} --sizes --md5 --print-flat --data-dir {RedpandaService.DATA_DIR}"
        lines = node.account.ssh_output(cmd, timeout_sec=120)
        lines = lines.decode().split("\n")

        # there is a race between `find` iterating over file names and passing
        # those to an invocation of `md5sum` in which the file may be deleted.
        # here we log these instances for debugging, but otherwise ignore them.
        found: list[str] = []
        for line in lines:
            if "No such file or directory" in line:
                self.logger.debug(f"Skipping file that disappeared: {line}")
                continue
            found.append(line)
        lines = found

        # the `find` command will stick a newline at the end of the results
        # which gets parsed as an empty line by `split` above
        if lines[-1] == "":
            lines.pop()

        return {
            tokens[0]: (tokens[2], int(tokens[1]))
            for tokens in map(lambda l: l.split(), lines)
        }

    def data_stat(self, node: ClusterNode):
        """
        Return a collection of (file path, file size) tuples for all files found
        under the Redpanda data directory. File paths are normalized to be relative
        to the data directory.
        """
        cmd = (
            f"find {RedpandaService.DATA_DIR} -type f -exec stat -c '%n %s' '{{}}' \\;"
        )
        lines = node.account.ssh_output(cmd, timeout_sec=120)
        lines = lines.decode().split("\n")

        # 1. find and stat race. skip any files that were deleted.
        # 2. skip empty lines: find may stick one on the end of the results
        lines = filter(lambda l: "No such file or directory" not in l, lines)
        lines = filter(lambda l: len(l) > 0, lines)

        # split into pathlib.Path / file size pairs
        parts = map(lambda l: l.split(), lines)
        parts = ((pathlib.Path(f), int(s)) for f, s in parts)

        # return results with relative paths
        data_path = pathlib.Path(RedpandaService.DATA_DIR)
        return ((p.relative_to(data_path), s) for p, s in parts)

    def data_dir_usage(self, subdir: str, node: ClusterNode):
        """
        Return a rolled up disk usage report for the given data sub-directory.
        """
        dir = os.path.join(RedpandaService.DATA_DIR, subdir)
        if not node.account.exists(dir):
            return 0
        script_path = inject_remote_script(node, "disk_usage.py")
        cmd = ["python3", script_path, dir]
        return int(node.account.ssh_output(shlex.join(cmd), timeout_sec=10))

    def broker_address(
        self, node: ClusterNode, listener: str = "dnslistener"
    ) -> str | None:
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        assert node in self._started
        cfg = self._node_configs[node]
        if cfg["redpanda"]["kafka_api"]:
            if isinstance(cfg["redpanda"]["kafka_api"], list):
                for entry in cfg["redpanda"]["kafka_api"]:
                    if entry["name"] == listener:
                        return f"{entry['address']}:{entry['port']}"
            else:
                entry = cfg["redpanda"]["kafka_api"]
                return f"{entry['address']}:{entry['port']}"
        else:
            return None

    def admin_endpoint(self, node: ClusterNode):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        return f"{node.account.hostname}:9644"

    def admin_endpoints_list(self):
        brokers = [self.admin_endpoint(n) for n in self._started]
        random.shuffle(brokers)
        return brokers

    def admin_endpoints(self):
        return ",".join(self.admin_endpoints_list())

    def brokers(self, limit: int | None = None, listener: str = "dnslistener") -> str:
        return ",".join(self.brokers_list(limit, listener))

    def brokers_list(
        self, limit: int | None = None, listener: str = "dnslistener"
    ) -> list[str]:
        brokers = [
            self.broker_address(n, listener) for n in list(self._started)[:limit]
        ]
        brokers = [b for b in brokers if b is not None]
        random.shuffle(brokers)
        return brokers

    def schema_reg(self, limit: int | None = None) -> str:
        schema_reg = [
            f"http://{n.account.hostname}:8081" for n in list(self._started)[:limit]
        ]
        return ",".join(schema_reg)

    def shards(self):
        """
        Fetch the max shard id for each node.
        """

        def get_node_shards(node: ClusterNode) -> tuple[int, int]:
            num_shards = 0
            metrics = self.metrics(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_reactor_utilization":
                        num_shards = max(num_shards, int(sample.labels["shard"]))
            assert num_shards > 0
            return (self.idx(node), num_shards)

        return dict(self.for_nodes(list(self._started), get_node_shards))

    def cov_enabled(self):
        cov_option = self._context.globals.get(self.COV_KEY, self.DEFAULT_COV_OPT)
        if cov_option == "ON":
            return True
        elif cov_option == "OFF":
            return False

        self.logger.warning(f"{self.COV_KEY} should be one of 'ON', or 'OFF'")
        return False

    def count_log_node(self, node: ClusterNode, pattern: str):
        accum = 0
        for line in node.account.ssh_capture(
            f'grep "{pattern}" {RedpandaService.STDOUT_STDERR_CAPTURE} || true',
            timeout_sec=60,
        ):
            # We got a match
            self.logger.debug(f"Found {pattern} on node {node.name}: {line}")
            accum += 1

        return accum

    def search_log_node(self, node: ClusterNode, pattern: str):
        for line in node.account.ssh_capture(
            f'grep "{pattern}" {RedpandaService.STDOUT_STDERR_CAPTURE} || true',
            timeout_sec=60,
        ):
            # We got a match
            self.logger.debug(f"Found {pattern} on node {node.name}: {line}")
            return True

        return False

    def search_log_any(self, pattern: str, nodes: list[ClusterNode] | None = None):
        """
        Test helper for grepping the redpanda log.
        The design follows python's built-in any() function.
        https://docs.python.org/3/library/functions.html#any

        :param pattern: the string to search for
        :param nodes: a list of nodes to run grep on
        :return:  true if any instances of `pattern` found
        """
        if nodes is None:
            nodes = self.nodes

        for node in nodes:
            if self.search_log_node(node, pattern):
                return True

        # Fall through, no matches
        return False

    def search_log_all(self, pattern: str, nodes: list[ClusterNode] | None = None):
        # Test helper for grepping the redpanda log
        # The design follows python's  built-in all() function.
        # https://docs.python.org/3/library/functions.html#all

        # :param pattern: the string to search for
        # :param nodes: a list of nodes to run grep on
        # :return:  true if `pattern` is found in all nodes
        if nodes is None:
            nodes = self.nodes

        def search_node(node: ClusterNode) -> bool:
            exit_status = node.account.ssh(
                f'grep "{pattern}" {RedpandaService.STDOUT_STDERR_CAPTURE}',
                allow_fail=True,
            )
            if exit_status != 0:
                self.logger.debug(f"Did not find {pattern} on node {node.name}")
                return False
            return True

        return all(self.for_nodes(nodes, search_node))

    def wait_for_controller_snapshot(
        self,
        node: ClusterNode,
        prev_mtime: float = 0,
        prev_start_offset: int = 0,
        timeout_sec: int = 30,
    ):
        def check():
            snap_path = os.path.join(self.DATA_DIR, "redpanda/controller/0_0/snapshot")
            try:
                stat = node.account.sftp_client.stat(snap_path)
                mtime = stat.st_mtime
                size = stat.st_size
            except FileNotFoundError:
                mtime = 0
                size = 0

            controller_status = self._admin.get_controller_status(node)
            self.logger.info(
                f"node {node.account.hostname}: "
                f"controller status: {controller_status}, "
                f"snapshot size: {size}, mtime: {mtime}"
            )

            so = controller_status["start_offset"]
            return (mtime > prev_mtime and so > prev_start_offset, (mtime, so))

        return wait_until_result(check, timeout_sec=timeout_sec, backoff_sec=1)

    def _get_object_storage_report(
        self, tolerate_empty_object_storage: bool = False, timeout: int = 300
    ) -> tuple[dict[str, Any], CloudStorageUsage]:
        """
        Uses rp-storage-tool to get the object storage report.
        If the cluster is running the tool could see some inconsistencies and report anomalies,
        so the result needs to be interpreted.
        Example of a report:
        {"malformed_manifests":[],
         "malformed_topic_manifests":[]
         "missing_segments":[
           "db0df8df/kafka/test/58_57/0-6-895-1-v1.log.2",
           "5c34266a/kafka/test/52_57/0-6-895-1-v1.log.2"
         ],
         "ntpr_no_manifest":[],
         "ntr_no_topic_manifest":[],
         "segments_outside_manifest":[],
         "unknown_keys":[]}
        """
        vars: dict[str, Any] = {"RUST_LOG": "warn"}
        backend = ""
        if self.si_settings.cloud_storage_type == CloudStorageType.S3:
            backend = "aws"
            vars["AWS_REGION"] = self.si_settings.cloud_storage_region
            if self.si_settings.endpoint_url:
                vars["AWS_ENDPOINT"] = self.si_settings.endpoint_url
                if self.si_settings.endpoint_url.startswith("http://"):
                    vars["AWS_ALLOW_HTTP"] = "true"

            if self.si_settings.cloud_storage_access_key is not None:
                # line below is implied by line above
                assert self.si_settings.cloud_storage_secret_key is not None
                vars["AWS_ACCESS_KEY_ID"] = self.si_settings.cloud_storage_access_key
                vars["AWS_SECRET_ACCESS_KEY"] = (
                    self.si_settings.cloud_storage_secret_key
                )
        elif self.si_settings.cloud_storage_type == CloudStorageType.ABS:
            backend = "azure"
            if (
                self.si_settings.cloud_storage_azure_storage_account
                == SISettings.ABS_AZURITE_ACCOUNT
            ):
                vars["AZURE_STORAGE_USE_EMULATOR"] = "true"
                # We do not use the SISettings.endpoint_url, because that includes the account
                # name in the URL, and the `object_store` crate used in the scanning tool
                # assumes that when the emulator is used, the account name should always
                # appear in the path.
                vars["AZURITE_BLOB_STORAGE_URL"] = (
                    f"http://{AZURITE_HOSTNAME}:{AZURITE_PORT}"
                )
            else:
                vars["AZURE_STORAGE_CONNECTION_STRING"] = cast(
                    ABSClient, self.cloud_storage_client
                ).conn_str
                vars["AZURE_STORAGE_ACCOUNT_KEY"] = (
                    self.si_settings.cloud_storage_azure_shared_key
                )
                vars["AZURE_STORAGE_ACCOUNT_NAME"] = (
                    self.si_settings.cloud_storage_azure_storage_account
                )

        if (
            self.si_settings.endpoint_url
            and self.si_settings.endpoint_url == "https://storage.googleapis.com"
        ):
            backend = "gcp"
        # Pick an arbitrary node to run the scrub from
        node = self.nodes[0]

        bucket = self.si_settings.cloud_storage_bucket
        environment = " ".join(f'{k}="{v}"' for k, v in vars.items())
        usage = CloudStorageUsage()
        for obj in self.get_objects_from_si():
            usage.object_count += 1
            usage.total_bytes_stored += obj.content_length
        bucket_arity = usage.object_count
        effective_timeout = min(max(bucket_arity * 5, 20), timeout)
        self.logger.info(
            f"num objects in the {bucket=}: {bucket_arity}, will apply {effective_timeout=}"
        )
        output, stderr = ssh_output_stderr(
            self,
            node,
            f"{environment} rp-storage-tool --backend {backend} scan-metadata --source {bucket}",
            allow_fail=True,
            timeout_sec=effective_timeout,
        )

        # if stderr contains a WARN logline, log it as DEBUG, since this is mostly related to debugging rp-storage-tool itself
        if re.search(rb"\[\S+ WARN", stderr) is not None:
            self.logger.debug(f"rp-storage-tool stderr output: {stderr}")

        report: dict[str, Any] = {}
        try:
            report = json.loads(output)
        except Exception as exc:
            self.logger.error(f"Error running bucket scrub: {output=} {stderr=}")
            if not tolerate_empty_object_storage:
                raise RuntimeError(
                    f"Failed to json parse report: {output=}, {stderr=}"
                ) from exc
        else:
            self.logger.info(json.dumps(report, indent=2))

        return report, usage

    def raise_on_cloud_storage_inconsistencies(
        self, inconsistencies: list[str], run_timeout: int = 300
    ):
        """
        like stop_and_scrub_object_storage, use rp-storage-tool to explicitly check for inconsistencies,
        but without stopping the cluster.
        """
        report, _ = self._get_object_storage_report(
            tolerate_empty_object_storage=True, timeout=run_timeout
        )
        fatal_anomalies = set(
            k for k, v in report.items() if len(v) > 0 and k in inconsistencies
        )
        if fatal_anomalies:
            self.logger.error(
                f"Found fatal inconsistencies in object storage: {json.dumps(report, indent=2)}"
            )
            raise RuntimeError(
                f"Object storage reports fatal anomalies of type {fatal_anomalies}"
            )

    def stop_and_scrub_object_storage(
        self, run_timeout: int = 300
    ) -> CloudStorageUsage:
        # Before stopping, ensure that all tiered storage partitions
        # have uploaded at least a manifest: we do not require that they
        # have uploaded until the head of their log, just that they have
        # some metadata to validate, so that we will not experience
        # e.g. missing topic manifests.
        #
        # This should not need to wait long: even without waiting for
        # manifest upload interval, partitions should upload their initial
        # manifest as soon as they can, and that's all we require.
        # :param run_timeout timeout for the execution of rp-storage-tool.
        # can be set to None for no timeout

        # We stop because the scrubbing routine would otherwise interpret
        # ongoing uploads as inconsistency.  In future, we may replace this
        # stop with a flush, when Redpanda gets an admin API for explicitly
        # flushing data to remote storage.
        self.wait_for_manifest_uploads()
        self.stop()

        scrub_timeout = max(run_timeout, self.cloud_storage_scrub_timeout_s)
        report, usage = self._get_object_storage_report(timeout=scrub_timeout)

        # It is legal for tiered storage to leak objects under
        # certain circumstances: this will remain the case until
        # we implement background scrub + modify test code to
        # insist on Redpanda completing its own scrub before
        # we externally validate
        # (https://github.com/redpanda-data/redpanda/issues/9072)
        # see https://github.com/redpanda-data/redpanda/issues/17502 for ntr_no_topic_manifest, remove it as soon as it's fixed
        permitted_anomalies = {"segments_outside_manifest", "ntr_no_topic_manifest"}

        # Whether any anomalies were found
        any_anomalies = any(len(v) for v in report["anomalies"].values())

        # List of fatal anomalies found
        fatal_anomalies = set(
            k
            for k, v in report["anomalies"].items()
            if len(v) and k not in permitted_anomalies
        )

        if not any_anomalies:
            self.logger.info("No anomalies in object storage scrub")
        elif not fatal_anomalies:
            self.logger.info(
                f"Non-fatal anomalies in remote storage: {json.dumps(report, indent=2)}"
            )
        else:
            # Tests may declare that they expect some anomalies, e.g. if they
            # intentionally damage the data.
            if self.si_settings.is_damage_expected(fatal_anomalies):
                self.logger.warning(
                    f"Tolerating anomalies in remote storage: {json.dumps(report, indent=2)}"
                )
            else:
                self.logger.error(
                    f"Fatal anomalies in remote storage: {json.dumps(report, indent=2)}"
                )
                raise RuntimeError(
                    f"Object storage scrub detected fatal anomalies of type {fatal_anomalies}"
                )
        return usage

    def maybe_do_internal_scrub(self):
        if not self._si_settings:
            return

        cloud_partitions = self.wait_for_manifest_uploads()
        results = self.wait_for_internal_scrub(cloud_partitions)

        if results:
            self.logger.error(
                "Fatal anomalies reported by internal scrub: "
                f"{json.dumps(results, indent=2)}"
            )
            raise RuntimeError(
                f"Internal object storage scrub detected fatal anomalies: {results}"
            )
        else:
            self.logger.info("No anomalies in internal object storage scrub")

    def validate_metastore(
        self,
        check_object_metadata: bool = True,
        check_object_storage: bool = True,
        max_extents_per_call: int = 1000,
    ):
        """
        Validate metastore state for all cloud topic partitions.

        Discovers cloud topics and calls ValidatePartition for each cloud topic
        partition, paginating to bound the work per call.
        """
        if not self.started_nodes():
            return

        admin = AdminV2(self)
        all_anomalies: list[str] = []
        max_retries = 5

        def validate_topic(topic: metastore_pb.CloudTopicInfo):
            for pid in range(topic.partition_count):
                resume: int | None = None
                total_extents = 0
                retries = 0
                while True:
                    req = metastore_pb.ValidatePartitionRequest(
                        topic_id=topic.topic_id,
                        partition_id=pid,
                        check_object_metadata=check_object_metadata,
                        check_object_storage=check_object_storage,
                        max_extents=max_extents_per_call,
                    )
                    if resume is not None:
                        req.resume_at_offset = resume
                    try:
                        resp = admin.metastore().validate_partition(req=req)
                        retries = 0
                    except ConnectError as e:
                        if (
                            e.code == ConnectErrorCode.UNAVAILABLE
                            and retries < max_retries
                        ):
                            retries += 1
                            self.logger.warning(
                                f"validate_partition unavailable for "
                                f"{topic.topic_name}/{pid}, "
                                f"retry {retries}/{max_retries}..."
                            )
                            time.sleep(1)
                            continue
                        raise
                    total_extents += resp.extents_validated
                    for a in resp.anomalies:
                        all_anomalies.append(
                            f"{topic.topic_name}/{pid}: "
                            f"[{metastore_pb.AnomalyType.Name(a.anomaly_type)}]"
                            f" {a.description}"
                        )
                    if not resp.HasField("resume_at_offset"):
                        break
                    resume = resp.resume_at_offset

                self.logger.debug(
                    f"Validated {total_extents} extents for {topic.topic_name}/{pid}"
                )

        after_name = ""
        while True:
            try:
                list_resp = admin.metastore().list_cloud_topics(
                    req=metastore_pb.ListCloudTopicsRequest(
                        after_topic_name=after_name,
                        max_topics=100,
                    )
                )
            except ConnectError as e:
                if e.code == ConnectErrorCode.UNIMPLEMENTED:
                    self.logger.info(
                        "ListCloudTopics not supported, skipping metastore validation"
                    )
                    return
                raise
            for topic in list_resp.topics:
                validate_topic(topic)
            if not list_resp.has_more:
                break
            if not list_resp.topics:
                self.logger.warning(
                    "ListCloudTopics returned has_more=true with empty topics page"
                )
                break
            after_name = list_resp.topics[-1].topic_name

        if all_anomalies:
            summary = "\n".join(f"  {a}" for a in all_anomalies)
            raise RuntimeError(
                f"Metastore validation failed with {len(all_anomalies)} "
                f"anomalies:\n{summary}"
            )
        self.logger.info("Metastore validation passed")

    def wait_for_manifest_uploads(self) -> set[CloudStoragePartition]:
        cloud_storage_partitions: set[CloudStoragePartition] = set()

        def all_partitions_uploaded_manifest():
            manifest_not_uploaded: list[Partition] = []
            for p in self.partitions():
                try:
                    if p.topic == "__consumer_offsets":
                        # We don't tier this topic, so skip it
                        continue

                    status = self._admin.get_partition_cloud_storage_status(
                        p.topic, p.index, node=p.leader
                    )
                except MaxRetryError as e:
                    self.logger.info(
                        f"Max retries exceeded while fetching cloud partition status: {e}"
                    )
                    return False
                except HTTPError as he:
                    if he.response.status_code == 404:
                        # Old redpanda, doesn't have this endpoint.  We can't
                        # do our upload check.
                        continue
                    else:
                        raise

                remote_write = status["cloud_storage_mode"] in {"full", "write_only"}

                if remote_write:
                    cloud_storage_partitions.add(
                        CloudStoragePartition(
                            topic=p.topic,
                            index=p.index,
                            leader=p.leader,
                            replicas=tuple(p.replicas),
                        )
                    )

                has_uploaded_manifest = (
                    status["metadata_update_pending"] is False
                    or status.get("ms_since_last_manifest_upload", None) is not None
                )
                if remote_write and not has_uploaded_manifest:
                    manifest_not_uploaded.append(p)

            if len(manifest_not_uploaded) != 0:
                self.logger.info(
                    f"Partitions that haven't yet uploaded: {manifest_not_uploaded}"
                )
                return False

            return True

        # If any nodes are up, then we expect to be able to talk to the cluster and
        # check tiered storage status to wait for uploads to complete.
        if self._started:
            # Aggressive retry because almost always this should already be done
            # Each 1000 partititions add 30s of timeout
            n_partitions = len(self.partitions())
            timeout = 30 if n_partitions < 1000 else (n_partitions / 1000) * 30
            wait_until(
                all_partitions_uploaded_manifest,
                timeout_sec=30 + timeout,
                backoff_sec=1,
            )

        return cloud_storage_partitions

    def wait_for_internal_scrub(
        self, cloud_storage_partitions: set[CloudStoragePartition]
    ):
        """
        Configure the scrubber such that it will run aggresively
        until the entire partition is scrubbed. Once that happens,
        the scrubber will pause due to the `cloud_storage_full_scrub_interval_ms`
        config. Returns the aggregated anomalies for all partitions after applying
        filtering for expected damage.
        """
        if not cloud_storage_partitions:
            return None

        # Set the scrub interval down very low so the scrubs happen "soon"
        # note that because we reset the scrubbing metadata below, the next
        # scrub will effectively be a full scrub (since the last scrubbed
        # offset will be forgotten).
        self.set_cluster_config(
            {
                "cloud_storage_enable_scrubbing": True,
                # these two intervals usually don't matter as the metadata reset
                # forces an immediate scrub, but sometimes due to leadership
                # transfer we end up waiting for the next full scrub cycle,
                # see CORE-14424
                "cloud_storage_partial_scrub_interval_ms": 100,
                "cloud_storage_full_scrub_interval_ms": 10 * 1000,
                "cloud_storage_scrubbing_interval_jitter_ms": 100,
                "cloud_storage_background_jobs_quota": 5000,
                "cloud_storage_housekeeping_interval_ms": 100,
                # Segment merging may resolve gaps in the log, so disable it
                "cloud_storage_enable_segment_merging": False,
                # Leadership moves may perturb the scrub, so disable it to
                # streamline the actions below.
                "enable_leader_balancer": False,
            },
            tolerate_stopped_nodes=True,
        )

        unavailable: set[CloudStoragePartition] = set()
        for p in cloud_storage_partitions:
            try:
                leader_id = self._admin.await_stable_leader(
                    topic=p.topic, partition=p.index
                )

                self._admin.reset_scrubbing_metadata(
                    namespace="kafka",
                    topic=p.topic,
                    partition=p.index,
                    node=self.get_node_by_id(leader_id),
                )
            except HTTPError as he:
                if he.response.status_code == 404:
                    # Old redpanda, doesn't have this endpoint.  We can't
                    # do our upload check.
                    unavailable.add(p)
                    continue
                else:
                    raise

        cloud_storage_partitions -= unavailable
        scrubbed: set[CloudStoragePartition] = set()
        all_anomalies: list[dict[str, Any]] = []

        allowed_keys = set(
            ["ns", "topic", "partition", "revision_id", "last_complete_scrub_at"]
        )

        expected_damage = self.si_settings.get_expected_damage()

        def filter_anomalies(detected: dict[str, Any]):
            bad_delta_types = set(
                ["non_monotonical_delta", "mising_delta", "end_delta_smaller"]
            )

            for anomaly_type in expected_damage:
                if anomaly_type == "ntpr_no_manifest":
                    detected.pop("missing_partition_manifest", None)
                if anomaly_type == "missing_segments":
                    detected.pop("missing_segments", None)
                if anomaly_type == "missing_spillover_manifests":
                    detected.pop("missing_spillover_manifests", None)
                if anomaly_type == "ntpr_bad_deltas":
                    if metas := detected.get("segment_metadata_anomalies"):
                        metas = [m for m in metas if m["type"] not in bad_delta_types]
                        if metas:
                            detected["segment_metadata_anomalies"] = metas
                        else:
                            detected.pop("segment_metadata_anomalies", None)
                if anomaly_type == "ntpr_overlap_offsets":
                    if metas := detected.get("segment_metadata_anomalies"):
                        metas = [m for m in metas if m["type"] != "offset_overlap"]
                        if metas:
                            detected["segment_metadata_anomalies"] = metas
                        else:
                            detected.pop("segment_metadata_anomalies", None)
                if anomaly_type == "metadata_offset_gaps":
                    if metas := detected.get("segment_metadata_anomalies"):
                        metas = [m for m in metas if m["type"] != "offset_gap"]
                        if metas:
                            detected["segment_metadata_anomalies"] = metas
                        else:
                            detected.pop("segment_metadata_anomalies", None)

        def all_partitions_scrubbed():
            waiting_for = cloud_storage_partitions - scrubbed
            self.logger.info(
                f"Waiting for {len(waiting_for)} partitions to be scrubbed"
            )
            for p in waiting_for:
                result = self._admin.get_cloud_storage_anomalies(
                    namespace="kafka", topic=p.topic, partition=p.index
                )
                if "last_complete_scrub_at" in result:
                    scrubbed.add(p)

                    filter_anomalies(result)
                    if set(result.keys()) != allowed_keys:
                        all_anomalies.append(result)

            return len(waiting_for) == 0

        n_partitions = len(cloud_storage_partitions)
        timeout = (n_partitions // 100) * 60 + 120
        wait_until(
            all_partitions_scrubbed,
            timeout_sec=timeout,
            backoff_sec=5,
            retry_on_exc=True,
        )

        return all_anomalies

    def set_expected_controller_records(self, max_records: int | None):
        self._expect_max_controller_records = max_records

    def set_up_failure_injection(
        self,
        finject_cfg: FailureInjectionConfig,
        enabled: bool,
        nodes: list[ClusterNode],
        tolerate_crashes: bool,
    ):
        """
        Deploy the given failure injection configuration to the
        requested nodes. Should be called before
        `RedpandaService.start`.
        """
        tmp_file = "/tmp/failure_injection_config.json"
        finject_cfg.write_to_file(tmp_file)

        def setup_node(node: ClusterNode) -> None:
            node.account.mkdirs(
                os.path.dirname(RedpandaService.FAILURE_INJECTION_CONFIG_PATH)
            )
            node.account.copy_to(
                tmp_file, RedpandaService.FAILURE_INJECTION_CONFIG_PATH
            )
            self._extra_node_conf[node].update(
                {
                    "storage_failure_injection_enabled": enabled,
                    "storage_failure_injection_config_path": RedpandaService.FAILURE_INJECTION_CONFIG_PATH,
                }
            )

        self.for_nodes(nodes, setup_node)

        # Disable segment size jitter in order to get more deterministic
        # failure injection.
        self.add_extra_rp_conf({"log_segment_size_jitter_percent": 0})

        # This flag prevents RedpandaService from asserting out when it
        # detects that a Redpanda node has crashed. See
        # `RedpandaService.wait_until`.
        self._failure_injection_enabled = True

        self._tolerate_crashes = tolerate_crashes

        self._log_config.enable_finject_logging()

        self.logger.info(f"Set up failure injection config for nodes: {nodes}")

    def set_tolerate_crashes(self, tolerate_crashes: bool):
        """
        Do not fail test on crashes including asserts. This is useful when
        running with redpanda's fault injection enabled.
        """
        self._tolerate_crashes = tolerate_crashes

    def validate_controller_log(self):
        """
        This method is for use at end of tests, to detect issues that might
        lead to huge numbers of writes to the controller log (e.g. rogue
        loops/retries).

        Any test that intentionally
        """

        def get_node_length(node: ClusterNode) -> int | None:
            try:
                status = self._admin.get_controller_status(node=node)
                return status["committed_index"] - max(0, status["start_offset"] - 1)
            except Exception as e:
                self.logger.warning(
                    f"Failed to read controller status from {node.name}: {e}"
                )
                return None

        lengths = [
            l
            for l in self.for_nodes(list(self.started_nodes()), get_node_length)
            if l is not None
        ]
        max_length = max(lengths) if lengths else None

        if max_length is None:
            self.logger.warning(
                "Failed to read controller status from any node, cannot validate record count"
            )
            return

        if self._expect_max_controller_records is not None:
            self.logger.debug(
                f"Checking controller record count ({max_length}/{self._expect_max_controller_records})"
            )
            if max_length > self._expect_max_controller_records:
                raise RuntimeError(
                    f"Oversized controller log detected!  {max_length} records"
                )

    def estimate_bytes_written(self) -> float | None:
        try:
            samples = self.metrics_sample(
                "vectorized_io_queue_total_write_bytes_total",
                nodes=self.started_nodes(),
            )
        except Exception as e:
            self.logger.warning(
                f"Cannot check metrics, did a test finish with all nodes down? ({e})"
            )
            return None

        if samples is not None and samples.samples:
            return sum(s.value for s in samples.samples)
        else:
            return None

    def estimate_total_disk_bytes_read(self):
        try:
            samples = self.metrics_sample(
                "vectorized_io_queue_total_read_bytes_total", nodes=self.started_nodes()
            )
        except Exception as e:
            self.logger.warning(
                f"Cannot check metrics, did a test finish with all nodes down? ({e})"
            )
            return None

        if samples is not None and samples.samples:
            return sum(s.value for s in samples.samples)
        else:
            return None

    def wait_node_add_rebalance_finished(
        self,
        new_nodes: list[ClusterNode],
        admin: Admin | None = None,
        min_partitions: int = 5,
        progress_timeout: int = 60,
        timeout: int = 300,
        backoff: int = 2,
    ):
        """Waits until the rebalance triggered by adding new nodes is finished."""

        new_node_names = [n.name for n in new_nodes]

        if admin is None:
            admin = Admin(self)
        started_at = time.monotonic()
        last_reconfiguring: set[str] = set()
        last_bytes_moved = 0
        last_update = started_at

        while True:
            time.sleep(backoff)

            if time.monotonic() - last_update > progress_timeout:
                raise TimeoutError(
                    f"rebalance after adding nodes {new_node_names} "
                    "stopped making progress"
                )

            if time.monotonic() - started_at > timeout:
                raise TimeoutError(
                    f"rebalance after adding nodes {new_node_names} timed out"
                )

            cur_reconfiguring: set[str] = set()
            cur_bytes_moved = 0
            for p in admin.list_reconfigurations():
                cur_reconfiguring.add(f"{p['ns']}/{p['topic']}/{p['partition']}")
                cur_bytes_moved += p["bytes_moved"]

            if (
                cur_reconfiguring != last_reconfiguring
                or cur_bytes_moved != last_bytes_moved
            ):
                last_update = time.monotonic()

            last_reconfiguring = cur_reconfiguring
            last_bytes_moved = cur_bytes_moved

            if len(cur_reconfiguring) > 0:
                continue

            partition_counts = [len(admin.get_partitions(node=n)) for n in new_nodes]
            if any(pc < min_partitions for pc in partition_counts):
                continue

            return

    def install_license(self):
        """Install a sample Enterprise License for testing Enterprise features during upgrades"""
        self.logger.debug("Installing an Enterprise License")
        license = sample_license(assert_exists=True)
        assert license is not None, "License must exist"
        assert self._admin.put_license(license).status_code == 200, (
            "Configuring the Enterprise license failed (required for feature upgrades)"
        )

        def license_observable():
            for node in self.started_nodes():
                license = self._admin.get_license(node)
                if not self._admin.is_sample_license(license):
                    return False
            return True

        self.logger.debug(
            f"Waiting for license to be observable by {len(self.started_nodes())} nodes"
        )
        wait_until(
            license_observable,
            timeout_sec=15,
            backoff_sec=1,
            err_msg="Inserted license not observable in time",
        )
        self.logger.debug("Enterprise License installed successfully")

    def has_license_nag(self):
        return self.search_log_any(self.ENTERPRISE_LICENSE_NAG)


def make_redpanda_service(
    context: TestContext,
    num_brokers: int | None,
    *,
    extra_rp_conf: dict[str, Any] | None = None,
    **kwargs: Any,
) -> RedpandaService:
    """Factory function for instatiating the appropriate RedpandaServiceABC subclass."""

    # https://github.com/redpanda-data/core-internal/issues/1002
    assert not is_redpanda_cloud(context), (
        "make_redpanda_service " + "should not be called in a cloud test context"
    )

    if num_brokers is None:
        # Default to a 3 node cluster if sufficient nodes are available, else
        # a single node cluster.  This is just a default: tests are welcome
        # to override constructor to pass an explicit size.  This logic makes
        # it convenient to mix 3 node and 1 node cases in the same class, by
        # just modifying the @cluster node count per test.
        if context.cluster.available().size() >= 3:
            num_brokers = 3
        else:
            num_brokers = 1

    return RedpandaService(context, num_brokers, extra_rp_conf=extra_rp_conf, **kwargs)


def make_redpanda_cloud_service(
    context: TestContext, *, min_brokers: int | None = None
) -> RedpandaServiceCloud:
    """Create a RedpandaServiceCloud service. This can only be used in a test
    running against Redpanda Cloud or else it will throw."""

    assert is_redpanda_cloud(context), (
        "make_redpanda_cloud_service "
        + "called but not in a cloud context (missing cloud_cluster in globals)"
    )

    cloud_config = context.globals[RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG]

    config_profile_name = get_config_profile_name(cloud_config)

    return RedpandaServiceCloud(
        context,
        config_profile_name=config_profile_name,
        min_brokers=min_brokers if min_brokers else 1,
    )


def make_redpanda_mixed_service(
    context: TestContext, *, min_brokers: int | None = 3
) -> "AnyRedpandaService":
    """Creates either a RedpandaService or RedpandaServiceCloud depending on which
    environemnt we are running in. This allows you to write a so-called 'mixed' test
    which can run against services of different types.

    :param min_brokers: Create or expose a cluster with at least this many brokers.
    None indicates that the caller doens't have any requirement on the number of brokers
    and that the framework may select an appropriate number."""

    # For cloud tests, we can't affect the number of brokers (or any other cluster
    # parameters, really), so we just check (eventually) that the number of brokers
    # is at least the specified minimum (by passing as min_brokers). For vanilla tests,
    # we create a cluster with exactly the requested number of brokers.
    if is_redpanda_cloud(context):
        return make_redpanda_cloud_service(context, min_brokers=min_brokers)
    else:
        return make_redpanda_service(context, num_brokers=min_brokers)


AnyRedpandaService = RedpandaService | RedpandaServiceCloud


class ValidationMode(str, Enum):
    """
    Redpanda validation modes (see cluster config `kafka_produce_batch_validation`).
    """

    LEGACY = "legacy"
    RELAXED = "relaxed"
    STRICT = "strict"
