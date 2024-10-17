# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import subprocess
from abc import ABC, abstractmethod
import concurrent.futures
import copy
from functools import cached_property
from logging import Logger

import time
import os
import socket
import signal
import tempfile
import shutil
import requests
import json
import random
import threading
import collections
import re
import uuid
import zipfile
import pathlib
import shlex
from enum import Enum, IntEnum
from typing import Callable, List, Generator, Literal, Mapping, Optional, Protocol, Set, Tuple, Any, Type, cast

import yaml
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from rptest.archival.s3_client import S3Client, S3AddressingStyle
from rptest.archival.abs_client import ABSClient
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteAccount
from ducktape.cluster.cluster import ClusterNode
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.metrics_core import Metric
from ducktape.errors import TimeoutError
from ducktape.tests.test import TestContext
from rptest.services.rpk_consumer import RpkConsumer

from rptest.clients.helm import HelmTool
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kubectl import KubectlTool, is_redpanda_pod
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.installpack import InstallPackClient
from rptest.clients.rp_storage_tool import RpStorageTool
from rptest.services import redpanda_types, tls
from rptest.services.redpanda_types import KafkaClientSecurity, LogAllowListElem
from rptest.services.admin import Admin
from rptest.services.redpanda_installer import RedpandaInstaller, VERSION_RE as RI_VERSION_RE, RedpandaVersionTriple, int_tuple as ri_int_tuple
from rptest.services.redpanda_cloud import CloudCluster, get_config_profile_name
from rptest.services.cloud_broker import CloudBroker
from rptest.services.rolling_restarter import RollingRestarter
from rptest.services.storage import ClusterStorage, NodeStorage, NodeCacheStorage
from rptest.services.storage_failure_injection import FailureInjectionConfig
from rptest.services.utils import NodeCrash, LogSearchLocal, LogSearchCloud, Stopwatch
from rptest.util import inject_remote_script, ssh_output_stderr, wait_until_result
from rptest.utils.allow_logs_on_predicate import AllowLogsOnPredicate
from rptest.utils.mode_checks import in_fips_environment
from rptest.utils.rpenv import sample_license
import enum

Partition = collections.namedtuple('Partition',
                                   ['topic', 'index', 'leader', 'replicas'])

MetricSample = collections.namedtuple(
    'MetricSample', ['family', 'sample', 'node', 'value', 'labels'])


class CloudStorageCleanupStrategy(enum.Enum):
    # I.e. if we are using a lifecycle rule, then we do NOT clean the bucket
    IF_NOT_USING_LIFECYCLE_RULE = "IF_NOT_USING_LIFECYCLE_RULE"

    # Ignore large buckets (based on number of objects). For small buckets, ALWAYS clean.
    ALWAYS_SMALL_BUCKETS_ONLY = "ALWAYS_SMALL_BUCKETS_ONLY"


SaslCredentials = redpanda_types.SaslCredentials

# Map of path -> (checksum, size)
FileToChecksumSize = dict[str, Tuple[str, int]]

# The endpoint info for the azurite (Azure ABS emulator )container that
# is used when running tests in a docker environment.
AZURITE_HOSTNAME = 'azurite'
AZURITE_PORT = 10000

DEFAULT_LOG_ALLOW_LIST = [
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
    re.compile(r"unexpected REST API error \"Internal Server Error\" detected"
               ),

    # Redpanda upgrade tests will use the default location of rpk (/usr/bin/rpk)
    # which is not present in typical CI runs, which results in the following
    # error message from the debug bundle service
    re.compile(r"Current specified RPK location"),
]

# Log errors that are expected in tests that restart nodes mid-test
RESTART_LOG_ALLOW_LIST = [
    re.compile(
        "(raft|rpc) - .*(disconnected_endpoint|Broken pipe|Connection reset by peer)"
    ),
    re.compile(
        "raft - .*recovery append entries error.*client_request_timeout"),
    # cluster - rm_stm.cc:550 - Error "raft::errc:19" on replicating pid:{producer_identity: id=1, epoch=0} commit batch
    # raft::errc:19 is the shutdown error code, the transaction subsystem encounters this and logs at error level
    re.compile("Error \"raft::errc:19\" on replicating"),
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
        "cluster - .*exception while executing partition operation:.*std::exception \(std::exception\)"
    ),
    # Failure to handle an internal RPC because the RPC server already handles connections but doesn't yet handle this method. This can happen while the node is still starting up/restarting.
    # e.g. "admin_api_server - server.cc:655 - [_anonymous] exception intercepted - url: [http://ip-172-31-9-208:9644/v1/brokers/7/decommission] http_return_status[500] reason - seastar::httpd::server_error_exception (Unexpected error: rpc::errc::method_not_found)"
    re.compile(
        "admin_api_server - .*Unexpected error: rpc::errc::method_not_found")
]

# Log errors emitted by refresh credentials system when cloud storage is enabled with IAM roles
# without a corresponding mock service set up to return credentials
IAM_ROLES_API_CALL_ALLOW_LIST = [
    re.compile(r'cloud_roles - .*api request failed'),
    re.compile(r'cloud_roles - .*Failed to get IMDSv2 token'),
    re.compile(r'cloud_roles - .*failed during IAM credentials refresh:'),
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
    "abs - .*FeatureNotYetSupportedForHierarchicalNamespaceAccounts"
]

AUDIT_LOG_ALLOW_LIST = RESTART_LOG_ALLOW_LIST + [
    re.compile(".*Failed to audit authentication.*"),
    re.compile(".*Failed to append authz event to audit log.*"),
    re.compile(".*Failed to append authentication event to audit log.*"),
    re.compile(".*Failed to audit authorization request for endpoint.*")
]

# Path to the LSAN suppressions file
LSAN_SUPPRESSIONS_FILE = "/opt/lsan_suppressions.txt"

# Path to the UBSAN suppressions file
UBSAN_SUPPRESSIONS_FILE = "/opt/ubsan_suppressions.txt"

FAILURE_INJECTION_LOG_ALLOW_LIST = [
    re.compile(
        "Assert failure: .* filesystem error: Injected Failure: Input/output error"
    ),
    re.compile("assert - Backtrace below:"),
    re.compile("finject - .* flush called concurrently with other operations")
]


class RemoteClusterNode(Protocol):
    account: RemoteAccount

    @property
    def name(self) -> str:
        ...


class MetricSamples:
    def __init__(self, samples: list[MetricSample]):
        self.samples = samples

    def label_filter(self, labels: Mapping[str, float]):
        def f(sample):
            for key, value in labels.items():
                assert key in sample.labels
                return sample.labels[key] == value

        return MetricSamples([s for s in filter(f, self.samples)])


class MetricsEndpoint(Enum):
    METRICS = 'metrics'
    PUBLIC_METRICS = 'public_metrics'


class CloudStorageType(IntEnum):
    # Use (AWS, GCP) S3 compatible API on dedicated nodes, or minio in docker
    S3 = 1
    # Use Azure ABS on dedicated nodes, or azurite in docker
    ABS = 2


CloudStorageTypeAndUrlStyle = Tuple[CloudStorageType, Literal['virtual_host',
                                                              'path']]


def prepare_allow_list(allow_list):
    if allow_list is None:
        allow_list = DEFAULT_LOG_ALLOW_LIST
    else:
        combined_allow_list = DEFAULT_LOG_ALLOW_LIST.copy()
        # Accept either compiled or string regexes
        for a in allow_list:
            if should_compile(a):
                a = re.compile(a)
            combined_allow_list.append(a)
        allow_list = combined_allow_list
    return allow_list


def one_or_many(value):
    """
    Helper for reading `one_or_many_property` configs when
    we only care about getting one value out
    """
    if isinstance(value, list):
        return value[0]
    else:
        return value


def get_cloud_provider() -> str:
    """
    Returns the cloud provider in use.  If one is not set then return 'docker'
    """
    return os.getenv("CLOUD_PROVIDER", "docker")


def get_cloud_storage_type(applies_only_on: list[CloudStorageType]
                           | None = None,
                           docker_use_arbitrary=False):
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
        cloud_storage_type = list(
            set(applies_only_on).intersection(cloud_storage_type))
    return cloud_storage_type


def get_cloud_storage_url_style(
    cloud_storage_type: CloudStorageType
    | None = None
) -> list[Literal['virtual_host', 'path']]:
    if cloud_storage_type is None:
        return ['virtual_host', 'path']

    if cloud_storage_type == CloudStorageType.S3:
        return ['virtual_host', 'path']
    else:
        return ['virtual_host']


def get_cloud_storage_type_and_url_style(
) -> List[CloudStorageTypeAndUrlStyle]:
    """
    Returns a list of compatible cloud storage types and url styles.
    I.e, Returns [(CloudStorageType.S3, 'virtual_host'),
                  (CloudStorageType.S3, 'path'),
                  (CloudStorageType.ABS, 'virtual_host')]
    """
    return [
        tus for tus_list in map(
            lambda t: [(t, us) for us in get_cloud_storage_url_style(t)],
            get_cloud_storage_type()) for tus in tus_list
    ]


def is_redpanda_cloud(context: TestContext):
    """
    Returns True if we are running againt a Redpanda Cloud cluster,
    False otherwise."""

    # we use the presence of a non-empty cloud_cluster key in the config as our
    # global signal that it's a cloud run
    return bool(
        context.globals.get(RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG))


def should_compile(allow_list_element: LogAllowListElem) -> bool:
    return not isinstance(allow_list_element, re.Pattern) and not isinstance(
        allow_list_element, AllowLogsOnPredicate)


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
    # Redpanda's default limit on memory per shard is 1GB
    DEFAULT_MEMORY_MB = 2048

    def __init__(self,
                 *,
                 num_cpus: Optional[int] = None,
                 memory_mb: Optional[int] = None,
                 bypass_fsync: Optional[bool] = None,
                 nfiles: Optional[int] = None,
                 reactor_stall_threshold: Optional[int] = None):
        self._num_cpus = num_cpus
        self._memory_mb = memory_mb

        if bypass_fsync is None:
            self._bypass_fsync = False
        else:
            self._bypass_fsync = bypass_fsync

        self._nfiles = nfiles
        self._reactor_stall_threshold = reactor_stall_threshold

    @property
    def memory_mb(self):
        return self._memory_mb

    @property
    def num_cpus(self):
        return self._num_cpus

    def to_cli(self, *, dedicated_node):
        """

        Generate Redpanda CLI flags based on the settings passed in at construction
        time.

        :return: 2 tuple of strings, first goes before the binary, second goes after it
        """
        preamble = "ulimit -Sc unlimited"
        preamble += f" -Sn {self._nfiles}; " if self._nfiles else "; "

        if self._num_cpus is None and not dedicated_node:
            num_cpus = self.DEFAULT_NUM_CPUS
        else:
            num_cpus = self._num_cpus

        if self._memory_mb is None and not dedicated_node:
            memory_mb = self.DEFAULT_MEMORY_MB
        else:
            memory_mb = self._memory_mb

        if self._bypass_fsync is None and not dedicated_node:
            bypass_fsync = True
        else:
            bypass_fsync = self._bypass_fsync

        args = []
        if not dedicated_node:
            args.extend([
                "--kernel-page-cache=true", "--overprovisioned ",
                "--reserve-memory=0M"
            ])

        if self._reactor_stall_threshold is not None:
            args.append(
                f"--blocked-reactor-notify-ms={self._reactor_stall_threshold}")

        if num_cpus is not None:
            args.append(f"--smp={num_cpus}")
        if memory_mb is not None:
            args.append(f"--memory={memory_mb}M")
        if bypass_fsync is not None:
            args.append(
                f"--unsafe-bypass-fsync={'1' if bypass_fsync else '0'}")

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
    GLOBAL_S3_ACCESS_KEY = "s3_access_key"
    GLOBAL_S3_SECRET_KEY = "s3_secret_key"
    GLOBAL_S3_REGION_KEY = "s3_region"
    GLOBAL_GCP_PROJECT_ID_KEY = "gcp_project_id"
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

    def __init__(self,
                 test_context,
                 *,
                 log_segment_size: int = 16 * 1000000,
                 cloud_storage_cache_chunk_size: Optional[int] = None,
                 cloud_storage_credentials_source: str = 'config_file',
                 cloud_storage_access_key: str = 'panda-user',
                 cloud_storage_secret_key: str = 'panda-secret',
                 cloud_storage_region: str = 'panda-region',
                 cloud_storage_api_endpoint: Optional[str] = None,
                 cloud_storage_api_endpoint_port: int = 9000,
                 cloud_storage_url_style: str = 'virtual_host',
                 cloud_storage_cache_size: Optional[int] = None,
                 cloud_storage_cache_max_objects: Optional[int] = None,
                 cloud_storage_enable_remote_read: bool = True,
                 cloud_storage_enable_remote_write: bool = True,
                 cloud_storage_max_connections: Optional[int] = None,
                 cloud_storage_disable_tls: bool = True,
                 cloud_storage_segment_max_upload_interval_sec: Optional[
                     int] = None,
                 cloud_storage_manifest_max_upload_interval_sec: Optional[
                     int] = None,
                 cloud_storage_readreplica_manifest_sync_timeout_ms: Optional[
                     int] = None,
                 bypass_bucket_creation: bool = False,
                 use_bucket_cleanup_policy: bool = True,
                 cloud_storage_housekeeping_interval_ms: Optional[int] = None,
                 cloud_storage_spillover_manifest_max_segments: Optional[
                     int] = None,
                 fast_uploads=False,
                 retention_local_strict=True,
                 cloud_storage_max_throughput_per_shard: Optional[int] = None,
                 cloud_storage_signature_version: str = "s3v4",
                 before_call_headers: Optional[dict[str, Any]] = None,
                 skip_end_of_test_scrubbing: bool = False,
                 addressing_style: S3AddressingStyle = S3AddressingStyle.PATH):
        """
        :param fast_uploads: if true, set low upload intervals to help tests run
                             quickly when they wait for uploads to complete.
        """

        self._context = test_context
        self.cloud_storage_type = get_cloud_storage_type()[0]
        if hasattr(test_context, 'injected_args') \
        and test_context.injected_args is not None:
            if 'cloud_storage_type' in test_context.injected_args:
                self.cloud_storage_type = cast(
                    CloudStorageType,
                    test_context.injected_args['cloud_storage_type'])
            elif 'cloud_storage_type_and_url_style' in test_context.injected_args:
                self.cloud_storage_type = cast(
                    CloudStorageType, test_context.
                    injected_args['cloud_storage_type_and_url_style'][0])

        # For most cloud storage backends, we clean up small buckets always.
        # In some cases, we will modify this strategy in the block below.
        self.cloud_storage_cleanup_strategy = CloudStorageCleanupStrategy.ALWAYS_SMALL_BUCKETS_ONLY

        if self.cloud_storage_type == CloudStorageType.S3:
            self.cloud_storage_credentials_source = cloud_storage_credentials_source
            self.cloud_storage_access_key = cloud_storage_access_key
            self.cloud_storage_secret_key = cloud_storage_secret_key
            self.cloud_storage_region = cloud_storage_region
            self._cloud_storage_bucket = f'panda-bucket-{uuid.uuid1()}'
            self.cloud_storage_url_style = cloud_storage_url_style
            self.has_cloud_storage_api_endpoint_override = False

            if cloud_storage_api_endpoint is not None:
                self.has_cloud_storage_api_endpoint_override = True
                self.cloud_storage_api_endpoint = cloud_storage_api_endpoint
            elif test_context.globals.get(self.GLOBAL_CLOUD_PROVIDER,
                                          'aws') == 'gcp':
                self.cloud_storage_api_endpoint = 'storage.googleapis.com'
                # For GCP, we currently use S3 compat API over boto3, which does not support batch deletes
                # This makes cleanup slow, even for small-ish buckets.
                # Therefore, we maintain logic of skipping bucket cleaning completely, if we are using lifecycle rule.
                self.cloud_storage_cleanup_strategy = CloudStorageCleanupStrategy.IF_NOT_USING_LIFECYCLE_RULE
            else:
                # Endpoint defaults to minio-s3
                self.cloud_storage_api_endpoint = 'minio-s3'
            self.cloud_storage_api_endpoint_port = cloud_storage_api_endpoint_port

            if hasattr(test_context, 'injected_args') \
            and test_context.injected_args is not None:
                if 'cloud_storage_url_style' in test_context.injected_args:
                    self.cloud_storage_url_style = test_context.injected_args[
                        'cloud_storage_url_style']
                elif 'cloud_storage_type_and_url_style' in test_context.injected_args:
                    self.cloud_storage_url_style = test_context.injected_args[
                        'cloud_storage_type_and_url_style'][1]

        elif self.cloud_storage_type == CloudStorageType.ABS:
            self.cloud_storage_azure_shared_key = self.ABS_AZURITE_KEY
            self.cloud_storage_azure_storage_account = self.ABS_AZURITE_ACCOUNT

            self._cloud_storage_azure_container = f'panda-container-{uuid.uuid1()}'
            self.cloud_storage_api_endpoint = f'{self.cloud_storage_azure_storage_account}.blob.localhost'
            self.cloud_storage_api_endpoint_port = AZURITE_PORT
        else:
            assert False, f"Unexpected value provided for 'cloud_storage_type' injected arg: {self.cloud_storage_type}"

        self.log_segment_size = log_segment_size
        self.cloud_storage_cache_chunk_size = cloud_storage_cache_chunk_size
        self.cloud_storage_cache_size = cloud_storage_cache_size
        self.cloud_storage_cache_max_objects = cloud_storage_cache_max_objects
        self.cloud_storage_enable_remote_read = cloud_storage_enable_remote_read
        self.cloud_storage_enable_remote_write = cloud_storage_enable_remote_write
        self.cloud_storage_max_connections = cloud_storage_max_connections
        self.cloud_storage_disable_tls = cloud_storage_disable_tls
        self.cloud_storage_segment_max_upload_interval_sec = cloud_storage_segment_max_upload_interval_sec
        self.cloud_storage_manifest_max_upload_interval_sec = cloud_storage_manifest_max_upload_interval_sec
        self.cloud_storage_readreplica_manifest_sync_timeout_ms = cloud_storage_readreplica_manifest_sync_timeout_ms
        self.endpoint_url = f'http://{self.cloud_storage_api_endpoint}:{self.cloud_storage_api_endpoint_port}'
        self.bypass_bucket_creation = bypass_bucket_creation
        self.use_bucket_cleanup_policy = use_bucket_cleanup_policy

        self.cloud_storage_housekeeping_interval_ms = cloud_storage_housekeeping_interval_ms
        self.cloud_storage_spillover_manifest_max_segments = cloud_storage_spillover_manifest_max_segments
        self.retention_local_strict = retention_local_strict
        self.cloud_storage_max_throughput_per_shard = cloud_storage_max_throughput_per_shard
        self.cloud_storage_signature_version = cloud_storage_signature_version
        self.before_call_headers = before_call_headers
        self.addressing_style = addressing_style

        # Allow disabling end of test scrubbing.
        # It takes a long time with lots of segments i.e. as created in scale
        # tests. Should figure out how to re-enable it, or consider using
        # redpanda's built-in scrubbing capabilities.
        self.skip_end_of_test_scrubbing = skip_end_of_test_scrubbing

        if fast_uploads:
            self.cloud_storage_segment_max_upload_interval_sec = 10
            self.cloud_storage_manifest_max_upload_interval_sec = 1

        self._expected_damage_types = set()

    def get_use_fips_s3_endpoint(self) -> bool:
        use_fips_option = self._context.globals.get(
            self.GLOBAL_USE_FIPS_S3_ENDPOINT,
            self.DEFAULT_USE_FIPS_S3_ENDPOINT)
        if use_fips_option == "ON":
            return True
        elif use_fips_option == "OFF":
            return False

        self.logger.warn(
            f"{self.GLOBAL_USE_FIPS_S3_ENDPOINT} should be 'ON', or 'OFF'")
        return False

    def use_fips_endpoint(self) -> bool:
        """
        Returns whether or not to use the FIPS S3 endpoints.  Only true if:
        * Cloud provider is AWS, and
        * Cloud storage type is S3, and
        * Either
          * we are in a FIPS environment or,
          * the global variable 'use_fips_s3_endpoint' is 'ON'
        """
        return get_cloud_provider() == "aws" and  \
            self.cloud_storage_type == CloudStorageType.S3 and \
                  (self.get_use_fips_s3_endpoint() or in_fips_environment())

    def load_context(self, logger, test_context):
        if self.cloud_storage_type == CloudStorageType.S3:
            self._load_s3_context(logger, test_context)
        elif self.cloud_storage_type == CloudStorageType.ABS:
            self._load_abs_context(logger, test_context)

    def _load_abs_context(self, logger, test_context):
        storage_account = test_context.globals.get(
            self.GLOBAL_ABS_STORAGE_ACCOUNT, None)
        shared_key = test_context.globals.get(self.GLOBAL_ABS_SHARED_KEY, None)

        if storage_account and shared_key:
            logger.info("Running on Azure, setting credentials from env")
            self.cloud_storage_azure_storage_account = storage_account
            self.cloud_storage_azure_shared_key = shared_key

            self.endpoint_url = None
            self.cloud_storage_disable_tls = False
            self.cloud_storage_api_endpoint_port = 443
        else:
            logger.debug("Running in Dockerised env against Azurite. "
                         "Using Azurite defualt credentials.")

    def _load_s3_context(self, logger, test_context):
        """
        Update based on the test context, to e.g. consume AWS access keys in
        the globals dictionary.
        """
        if self.has_cloud_storage_api_endpoint_override:
            logger.info(
                "cloud_storage_api_endpoint is overridden, skipping loading global test_context options in _load_s3_context."
            )
            return
        cloud_storage_credentials_source = test_context.globals.get(
            self.GLOBAL_CLOUD_STORAGE_CRED_SOURCE_KEY, 'config_file')
        cloud_storage_access_key = test_context.globals.get(
            self.GLOBAL_S3_ACCESS_KEY, None)
        cloud_storage_secret_key = test_context.globals.get(
            self.GLOBAL_S3_SECRET_KEY, None)
        cloud_storage_region = test_context.globals.get(
            self.GLOBAL_S3_REGION_KEY, None)
        cloud_storage_gcp_project_id = test_context.globals.get(
            self.GLOBAL_GCP_PROJECT_ID_KEY, None)

        # Enable S3 if AWS creds were given at globals
        if cloud_storage_credentials_source == 'aws_instance_metadata' or cloud_storage_credentials_source == 'gcp_instance_metadata':
            logger.info("Running on AWS S3, setting IAM roles")
            self.cloud_storage_credentials_source = cloud_storage_credentials_source
            self.cloud_storage_access_key = None
            self.cloud_storage_secret_key = None
            self.endpoint_url = None  # None so boto auto-gens the endpoint url
            if test_context.globals.get(self.GLOBAL_CLOUD_PROVIDER,
                                        'aws') == 'gcp':
                self.endpoint_url = 'https://storage.googleapis.com'
                self.cloud_storage_signature_version = "unsigned"
                self.before_call_headers = {
                    "Authorization": f"Bearer {self.gcp_iam_token(logger)}",
                    "x-goog-project-id": cloud_storage_gcp_project_id
                }
            self.cloud_storage_disable_tls = False  # SI will fail to create archivers if tls is disabled
            self.cloud_storage_region = cloud_storage_region
            self.cloud_storage_api_endpoint_port = 443
            self.addressing_style = S3AddressingStyle.VIRTUAL
        elif cloud_storage_credentials_source == 'config_file' and cloud_storage_access_key and cloud_storage_secret_key:
            # `config_file`` source allows developers to run ducktape tests from
            # non-AWS hardware but targeting a real S3 backend.
            logger.info("Running on AWS S3, setting credentials")
            self.cloud_storage_access_key = cloud_storage_access_key
            self.cloud_storage_secret_key = cloud_storage_secret_key
            self.endpoint_url = None  # None so boto auto-gens the endpoint url
            if test_context.globals.get(self.GLOBAL_CLOUD_PROVIDER,
                                        'aws') == 'gcp':
                self.endpoint_url = 'https://storage.googleapis.com'
            self.cloud_storage_disable_tls = False  # SI will fail to create archivers if tls is disabled
            self.cloud_storage_region = cloud_storage_region
            self.cloud_storage_api_endpoint_port = 443
            self.addressing_style = S3AddressingStyle.VIRTUAL
        else:
            logger.info('No AWS credentials supplied, assuming minio defaults')

    @property
    def cloud_storage_bucket(self) -> str:
        if self.cloud_storage_type == CloudStorageType.S3:
            bucket = self._cloud_storage_bucket
        elif self.cloud_storage_type == CloudStorageType.ABS:
            bucket = self._cloud_storage_azure_container

        return bucket

    def reset_cloud_storage_bucket(self, new_bucket_name: str) -> None:
        if self.cloud_storage_type == CloudStorageType.S3:
            self._cloud_storage_bucket = new_bucket_name
        elif self.cloud_storage_type == CloudStorageType.ABS:
            self._cloud_storage_azure_container = new_bucket_name

    def gcp_iam_token(self, logger):
        logger.info('Getting gcp iam token')
        s = requests.Session()
        s.mount('http://169.254.169.254', HTTPAdapter(max_retries=5))
        res = s.request(
            "GET",
            "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"})
        res.raise_for_status()
        return res.json()["access_token"]

    # Call this to update the extra_rp_conf
    def update_rp_conf(self, conf) -> dict[str, Any]:
        if self.cloud_storage_type == CloudStorageType.S3:
            conf[
                "cloud_storage_credentials_source"] = self.cloud_storage_credentials_source
            conf["cloud_storage_access_key"] = self.cloud_storage_access_key
            conf["cloud_storage_secret_key"] = self.cloud_storage_secret_key
            conf["cloud_storage_region"] = self.cloud_storage_region
            conf["cloud_storage_bucket"] = self._cloud_storage_bucket
            conf["cloud_storage_url_style"] = self.cloud_storage_url_style
        elif self.cloud_storage_type == CloudStorageType.ABS:
            conf[
                'cloud_storage_azure_storage_account'] = self.cloud_storage_azure_storage_account
            conf[
                'cloud_storage_azure_container'] = self._cloud_storage_azure_container
            conf[
                'cloud_storage_azure_shared_key'] = self.cloud_storage_azure_shared_key

        conf["log_segment_size"] = self.log_segment_size
        if (self.cloud_storage_cache_chunk_size):
            conf[
                "cloud_storage_cache_chunk_size"] = self.cloud_storage_cache_chunk_size

        conf["cloud_storage_enabled"] = True
        if self.cloud_storage_cache_size is None:
            # Default cache size for testing: large enough to enable streaming throughput up to 100MB/s, but no
            # larger, so that a test doing any significant amount of throughput will exercise trimming.
            conf[
                "cloud_storage_cache_size"] = SISettings.cache_size_for_throughput(
                    1024 * 1024 * 100)
        else:
            conf["cloud_storage_cache_size"] = self.cloud_storage_cache_size

        if self.cloud_storage_cache_max_objects is not None:
            conf[
                "cloud_storage_cache_max_objects"] = self.cloud_storage_cache_max_objects

        conf[
            'cloud_storage_enable_remote_read'] = self.cloud_storage_enable_remote_read
        conf[
            'cloud_storage_enable_remote_write'] = self.cloud_storage_enable_remote_write

        if self.endpoint_url is not None:
            conf[
                "cloud_storage_api_endpoint"] = self.cloud_storage_api_endpoint
            conf[
                "cloud_storage_api_endpoint_port"] = self.cloud_storage_api_endpoint_port

        if self.cloud_storage_disable_tls:
            conf['cloud_storage_disable_tls'] = self.cloud_storage_disable_tls

        if self.cloud_storage_max_connections:
            conf[
                'cloud_storage_max_connections'] = self.cloud_storage_max_connections
        if self.cloud_storage_readreplica_manifest_sync_timeout_ms:
            conf[
                'cloud_storage_readreplica_manifest_sync_timeout_ms'] = self.cloud_storage_readreplica_manifest_sync_timeout_ms
        if self.cloud_storage_segment_max_upload_interval_sec:
            conf[
                'cloud_storage_segment_max_upload_interval_sec'] = self.cloud_storage_segment_max_upload_interval_sec
        if self.cloud_storage_manifest_max_upload_interval_sec:
            conf[
                'cloud_storage_manifest_max_upload_interval_sec'] = self.cloud_storage_manifest_max_upload_interval_sec
        if self.cloud_storage_housekeeping_interval_ms:
            conf[
                'cloud_storage_housekeeping_interval_ms'] = self.cloud_storage_housekeeping_interval_ms
        if self.cloud_storage_spillover_manifest_max_segments:
            conf[
                'cloud_storage_spillover_manifest_max_segments'] = self.cloud_storage_spillover_manifest_max_segments

        conf['retention_local_strict'] = self.retention_local_strict

        if self.cloud_storage_max_throughput_per_shard:
            conf[
                'cloud_storage_max_throughput_per_shard'] = self.cloud_storage_max_throughput_per_shard

        # Enable scrubbing in testing unless it was explicitly disabled.
        if 'cloud_storage_enable_scrubbing' not in conf:
            conf['cloud_storage_enable_scrubbing'] = True

        return conf

    def set_expected_damage(self, damage_types: set[str]):
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

    def is_damage_expected(self, damage_types: set[str]):
        return (damage_types & self._expected_damage_types) == damage_types

    def get_expected_damage(self):
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

    def create_broker_cert(self, service: Service,
                           node: ClusterNode) -> tls.Certificate:
        """
        Create a certificate for a broker.
        """
        raise NotImplementedError("create_broker_cert")

    def create_service_client_cert(self, service: Service,
                                   name: str) -> tls.Certificate:
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

    def __init__(self):
        self.enable_sasl = False
        self.kafka_enable_authorization: Optional[bool] = None
        self.sasl_mechanisms: Optional[list[str]] = None
        self.http_authentication: Optional[list[str]] = None
        self.endpoint_authn_method: Optional[str] = None
        self.tls_provider: Optional[TLSProvider] = None
        self.require_client_auth: bool = True
        self.auto_auth: Optional[bool] = None

        # The rules to extract principal from mtls
        self.principal_mapping_rules = self.__DEFAULT_PRINCIPAL_MAPPING_RULES

    # sasl is required
    def sasl_enabled(self):
        return (self.kafka_enable_authorization is None and self.enable_sasl
                and self.endpoint_authn_method
                is None) or self.endpoint_authn_method == "sasl"

    # principal is extracted from mtls distinguished name
    def mtls_identity_enabled(self):
        return self.endpoint_authn_method == "mtls_identity"


class LoggingConfig:
    def __init__(self, default_level: str, logger_levels={}):
        self.default_level = default_level
        self.logger_levels = logger_levels

    def enable_finject_logging(self):
        self.logger_levels["finject"] = "trace"

    def to_args(self) -> str:
        """
        Generate redpanda CLI arguments for this logging config
        :return: string
        """
        args = f"--default-log-level {self.default_level}"
        if self.logger_levels:
            levels_arg = ":".join(
                [f"{k}={v}" for k, v in self.logger_levels.items()])
            args += f" --logger-log-level={levels_arg}"

        return args


class AuthConfig:
    def __init__(self):
        self.authn_method: Optional[str] = None


class TlsConfig(AuthConfig):
    def __init__(self):
        super(TlsConfig, self).__init__()
        self.server_key: Optional[str] = None
        self.server_crt: Optional[str] = None
        self.truststore_file: Optional[str] = None
        self.crl_file: Optional[str] = None
        self.client_key: Optional[str] = None
        self.client_crt: Optional[str] = None
        self.require_client_auth: bool = True
        self.enable_broker_tls: bool = True

    def maybe_write_client_certs(self, node, logger, tls_client_key_file: str,
                                 tls_client_crt_file: str):
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

    def __init__(self):
        super(PandaproxyConfig, self).__init__()
        self.cache_keep_alive_ms: Optional[int] = 300000
        self.cache_max_size: Optional[int] = 10
        self.advertised_api_host: Optional[str] = None


class SchemaRegistryConfig(TlsConfig):
    SR_TLS_CLIENT_KEY_FILE = "/etc/redpanda/sr_client.key"
    SR_TLS_CLIENT_CRT_FILE = "/etc/redpanda/sr_client.crt"

    mode_mutability = False

    def __init__(self):
        super(SchemaRegistryConfig, self).__init__()


class AuditLogConfig(TlsConfig):
    AUDIT_LOG_TLS_CLIENT_KEY_FILE = "/etc/redpanda/audit_log_client.key"
    AUDIT_LOG_TLS_CLIENT_CRT_FILE = "/etc/redpanda/audit_log_client.crt"

    def __init__(self,
                 listener_port: Optional[int] = None,
                 listener_authn_method: Optional[str] = None):
        super(AuditLogConfig, self).__init__()
        self.listener_port = listener_port
        self.listener_authn_method = listener_authn_method


class RpkNodeConfig:
    def __init__(self):
        self.ca_file = None


class RedpandaServiceConstants:
    SUPERUSER_CREDENTIALS: SaslCredentials = SaslCredentials(
        "admin", "admin", "SCRAM-SHA-256")


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
    logger: Logger

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Now ensure that this base is part of the right type of object.
        # For these checks to work, this __init__ method should appear in
        # the MRO after all the __init__ methods that would set these properties
        self._check_attr('context', TestContext)
        self._check_attr('logger', Logger)

    @abstractmethod
    def all_up(self):
        pass

    @abstractmethod
    def kafka_client_security(self) -> KafkaClientSecurity:
        """Return a KafkaClientSecurity object suitable for connecting to the Kafka API
         on this broker."""
        pass

    def wait_until(self,
                   fn: Callable[[], Any],
                   timeout_sec: int,
                   backoff_sec: int,
                   err_msg: str | Callable[[], str] = "") -> None:
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
                assert self.all_up() or getattr(self, '_tolerate_crashes',
                                                False)
            return r

        wait_until(wrapped,
                   timeout_sec=timeout_sec,
                   backoff_sec=backoff_sec,
                   err_msg=err_msg)

    def _check_attr(self, name: str, t: Type):
        v = getattr(self, name, None)
        mro = self.__class__.__mro__
        assert v is not None, f'RedpandaServiceABC was missing attribute {name} after __init__: {mro}\n'
        assert isinstance(
            v, t), f'{name} had wrong type, expected {t} but was {type(v)}'

    def _extract_samples(self, metrics, sample_pattern: str,
                         node) -> list[MetricSamples]:
        '''Extract metrics samples given a sample pattern. Embed the node in which it came from.
        '''
        found_sample = None
        sample_values = []

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
                    MetricSample(family.name, sample.name, node, sample.value,
                                 sample.labels))

        return sample_values

    @abstractmethod
    def metrics(
        self,
        node,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS
    ) -> Generator[Metric, Any, None]:
        '''Implement this method to parse the prometheus text format metric from a given node.'''
        pass

    @abstractmethod
    def metrics_sample(
        self,
        sample_pattern: str,
        nodes=None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> Optional[MetricSamples]:
        '''Implement this method to iterate over nodes to query metrics.

        Query metrics for a single sample using fuzzy name matching. This
        interface matches the sample pattern against sample names, and requires
        that exactly one (family, sample) match the query. All values for the
        sample across the requested set of nodes are returned in a flat array.

        None will be returned if less than one (family, sample) matches.
        An exception will be raised if more than one (family, sample) matches.

        For example, the query:

            redpanda.metrics_sample("under_replicated")

        will return an array containing MetricSample instances for each node and
        core/shard in the cluster. Each entry will correspond to a value from:

            family = vectorized_cluster_partition_under_replicated_replicas
            sample = vectorized_cluster_partition_under_replicated_replicas
        '''
        pass

    def _metrics_sample(
        self,
        sample_pattern: str,
        ns: list[Any],
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> Optional[MetricSamples]:
        '''Does the main work of the metrics_sample() implementation given a list of ns to iterate over.
        '''

        sample_values = []
        for n in ns:
            metrics = self.metrics(n, metrics_endpoint)
            sample_values += self._extract_samples(metrics, sample_pattern, n)

        if not sample_values:
            return None
        else:
            return MetricSamples(sample_values)

    @abstractmethod
    def metrics_samples(
        self,
        sample_patterns: list[str],
        nodes=None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> dict[str, MetricSamples]:
        '''Implement this method to iterate over nodes to query multiple sample patterns.

        Query metrics for multiple sample names using fuzzy matching.
        Similar to metrics_sample(), but works with multiple patterns.
        '''
        pass

    def _metrics_samples(
        self,
        sample_patterns: list[str],
        ns: list[Any],
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> dict[str, MetricSamples]:
        '''Does the main work of the metrics_samples() implementation given a list of ns to iterate over.
        '''
        sample_values_per_pattern = {
            pattern: []
            for pattern in sample_patterns
        }

        for n in ns:
            for pattern in sample_patterns:
                metrics = self.metrics(n, metrics_endpoint)
                sample_values_per_pattern[pattern] += self._extract_samples(
                    metrics, pattern, n)

        return {
            pattern: MetricSamples(values)
            for pattern, values in sample_values_per_pattern.items() if values
        }

    @abstractmethod
    def metric_sum(self,
                   metric_name,
                   metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
                   namespace: str | None = None,
                   topic: str | None = None,
                   nodes=None):
        '''Implement this method to sum the metrics.

        Pings the 'metrics_endpoint' of each node and returns the summed values
        of the given metric, optionally filtering by namespace and topic.
        '''
        pass

    def _metric_sum(
            self,
            metric_name: str,
            ns: Any,
            metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
            namespace: str | None = None,
            topic: str | None = None):
        '''Does the main work of the metric_sum() implementation given a list of ns to iterate over.
        '''

        count = 0
        for n in ns:
            metrics = self.metrics(n, metrics_endpoint=metrics_endpoint)
            for family in metrics:
                for sample in family.samples:
                    if sample.name != metric_name:
                        continue
                    labels = sample.labels
                    if namespace:
                        assert "redpanda_namespace" in labels or "namespace" in labels, f"Missing namespace label: {sample}"
                        if labels.get("redpanda_namespace",
                                      labels.get("namespace")) != namespace:
                            continue
                    if topic:
                        assert "redpanda_topic" in labels or "topic" in labels, f"Missing topic label: {sample}"
                        if labels.get("redpanda_topic",
                                      labels.get("topic")) != topic:
                            continue
                    count += int(sample.value)
        return count


class RedpandaServiceBase(RedpandaServiceABC, Service):
    PERSISTENT_ROOT = "/var/lib/redpanda"
    TRIM_LOGS_KEY = "trim_logs"
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
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
    COVERAGE_PROFRAW_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                            "redpanda.profraw")
    TEMP_OSSL_CONFIG_FILE = "/etc/openssl.cnf"
    DEFAULT_NODE_READY_TIMEOUT_SEC = 30
    NODE_READY_TIMEOUT_MIN_SEC_KEY = "node_ready_timeout_min_sec"
    DEFAULT_CLOUD_STORAGE_SCRUB_TIMEOUT_SEC = 60
    DEDICATED_NODE_KEY = "dedicated_nodes"
    RAISE_ON_ERRORS_KEY = "raise_on_error"
    LOG_LEVEL_KEY = "redpanda_log_level"
    DEFAULT_LOG_LEVEL = "info"
    COV_KEY = "enable_cov"
    DEFAULT_COV_OPT = "OFF"

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

    CLUSTER_CONFIG_DEFAULTS = {
        'join_retry_timeout_ms': 200,
        'default_topic_partitions': 4,
        'enable_metrics_reporter': False,
        'superusers': [RedpandaServiceConstants.SUPERUSER_CREDENTIALS[0]],
        # Disable segment size jitter to make tests more deterministic if they rely on
        # inspecting storage internals (e.g. number of segments after writing a certain
        # amount of data).
        'log_segment_size_jitter_percent': 0,

        # This is high enough not to interfere with the logic in any tests, while also
        # providing some background coverage of the connection limit code (i.e. that it
        # doesn't crash, it doesn't limit when it shouldn't)
        'kafka_connections_max': 2048,
        'kafka_connections_max_per_ip': 1024,
        'kafka_connections_max_overrides': ["1.2.3.4:5"],
    }

    logs = {
        "redpanda_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        },
        "code_coverage_profraw_file": {
            "path": COVERAGE_PROFRAW_CAPTURE,
            "collect_default": True
        },
        "executable": {
            "path": EXECUTABLE_SAVE_PATH,
            "collect_default": False
        },
        "backtraces": {
            "path": BACKTRACE_CAPTURE,
            "collect_default": True
        }
    }

    class FIPSMode(Enum):
        disabled = 0
        permissive = 1
        enabled = 2

    def __init__(self,
                 context: TestContext,
                 num_brokers: int,
                 *,
                 cluster_spec=None,
                 extra_rp_conf=None,
                 resource_settings: Optional[ResourceSettings] = None,
                 si_settings: Optional[SISettings] = None,
                 superuser: Optional[SaslCredentials] = None,
                 skip_if_no_redpanda_log: Optional[bool] = False,
                 disable_cloud_storage_diagnostics=True):

        super(RedpandaServiceBase, self).__init__(context,
                                                  num_nodes=num_brokers,
                                                  cluster_spec=cluster_spec)
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

        self._admin = Admin(self,
                            auth=(self._superuser.username,
                                  self._superuser.password))

        if resource_settings is None:
            resource_settings = ResourceSettings()
        self._resource_settings = resource_settings

        # Disable saving cloud storage diagnostics. This may be useful for
        # tests that generate millions of objecst, as collecting diagnostics
        # may take a significant amount of time.
        self._disable_cloud_storage_diagnostics = disable_cloud_storage_diagnostics

        self._trim_logs = self._context.globals.get(self.TRIM_LOGS_KEY, True)

        self._node_id_by_idx = {}
        self._security_config: dict[str, str | int] = {}

        self._skip_if_no_redpanda_log = skip_if_no_redpanda_log

        self._dedicated_nodes: bool = self._context.globals.get(
            self.DEDICATED_NODE_KEY, False)

        self.logger.info(
            f"ResourceSettings: dedicated_nodes={self._dedicated_nodes}")

    def restart_nodes(self,
                      nodes,
                      override_cfg_params=None,
                      start_timeout=None,
                      stop_timeout=None,
                      auto_assign_node_id=False,
                      omit_seeds_on_idx_one=True,
                      extra_cli: list[str] = []):

        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(nodes)) as executor:
            # The list() wrapper is to cause futures to be evaluated here+now
            # (including throwing any exceptions) and not just spawned in background.
            list(
                executor.map(lambda n: self.stop_node(n, timeout=stop_timeout),
                             nodes))
            list(
                executor.map(
                    lambda n: self.start_node(
                        n,
                        override_cfg_params=override_cfg_params,
                        timeout=start_timeout,
                        auto_assign_node_id=auto_assign_node_id,
                        omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                        extra_cli=extra_cli), nodes))

    def set_extra_rp_conf(self, conf):
        self._extra_rp_conf = conf
        if self._si_settings is not None:
            self._extra_rp_conf = self._si_settings.update_rp_conf(
                self._extra_rp_conf)

    def set_si_settings(self, si_settings: SISettings):
        si_settings.load_context(self.logger, self._context)
        self._si_settings = si_settings
        self._extra_rp_conf = self._si_settings.update_rp_conf(
            self._extra_rp_conf)

    def add_extra_rp_conf(self, conf):
        self._extra_rp_conf = {**self._extra_rp_conf, **conf}

    def metric_sum(self,
                   metric_name: str,
                   metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
                   namespace: str | None = None,
                   topic: str | None = None,
                   nodes: Any = None):
        '''
        Pings the 'metrics_endpoint' of each node and returns the summed values
        of the given metric, optionally filtering by namespace and topic.
        '''

        if nodes is None:
            nodes = self.nodes

        return self._metric_sum(metric_name, nodes, metrics_endpoint,
                                namespace, topic)

    def healthy(self):
        """
        A primitive health check on all the nodes which returns True when all
        nodes report that no under replicated partitions exist. This should
        later be replaced by a proper / official start-up probe type check on
        the health of a node after a restart.
        """
        counts: dict[int, int
                     | None] = {
                         self.idx(node): None
                         for node in self.nodes
                     }
        for node in self.nodes:
            try:
                metrics = self.metrics(node)
            except:
                return False
            idx = self.idx(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_cluster_partition_under_replicated_replicas":
                        counts[idx] = int(sample.value) + (counts[idx] or 0)
        return all(map(lambda count: count == 0, counts.values()))

    def rolling_restart_nodes(self,
                              nodes,
                              override_cfg_params=None,
                              start_timeout=None,
                              stop_timeout=None,
                              use_maintenance_mode=True,
                              omit_seeds_on_idx_one=True,
                              auto_assign_node_id=False):
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        restarter = RollingRestarter(self)
        restarter.restart_nodes(nodes,
                                override_cfg_params=override_cfg_params,
                                start_timeout=start_timeout,
                                stop_timeout=stop_timeout,
                                use_maintenance_mode=use_maintenance_mode,
                                omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                                auto_assign_node_id=auto_assign_node_id)

    def set_resource_settings(self, rs):
        self._resource_settings = rs

    @property
    def si_settings(self) -> SISettings:
        '''Return the SISettings object associated with this redpanda service,
        containing the cloud storage associated settings. Throws if si settings
        were not configured for this service.'''
        assert self._si_settings, 'si_settings were None, probably because they were not specified during redpanda service creation'
        return self._si_settings

    def for_nodes(self, nodes, cb: Callable) -> list:
        n_workers = len(nodes)
        if n_workers > 0:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=n_workers) as executor:
                # The list() wrapper is to cause futures to be evaluated here+now
                # (including throwing any exceptions) and not just spawned in background.
                return list(executor.map(cb, nodes))
        else:
            return []

    def trim_logs(self):
        if not self._trim_logs:
            return

        # Excessive logging may cause disks to fill up quickly.
        # Call this method to removes TRACE and DEBUG log lines from redpanda logs
        # Ensure this is only done on tests that have passed
        def prune(node):
            node.account.ssh(
                f"sed -i -E -e '/TRACE|DEBUG/d' {RedpandaService.STDOUT_STDERR_CAPTURE} || true"
            )

        self.for_nodes(self.nodes, prune)

    def node_id(self, node, force_refresh=False, timeout_sec=30):
        """
        Returns the node ID of a given node. Uses a cached value unless
        'force_refresh' is set to True.

        NOTE: this is not thread-safe.
        """
        idx = self.idx(node)
        if not force_refresh:
            if idx in self._node_id_by_idx:
                return self._node_id_by_idx[idx]

        def _try_get_node_id():
            try:
                node_cfg = self._admin.get_node_config(node)
            except:
                return (False, -1)
            return (True, node_cfg["node_id"])

        node_id = wait_until_result(
            _try_get_node_id,
            timeout_sec=timeout_sec,
            err_msg=f"couldn't reach admin endpoint for {node.account.hostname}"
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

            creds = SaslCredentials(username=get_str('sasl_plain_username'),
                                    password=get_str('sasl_plain_password'),
                                    algorithm=get_str('sasl_mechanism'))
        else:
            creds = None

        return KafkaClientSecurity(creds, tls_enabled=False)

    def set_skip_if_no_redpanda_log(self, v: bool):
        self._skip_if_no_redpanda_log = v

    def raise_on_bad_logs(self, allow_list=None):
        """
        Raise a BadLogLines exception if any nodes' logs contain errors
        not permitted by `allow_list`

        :param allow_list: list of compiled regexes, or None for default
        :return: None
        """
        allow_list = prepare_allow_list(allow_list)

        _searchable_nodes = []
        for node in self.nodes:
            if self._skip_if_no_redpanda_log and not node.account.exists(
                    RedpandaServiceBase.STDOUT_STDERR_CAPTURE):
                self.logger.info(
                    f"{RedpandaServiceBase.STDOUT_STDERR_CAPTURE} not found on {node.account.hostname}. Skipping log scan."
                )
                continue
            _searchable_nodes.append(node)

        lsearcher = LogSearchLocal(self._context, allow_list, self.logger,
                                   RedpandaServiceBase.STDOUT_STDERR_CAPTURE)
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


class KubeServiceMixin:

    kubectl: KubectlTool

    def get_node_memory_mb(self):
        line = self.kubectl.exec("cat /proc/meminfo | grep MemTotal")
        memory_kb = int(line.strip().split()[1])
        return memory_kb / 1024

    def get_node_cpu_count(self):
        core_count_str = self.kubectl.exec(
            "cat /proc/cpuinfo | grep ^processor | wc -l")
        return int(core_count_str.strip())

    def get_node_disk_free(self):
        if self.kubectl.exists(RedpandaServiceBase.PERSISTENT_ROOT):
            df_path = RedpandaServiceBase.PERSISTENT_ROOT
        else:
            # If dir doesn't exist yet, use the parent.
            df_path = os.path.dirname(RedpandaServiceBase.PERSISTENT_ROOT)
        df_out = self.kubectl.exec(f"df --output=avail {df_path}")
        avail_kb = int(df_out.strip().split("\n")[1].strip())
        return avail_kb * 1024


class CorruptedClusterError(Exception):
    """Throw to indicate a cluster is an unhealthy or otherwise unsuitable state to
    continue the remainder of the tests."""
    pass


class RedpandaServiceCloud(KubeServiceMixin, RedpandaServiceABC):
    """
    Service class for running tests against Redpanda Cloud.

    Use the `make_redpanda_service` factory function to instantiate. Set
    the GLOBAL_CLOUD_* values in the global json file to have the factory
    use `RedpandaServiceCloud`.
    """

    GLOBAL_CLOUD_CLUSTER_CONFIG = 'cloud_cluster'

    def __init__(self,
                 context: TestContext,
                 *,
                 config_profile_name: str,
                 skip_if_no_redpanda_log: Optional[bool] = False,
                 min_brokers: int = 3,
                 **kwargs):
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
        self.logger = context.logger

        super().__init__()

        self.config_profile_name = config_profile_name
        self._min_brokers = min_brokers
        self._superuser = RedpandaServiceBase.SUPERUSER_CREDENTIALS

        self._trim_logs = False

        # Prepare values from globals.json to serialize
        # later to dataclass
        self._cc_config = context.globals[self.GLOBAL_CLOUD_CLUSTER_CONFIG]

        self._provider_config = {}
        match context.globals.get("cloud_provider"):
            case "aws" | "gcp":
                self._provider_config.update({
                    'access_key':
                        context.globals.get(SISettings.GLOBAL_S3_ACCESS_KEY, None),
                    'secret_key':
                        context.globals.get(SISettings.GLOBAL_S3_SECRET_KEY, None),
                    'region':
                        context.globals.get(SISettings.GLOBAL_S3_REGION_KEY, None)
                }) # yapf: disable
            case "azure":
                self._provider_config.update({
                    'azure_client_id':
                        context.globals.get(SISettings.GLOBAL_AZURE_CLIENT_ID, None),
                    'azure_client_secret':
                        context.globals.get(SISettings.GLOBAL_AZURE_CLIENT_SECRET, None),
                    'azure_tenant_id':
                        context.globals.get(SISettings.GLOBAL_AZURE_TENANT_ID, None)
                }) # yapf: disable
            case _:
                pass

        # log cloud cluster id
        self.logger.debug(f"initial cluster_id: {self._cc_config['id']}")

        # Create cluster class
        self._cloud_cluster = CloudCluster(
            context,
            self.logger,
            self._cc_config,
            provider_config=self._provider_config)
        # Prepare kubectl
        self.__kubectl = None

        # Backward compatibility with RedpandaService
        # Fake out sasl_enabled callable
        self.sasl_enabled = lambda: True
        # Always true for Cloud Cluster
        self._dedicated_nodes = True
        self.logger.info(
            'ResourceSettings: setting dedicated_nodes=True because serving from redpanda cloud'
        )

        cluster_id = self._cloud_cluster.create(superuser=self._superuser)
        remote_uri = f'redpanda@{cluster_id}-agent'
        self.__kubectl = KubectlTool(
            self,
            remote_uri=remote_uri,
            cluster_id=cluster_id,
            cluster_provider=self._cloud_cluster.config.provider,
            cluster_region=self._cloud_cluster.config.region,
            tp_proxy=self._cloud_cluster.config.teleport_auth_server,
            tp_token=self._cloud_cluster.config.teleport_bot_token)

        self.rebuild_pods_classes()

        node_count = self.config_profile['nodes_count']
        assert self._min_brokers <= node_count, f'Not enough brokers: test needs {self._min_brokers} but cluster has {node_count}'

    @property
    def kubectl(self):
        assert self.__kubectl, 'kubectl accessed before cluster was started?'
        return self.__kubectl

    def who_am_i(self):
        return self._cloud_cluster.cluster_id

    def security_config(self):
        return dict(security_protocol='SASL_SSL',
                    sasl_mechanism=self._superuser.algorithm,
                    sasl_plain_username=self._superuser.username,
                    sasl_plain_password=self._superuser.password,
                    enable_tls=True)

    def kafka_client_security(self):
        return KafkaClientSecurity(self._superuser, True)

    def rebuild_pods_classes(self):
        """Querry pods and create Classes fresh
        """
        self.pods = [
            CloudBroker(p, self.kubectl, self.logger)
            for p in self.get_redpanda_pods()
        ]

    def start(self):
        """Does nothing, do not call."""
        # everything here was moved into __init__
        pass

    def format_pod_status(self, pods):
        statuses_list = [(p['metadata']['name'], p['status']['phase'])
                         for p in pods]
        if len(statuses_list) < 1:
            return "none"
        else:
            return ", ".join([f"{n}: {s}" for n, s in statuses_list])

    def get_redpanda_pods_filtered(self, status):
        return [
            p for p in self.get_redpanda_pods()
            if p['status']['phase'].lower() == status
        ]

    def get_redpanda_pods_presorted(self):
        """Method gets all pods and separates them into active and inactive

        return: active_pods, inactive_pods, unknown lists
        """
        # Pod lifecycle is: Pending, Running, Succeeded, Failed, Unknown
        active_phases = ['running']
        inactive_phases = ['pending', 'succeeded', 'failed']

        all_pods = self.get_redpanda_pods()
        # Sort pods into bins
        active_rp_pods = []
        inactive_rp_pods = []
        unknown_rp_pods = []
        for pod in all_pods:
            _status = pod['status']['phase'].lower()
            if _status in active_phases:
                active_rp_pods.append(pod)
            elif _status in inactive_phases:
                inactive_rp_pods.append(pod)
            else:
                # Phase unknown and others
                unknown_rp_pods.append(pod)

        # Log pod names and statuses
        # Example: Current Redpanda pods: rp-cnplksdb0t6f2b421c20-0: Running, rp-cnplksdb0t6f2b421c20-1: Running, rp-cnplksdb0t6f2b421c20-2: Running'
        self.logger.debug("Active RP cluster pods: "
                          f"{self.format_pod_status(active_rp_pods)}")
        self.logger.debug("Inactive RP cluster pods: "
                          f"{self.format_pod_status(inactive_rp_pods)}")
        self.logger.debug("Other RP cluster pods: "
                          f"{self.format_pod_status(unknown_rp_pods)}")

        return active_rp_pods, inactive_rp_pods, unknown_rp_pods

    def get_redpanda_statefulset(self):
        """Get the statefulset for redpanda brokers"""
        if not self.is_operator_v2_cluster():
            return None
        return json.loads(
            self.kubectl.cmd(
                'get statefulset -n redpanda redpanda-broker -o json'))

    def get_redpanda_pods(self):
        """Get the current list of redpanda pods as k8s API objects."""
        pods = json.loads(self.kubectl.cmd('get pods -n redpanda -o json'))

        return [
            p for p in pods['items'] if is_redpanda_pod(p, self.cluster_id)
        ]

    @property
    def cluster_id(self):
        cid = self._cloud_cluster.cluster_id
        assert cid, cid
        return cid

    def get_node_by_id(self, id):
        for p in self.pods:
            if p.slot_id == id:
                return p
        return None

    @cached_property
    def config_profile(self) -> dict[str, Any]:
        profiles: dict[str, Any] = self.get_install_pack()['config_profiles']
        if self.config_profile_name not in profiles:
            # throw user friendly error
            raise RuntimeError(
                f"'{self.config_profile_name}' not found among config profiles: {profiles.keys()}"
            )
        return profiles[self.config_profile_name]

    def restart_pod(self, pod_name: str, timeout: int = 180):
        """Restart a pod by name

        Using kubectl delete to gracefully stop the pod,
        the pod will automatically be restarted by the cluster.
        Block until pod container is ready.
        The list of pod names can be found in self.pods property.

        :param pod_name: string of pod name, e.g. 'rp-clo88krkqkrfamptsst0-5'
        :param timeout: seconds to wait until pod is ready after kubectl delete
        """
        self.logger.info(f'restarting pod {pod_name}')

        def pod_container_ready(pod_name: str):
            # kubectl get pod rp-clo88krkqkrfamptsst0-0 -n=redpanda -o=jsonpath='{.status.containerStatuses[0].ready}'
            return self.kubectl.cmd([
                'get', 'pod', pod_name, '-n=redpanda',
                "-o=jsonpath='{.status.containerStatuses[0].ready}'"
            ])

        delete_cmd = ['delete', 'pod', pod_name, '-n=redpanda']
        self.logger.info(
            f'deleting pod {pod_name} so the cluster can recreate it')
        # kubectl delete pod rp-clo88krkqkrfamptsst0-0 -n=redpanda'
        self.kubectl.cmd(delete_cmd)

        self.logger.info(
            f'waiting for pod {pod_name} container status ready with timeout {timeout}'
        )
        wait_until(lambda: pod_container_ready(pod_name) == 'true',
                   timeout_sec=timeout,
                   backoff_sec=1,
                   err_msg=f'pod {pod_name} container status not ready')
        self.logger.info(f'pod {pod_name} container status ready')

        # Call to rebuild metadata for all cloud brokers
        self.rebuild_pods_classes()

    def is_operator_v2_cluster(self):
        # Below will fail if there is no 'redpanda' resource type. This is the
        # case on operator v1 but exists on operator v2.
        try:
            self.kubectl.cmd(
                ['get', 'redpanda', '-n=redpanda', '--ignore-not-found'])
        except subprocess.CalledProcessError as e:
            if 'have a resource type "redpanda"' in e.stderr:
                return False

        return True

    def rolling_restart_pods(self, pod_timeout: int = 180):
        """Restart all pods in the cluster one at a time.

        Restart a pod in the cluster and does not restart another
        until the previous one has finished.
        Block until cluster is ready after all restarts are finished.

        :param pod_timeout: seconds to wait for each pod to be ready after restart
        """

        pod_names = [p.name for p in self.pods]
        self.logger.info(f'rolling restart on pods: {pod_names}')

        for pod_name in pod_names:
            self.restart_pod(pod_name, pod_timeout)
            cluster_name = f'rp-{self._cloud_cluster.cluster_id}'
            expected_replicas = self.cluster_desired_replicas(cluster_name)
            # Check cluster readiness after pod restart
            self.check_cluster_readiness(cluster_name, expected_replicas,
                                         pod_timeout)

    def concurrent_restart_pods(self, pod_timeout):
        """
        Restart all pods in the cluster concurrently and wait
        for the entire cluster to be ready.
        """

        cluster_name = f'rp-{self._cloud_cluster.cluster_id}'
        pod_names = [p.name for p in self.pods]
        self.logger.info(f'Starting concurrent restart on pods: {pod_names}')

        threads = []
        for pod_name in pod_names:
            thread = threading.Thread(target=self.restart_pod,
                                      args=(pod_name, ))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()  # Wait for all threads to complete

        self.logger.info(
            "All pods have been restarted (deleted) concurrently.")

        # kubectl get cluster rp-clo88krkqkrfamptsst0 -n=redpanda -o=jsonpath='{.status.replicas}'
        expected_replicas = int(
            self.kubectl.cmd([
                'get', 'cluster', cluster_name, '-n=redpanda',
                "-o=jsonpath='{.status.replicas}'"
            ]))

        # Check cluster readiness after restart of all pods
        self.check_cluster_readiness(cluster_name, expected_replicas,
                                     pod_timeout)

    def check_cluster_readiness(self, cluster_name: str,
                                expected_replicas: int, pod_timeout: int):
        """Checks if the cluster has the expected number of ready replicas."""
        self.logger.info(
            f"Waiting for cluster {cluster_name} to have readyReplicas {expected_replicas} with timeout {pod_timeout}"
        )

        wait_until(
            lambda: self.cluster_ready_replicas(cluster_name
                                                ) == expected_replicas,
            timeout_sec=pod_timeout,
            backoff_sec=1,
            err_msg=
            f"Cluster {cluster_name} failed to arrive at readyReplicas {expected_replicas}"
        )

        self.logger.info(
            f"Cluster {cluster_name} arrived at readyReplicas {expected_replicas}"
        )

    def cluster_desired_replicas(self, cluster_name: str = ''):
        """Return cluster desired replica count."""
        if self.is_operator_v2_cluster():
            rp_statefulset = self.get_redpanda_statefulset()
            return int(rp_statefulset['status']['replicas'])
        expected_replicas = int(
            self.kubectl.cmd([
                'get', 'cluster', cluster_name, '-n=redpanda',
                "-o=jsonpath='{.status.replicas}'"
            ]))
        return expected_replicas

    def cluster_ready_replicas(self, cluster_name: str = ''):
        """Retrieves the number of ready replicas for the given cluster."""
        if self.is_operator_v2_cluster():
            rp_statefulset = self.get_redpanda_statefulset()
            return int(rp_statefulset['status']['readyReplicas'])
        ret = self.kubectl.cmd([
            'get', 'cluster', cluster_name, '-n=redpanda',
            "-o=jsonpath='{.status.readyReplicas}'"
        ])
        return int(0 if not ret else ret)

    def verify_basic_produce_consume(self, producer, consumer):
        self.logger.info("Checking basic producer functions")
        current_sent = producer.produce_status.sent
        produce_count = 100
        consume_count = 100

        def producer_complete():
            number_left = (current_sent +
                           produce_count) - producer.produce_status.sent
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
            err_msg=f"Could not consume {consume_count} msgs in 120 seconds")

        consumer.stop()
        consumer.free()
        self.logger.info(
            f"Successfully verified basic produce/consume with {produce_count}/"
            f"{consume_count} messages.")

    def stop(self, **kwargs):
        if self._cloud_cluster.config.delete_cluster:
            self._cloud_cluster.delete()
        else:
            self.logger.info(
                f'skipping delete of cluster {self._cloud_cluster.cluster_id}')

    def brokers(self) -> str:
        return self._cloud_cluster.get_broker_address()

    def install_pack_version(self) -> str:
        return self._cloud_cluster.get_install_pack_version()

    def sockets_clear(self, node: RemoteClusterNode):
        return True

    def all_up(self) -> bool:
        return self.cluster_healthy()

    def metrics(
            self,
            pod,
            metrics_endpoint: MetricsEndpoint = MetricsEndpoint.PUBLIC_METRICS
    ):
        '''Parse the prometheus text format metric from a given pod.'''
        if metrics_endpoint == MetricsEndpoint.PUBLIC_METRICS:
            text = self._cloud_cluster.get_public_metrics()
        else:
            text = self.kubectl.exec(
                'curl -f -s -S http://localhost:9644/metrics', pod.name)
        return text_string_to_metric_families(text)

    def metrics_sample(
        self,
        sample_pattern: str,
        pods=None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> Optional[MetricSamples]:
        '''
        Query metrics for a single sample using fuzzy name matching. This
        interface matches the sample pattern against sample names, and requires
        that exactly one (family, sample) match the query. All values for the
        sample across the requested set of pods are returned in a flat array.

        None will be returned if less than one (family, sample) matches.
        An exception will be raised if more than one (family, sample) matches.

        For example, the query:

            redpanda.metrics_sample("under_replicated")

        will return an array containing MetricSample instances for each pod and
        core/shard in the cluster. Each entry will correspond to a value from:

            family = vectorized_cluster_partition_under_replicated_replicas
            sample = vectorized_cluster_partition_under_replicated_replicas
        '''

        if pods is None:
            pods = self.pods

        return self._metrics_sample(sample_pattern, pods, metrics_endpoint)

    def metrics_samples(
        self,
        sample_patterns: list[str],
        pods=None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> dict[str, MetricSamples]:
        '''
        Query metrics for multiple sample names using fuzzy matching.
        Similar to metrics_sample(), but works with multiple patterns.
        '''
        pods = pods or self.pods
        return self._metrics_samples(sample_patterns, pods, metrics_endpoint)

    def metric_sum(self,
                   metric_name: str,
                   metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
                   namespace: str | None = None,
                   topic: str | None = None,
                   pods: Any = None):
        '''
        Pings the 'metrics_endpoint' of each pod and returns the summed values
        of the given metric, optionally filtering by namespace and topic.
        '''

        if pods is None:
            pods = self.pods

        return self._metric_sum(metric_name, pods, metrics_endpoint, namespace,
                                topic)

    @staticmethod
    def get_cloud_globals(globals: dict[str, Any]) -> dict[str, Any]:
        _config = {}
        if RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG in globals:
            # Load needed config values from cloud section
            # of globals prior to actual cluster creation
            _config = globals[RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG]
        return _config

    def get_product(self):
        """ Get product information.

        Returns dict with info of product, including advertised limits.
        Returns none if product info for the tier is not found.
        """
        return self._cloud_cluster.get_product()

    def get_install_pack(self):
        install_pack_client = InstallPackClient(
            self._cloud_cluster.config.install_pack_url_template,
            self._cloud_cluster.config.install_pack_auth_type,
            self._cloud_cluster.config.install_pack_auth)
        install_pack_version = self._cloud_cluster.config.install_pack_ver

        # Load install pack and check profile
        return install_pack_client.getInstallPack(install_pack_version)

    def cloud_agent_ssh(self, remote_cmd):
        """Run the given command on the redpanda agent node of the cluster.

        :param remote_cmd: The command to run on the agent node.
        """
        return self.kubectl._ssh_cmd(remote_cmd)

    def scale_cluster(self, nodes_count):
        """Scale out/in cluster to specified number of nodes.
        """
        return self._cloud_cluster.scale_cluster(nodes_count)

    def clean_cluster(self):
        """Cleans state from a running cluster to make it seem like it was newly provisioned.

        Call this function at the end of a test case so the next test case has a clean cluster.
        For safety, this function must not be called concurrently.
        """
        assert self.context.session_context.max_parallel < 2, 'unsafe to clean cluster if ducktape run with parallelism'

        # assuming topics beginning with '_' are system topics that should not be deleted
        rpk = RpkTool(self)
        topics = rpk.list_topics()
        deletable = [x for x in topics if not x.startswith('_')]
        self.logger.debug(
            f'found topics to delete ({len(deletable)}): {deletable}')
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

        uh_reason = self._cloud_cluster._ensure_cluster_health()
        if uh_reason is not None:
            raise CorruptedClusterError(uh_reason)

        expected_nodes = int(self.config_profile['nodes_count'])
        active, _, _ = self.get_redpanda_pods_presorted()
        failed = self.get_redpanda_pods_filtered('failed')
        active_count = len(active)
        failed_count = len(failed)
        assert expected_nodes == active_count, f'Expected {expected_nodes} per tier definition but found {active_count} active pods'
        assert failed_count == 0, f'Expected no failed pods, found {failed_count}'

        brokers = self._cloud_cluster.get_brokers()
        broker_count = len(brokers)
        assert expected_nodes == broker_count, (
            f'Expected {expected_nodes} per tier definition but there '
            f'were only {broker_count} brokers: {brokers}')

    def raise_on_crash(self,
                       log_allow_list: list[str | re.Pattern] | None = None):
        """Function checks if active RP pods has restart counter changed since last check
        """

        # Can't remove log_allow_list as it is present in the metadataaddeer call
        # Checking logs in case of crash is useless for pods as they are auto-restarted anyway
        def _get_stored_pod(uuid):
            """Shortcut to getting proper stored Broker class
            """
            for pod in self.pods:
                if uuid == pod.uuid:
                    return pod
            return None

        def _get_container_id(p):
            # Shortcut to getting containerID
            return p['containerStatuses'][0]['containerID']

        def _get_restart_count(p):
            # Shortcut to getting restart counter
            return p['containerStatuses'][0]['restartCount']

        # Not checking active count vs expected nodes
        active, _, _ = self.get_redpanda_pods_presorted()
        for pod in active:
            _name = pod['metadata']['name']

            # Check if stored pod and loaded one is the same
            _stored_pod = _get_stored_pod(pod['metadata']['uid'])
            if _stored_pod is None:
                raise NodeCrash(
                    (_name, "Pod not found among prior stored ones"))

            # Check if container inside pod stayed the same
            container_id = _get_container_id(pod['status'])
            if _get_container_id(_stored_pod._status) != container_id:
                raise NodeCrash(
                    (_name, "Pod container mismatch with prior stored one"))

            # Check that restart count is the same
            restart_count = _get_restart_count(pod['status'])
            if _get_restart_count(_stored_pod._status) != restart_count:
                raise NodeCrash(
                    (_name, "Pod has been restarted due to possible crash"))

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
        ret = self.kubectl.exec('rpk cluster health')

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
        self.logger.debug(f'rpk cluster health lines: {lines}')
        unhealthy_reasons = 'no line found'
        for line in lines:
            part = line.partition(':')
            heading = part[0].strip()
            if heading == 'Healthy':
                if part[2].strip() == 'true':
                    return None
            elif heading == 'Unhealthy reasons':
                unhealthy_reasons = part[2].strip()

        return unhealthy_reasons

    def cluster_healthy(self) -> bool:
        return self.cluster_unhealthy_reason is not None

    def raise_on_bad_logs(self, allow_list=None, test_start_time=None):
        """
        Raise a BadLogLines exception if any nodes' logs contain errors
        not permitted by `allow_list`

        :param allow_list: list of compiled regexes, or None for default
        :return: None
        """
        allow_list = prepare_allow_list(allow_list)

        lsearcher = LogSearchCloud(self._context,
                                   allow_list,
                                   self.logger,
                                   self.kubectl,
                                   test_start_time=test_start_time)
        lsearcher.search_logs(self.pods)

    def copy_cloud_logs(self, test_start_time):
        """Method makes sure that agent and cloud logs is copied after the test
        """
        def create_dest_path(service_name):
            # Create directory into which service logs will be copied
            dest = os.path.join(
                TestContext.results_dir(self._context,
                                        self._context.test_index),
                service_name)
            if not os.path.isdir(dest):
                mkdir_p(dest)

            return dest

        def copy_from_agent(since):
            service_name = f"{self._cloud_cluster.cluster_id}-agent"
            # Example path:
            # '/home/ubuntu/redpanda/tests/results/2024-04-11--019/SelfRedpandaCloudTest/test_healthy/2/coc12bfs0etj2dg9a5ig-agent'
            dest = create_dest_path(service_name)

            logfile = os.path.join(dest, "agent.log")
            # Query journalctl to copy logs from
            with open(logfile, 'wb') as lfile:
                for line in self.kubectl._ssh_cmd(
                        f"journalctl -u redpanda-agent -S '{since}'".split(),
                        capture=True):
                    lfile.writelines([line])
            return

        def copy_from_pod(params):
            """Function copies logs from agent and all RP pods
            """
            pod = params["pod"]
            test_start_time = params["s_time"]
            dest = create_dest_path(pod.name)
            try:
                remote_path = os.path.join("/tmp", "pod_log_extract.sh")
                logfile = os.path.join(dest, f"{pod.name}.log")
                with open(logfile, 'wb') as lfile:
                    for line in pod.nodeshell(
                            f"bash {remote_path} '{pod.name}' "
                            f"'{test_start_time}'".split(),
                            capture=True):
                        lfile.writelines([line])  # type: ignore
            except Exception as e:
                self.logger.warning(f"Error getting logs for {pod.name}: {e}")
            return pod.name

        # Safeguard if CloudService not created
        if self.pods is None or self._cloud_cluster is None:
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
        params = []
        for pod in self.pods:
            params.append({"pod": pod, "s_time": f_start_time})
        sw.start()
        for name in pool.map(copy_from_pod, params):
            # Calculate time for this node
            self.logger.info(
                sw.elapsedf(f"# Done log copy for {name} (interim)"))
        return {}


class RedpandaService(RedpandaServiceBase):

    nodes: list[ClusterNode]

    def __init__(self,
                 context: TestContext,
                 num_brokers: int,
                 *,
                 extra_rp_conf=None,
                 extra_node_conf=None,
                 resource_settings=None,
                 si_settings=None,
                 log_level: Optional[str] = None,
                 log_config: Optional[LoggingConfig] = None,
                 environment: Optional[dict[str, str]] = None,
                 security: SecurityConfig = SecurityConfig(),
                 node_ready_timeout_s=None,
                 superuser: Optional[SaslCredentials] = None,
                 skip_if_no_redpanda_log: bool = False,
                 pandaproxy_config: Optional[PandaproxyConfig] = None,
                 schema_registry_config: Optional[SchemaRegistryConfig] = None,
                 audit_log_config: Optional[AuditLogConfig] = None,
                 disable_cloud_storage_diagnostics=False,
                 cloud_storage_scrub_timeout_s=None,
                 rpk_node_config: Optional[RpkNodeConfig] = None):
        super(RedpandaService, self).__init__(
            context,
            num_brokers,
            extra_rp_conf=extra_rp_conf,
            resource_settings=resource_settings,
            si_settings=si_settings,
            superuser=superuser,
            skip_if_no_redpanda_log=skip_if_no_redpanda_log,
            disable_cloud_storage_diagnostics=disable_cloud_storage_diagnostics
        )
        self._security = security
        self._installer: RedpandaInstaller = RedpandaInstaller(self)
        self._pandaproxy_config = pandaproxy_config
        self._schema_registry_config = schema_registry_config
        self._audit_log_config = audit_log_config
        self._failure_injection_enabled = False
        self._tolerate_crashes = False
        self._rpk_node_config = rpk_node_config

        if node_ready_timeout_s is None:
            node_ready_timeout_s = RedpandaService.DEFAULT_NODE_READY_TIMEOUT_SEC
        # apply min timeout rule. some tests may override this with larger
        # timeouts, so take the maximum.
        node_ready_timeout_min_s = self._context.globals.get(
            self.NODE_READY_TIMEOUT_MIN_SEC_KEY, node_ready_timeout_s)
        node_ready_timeout_s = max(node_ready_timeout_s,
                                   node_ready_timeout_min_s)
        self.node_ready_timeout_s = node_ready_timeout_s

        if cloud_storage_scrub_timeout_s is None:
            cloud_storage_scrub_timeout_s = RedpandaService.DEFAULT_CLOUD_STORAGE_SCRUB_TIMEOUT_SEC
        self.cloud_storage_scrub_timeout_s = cloud_storage_scrub_timeout_s

        self._extra_node_conf = {}
        for node in self.nodes:
            self._extra_node_conf[node] = extra_node_conf or dict()

        if log_config is not None:
            self._log_config = log_config
        else:
            if log_level is None:
                self._log_level = self._context.globals.get(
                    self.LOG_LEVEL_KEY, self.DEFAULT_LOG_LEVEL)
            else:
                self._log_level = log_level
            self._log_config = LoggingConfig(
                self._log_level, {
                    'exception': 'debug',
                    'io': 'debug',
                    'seastar_memory': 'debug',
                    'dns_resolver': 'info'
                })

        self._started: Set[ClusterNode] = set()

        self._raise_on_errors = self._context.globals.get(
            self.RAISE_ON_ERRORS_KEY, True)

        self._cloud_storage_client: S3Client | ABSClient | None = None

        # enable asan abort / core dumps by default
        self._environment = dict(
            ASAN_OPTIONS=
            "abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1")

        # If lsan_suppressions.txt exists, then include it
        if os.path.exists(LSAN_SUPPRESSIONS_FILE):
            self.logger.debug(f'{LSAN_SUPPRESSIONS_FILE} exists')
            self._environment[
                'LSAN_OPTIONS'] = f'suppressions={LSAN_SUPPRESSIONS_FILE}'
        else:
            self.logger.debug(f'{LSAN_SUPPRESSIONS_FILE} does not exist')

        # ubsan, halt at first violation and include a stack trace
        # in the logs.
        ubsan_opts = 'print_stacktrace=1:halt_on_error=1:abort_on_error=1'
        if os.path.exists(UBSAN_SUPPRESSIONS_FILE):
            ubsan_opts += f":suppressions={UBSAN_SUPPRESSIONS_FILE}"

        self._environment['UBSAN_OPTIONS'] = ubsan_opts

        if environment is not None:
            self._environment.update(environment)

        self.config_file_lock = threading.Lock()

        self._saved_executable = False

        self._tls_cert = None
        self._init_tls()

        # Each time we start a node and write out its node_config (redpanda.yaml),
        # stash a copy here so that we can quickly look up e.g. addresses later.
        self._node_configs = {}

        self._seed_servers = self.nodes

        self._expect_max_controller_records = 1000

    def redpanda_env_preamble(self):
        # Pass environment variables via FOO=BAR shell expressions
        return " ".join([f"{k}={v}" for (k, v) in self._environment.items()])

    def set_seed_servers(self, node_list):
        assert len(node_list) > 0
        self._seed_servers = node_list

    def set_environment(self, environment: dict[str, str]):
        self._environment.update(environment)

    def unset_environment(self, keys: list):
        for k in keys:
            try:
                del self._environment[k]
            except KeyError:
                pass

    def set_extra_node_conf(self, node, conf):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        self._extra_node_conf[node] = conf

    def set_security_settings(self, settings):
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
                self, "redpanda.service.admin")

    def sasl_enabled(self):
        return self._security.sasl_enabled()

    def mtls_identity_enabled(self):
        return self._security.mtls_identity_enabled()

    def endpoint_authn_method(self):
        return self._security.endpoint_authn_method

    def require_client_auth(self):
        return self._security.require_client_auth

    def get_node_memory_mb(self):
        if self._resource_settings.memory_mb is not None:
            self.logger.info(f"get_node_memory_mb: got from ResourceSettings")
            return self._resource_settings.memory_mb
        elif self._dedicated_nodes is False:
            self.logger.info(
                f"get_node_memory_mb: using ResourceSettings default")
            return self._resource_settings.DEFAULT_MEMORY_MB
        else:
            self.logger.info(f"get_node_memory_mb: fetching from node")
            # Assume nodes are symmetric, so we can just ask one
            # how much memory it has.
            node = self.nodes[0]
            line = node.account.ssh_output("cat /proc/meminfo | grep MemTotal",
                                           timeout_sec=10)
            # Output line is like "MemTotal:       32552236 kB"
            memory_kb = int(line.strip().split()[1])
            return memory_kb / 1024

    def get_node_cpu_count(self):
        if self._resource_settings.num_cpus is not None:
            self.logger.info(f"get_node_cpu_count: got from ResourceSettings")
            return self._resource_settings.num_cpus
        elif self._dedicated_nodes is False:
            self.logger.info(
                f"get_node_cpu_count: using ResourceSettings default")
            return self._resource_settings.DEFAULT_NUM_CPUS
        else:
            self.logger.info(f"get_node_cpu_count: fetching from node")

            # Assume nodes are symmetric, so we can just ask one
            node = self.nodes[0]
            core_count_str = node.account.ssh_output(
                "cat /proc/cpuinfo | grep ^processor | wc -l", timeout_sec=10)
            return int(core_count_str.strip())

    def get_node_disk_free(self):
        # Assume nodes are symmetric, so we can just ask one
        node = self.nodes[0]

        if node.account.exists(self.PERSISTENT_ROOT):
            df_path = self.PERSISTENT_ROOT
        else:
            # If dir doesn't exist yet, use the parent.
            df_path = os.path.dirname(self.PERSISTENT_ROOT)

        df_out = node.account.ssh_output(f"df --output=avail {df_path}",
                                         timeout_sec=10)

        avail_kb = int(df_out.strip().split(b"\n")[1].strip())

        if not self.dedicated_nodes:
            # Assume docker images share a filesystem.  This may not
            # be the truth (e.g. in CI they get indepdendent XFS
            # filesystems), but it's the safe assumption on e.g.
            # a workstation.
            avail_kb = int(avail_kb / len(self.nodes))

        return avail_kb * 1024

    def get_node_disk_usage(self, node):
        """
        get disk usage for the redpanda volume on a particular node
        """

        for line in node.account.ssh_capture(
                f"df --block-size 1 {self.PERSISTENT_ROOT}", timeout_sec=10):
            self.logger.debug(line.strip())
            if self.PERSISTENT_ROOT in line:
                return int(line.split()[2])
        assert False, "couldn't parse df output"

    def _startup_poll_interval(self, first_start):
        """
        During startup, our eagerness depends on whether it's the first
        start, where we expect a redpanda node to start up very quickly,
        or a subsequent start where it may be more sedate as data replay
        takes place.
        """
        return 0.2 if first_start else 1.0

    def wait_for_membership(self, first_start, timeout_sec=30):
        self.logger.info("Waiting for all brokers to join cluster")

        wait_until(lambda: {n
                            for n in self._started
                            if self.registered(n)} == self._started,
                   timeout_sec=timeout_sec,
                   backoff_sec=self._startup_poll_interval(first_start),
                   err_msg="Cluster membership did not stabilize")

    def setup_azurite_dns(self):
        """
        Azure API relies on <container>.something DNS.  Doing DNS configuration
        with docker/podman is unstable, as it isn't consistent across operating
        systems.  Instead do it more crudely but robustly, but editing /etc/hosts.
        """
        azurite_ip = socket.gethostbyname(AZURITE_HOSTNAME)
        azurite_dns = f"{self.si_settings.cloud_storage_azure_storage_account}.blob.localhost"

        def update_hosts_file(node_name, path):
            ducktape_hosts = open(path, "r").read()
            if azurite_dns not in ducktape_hosts:
                ducktape_hosts += f"\n{azurite_ip}   {azurite_dns}\n"
                self.logger.info(
                    f"Adding Azurite entry to {path} for node {node_name}, new content:"
                )
                self.logger.info(ducktape_hosts)
                with open(path, 'w') as f:
                    f.write(ducktape_hosts)
            else:
                self.logger.debug(
                    f"Azurite /etc/hosts entry already present on {node_name}:"
                )
                self.logger.debug(ducktape_hosts)

        # Edit /etc/hosts on the node where ducktape is running
        update_hosts_file("ducktape", "/etc/hosts")

        def setup_node_dns(node):
            tmpfile = f"/tmp/{node.name}_hosts"
            node.account.copy_from(f"/etc/hosts", tmpfile)
            update_hosts_file(node.name, tmpfile)
            node.account.copy_to(tmpfile, f"/etc/hosts")

        # Edit /etc/hosts on Redpanda nodes
        self.for_nodes(self.nodes, setup_node_dns)

    def start(self,
              nodes=None,
              clean_nodes=True,
              start_si=True,
              expect_fail: bool = False,
              auto_assign_node_id: bool = False,
              omit_seeds_on_idx_one: bool = True,
              node_config_overrides={}):
        """
        Start the service on all nodes.

        :param expect_fail: if true, expect redpanda nodes to terminate shortly
                            after starting.  Raise exception if they don't.
        """
        to_start = nodes if nodes is not None else self.nodes
        assert all((node in self.nodes for node in to_start))
        self.logger.info("%s: starting service" % self.who_am_i())

        first_start = self._start_time < 0
        if first_start:
            self._start_time = time.time()

            if self._si_settings and self._si_settings.cloud_storage_type is CloudStorageType.ABS and self._si_settings.cloud_storage_azure_storage_account == SISettings.ABS_AZURITE_ACCOUNT:
                self.setup_azurite_dns()

        self.logger.debug(
            self.who_am_i() +
            ": killing processes and attempting to clean up before starting")

        def clean_one(node):
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
                    self.logger.debug("%s: skip cleaning node" %
                                      self.who_am_i(node))
            except Exception:
                self.logger.exception(
                    f"Error cleaning node {node.account.hostname}:")
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

        def start_one(node):
            node_overrides = node_config_overrides[
                node] if node in node_config_overrides else {}
            self.logger.debug("%s: starting node" % self.who_am_i(node))
            self.start_node(node,
                            first_start=first_start,
                            expect_fail=expect_fail,
                            auto_assign_node_id=auto_assign_node_id,
                            omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                            override_cfg_params=node_overrides)

        try:
            self.for_nodes(to_start, start_one)
        except TimeoutError as e:
            if expect_fail:
                raise e
            if "failed to start within" in str(e):
                self.logger.debug(
                    f"Checking for crashes after start-up error: {e}")
                self.raise_on_crash()
            raise e

        if expect_fail:
            # If we got here without an exception, it means we failed as expected
            return

        if self._start_duration_seconds < 0:
            self._start_duration_seconds = time.time() - self._start_time

        if not self._skip_create_superuser:
            self._admin.create_user(*self._superuser)

        self.wait_for_membership(first_start=first_start)

        self.logger.info("Verifying storage is in expected state")
        storage = self.storage()
        for node in storage.nodes:
            if node not in to_start:
                continue
            unexpected_ns = set(node.ns) - {"redpanda"}
            if unexpected_ns:
                for ns in unexpected_ns:
                    self.logger.error(
                        f"node {node.name}: unexpected namespace: {ns}, "
                        f"topics: {set(node.ns[ns].topics)}")
                raise RuntimeError("Unexpected files in data directory")

            unexpected_rp_topics = set(
                node.ns["redpanda"].topics) - {"controller", "kvstore"}
            if unexpected_rp_topics:
                self.logger.error(
                    f"node {node.name}: unexpected topics in redpanda namespace: "
                    f"{unexpected_rp_topics}")
                raise RuntimeError("Unexpected files in data directory")

        if self.sasl_enabled():
            username, password, algorithm = self._superuser
            self._security_config = dict(security_protocol='SASL_PLAINTEXT',
                                         sasl_mechanism=algorithm,
                                         sasl_plain_username=username,
                                         sasl_plain_password=password,
                                         request_timeout_ms=30000,
                                         api_version_auto_timeout_ms=3000)

    def write_crl_file(self, node: ClusterNode, ca: tls.CertificateAuthority):
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
            node.account.mkdirs(
                os.path.dirname(RedpandaService.TLS_SERVER_KEY_FILE))
            node.account.copy_to(cert.key, RedpandaService.TLS_SERVER_KEY_FILE)

            self.logger.info(
                f"Writing Redpanda node tls cert file: {RedpandaService.TLS_SERVER_CRT_FILE}"
            )
            self.logger.debug(open(cert.crt, "r").read())
            node.account.mkdirs(
                os.path.dirname(RedpandaService.TLS_SERVER_CRT_FILE))
            node.account.copy_to(cert.crt, RedpandaService.TLS_SERVER_CRT_FILE)

            self.logger.info(
                f"Writing Redpanda node P12 file: {RedpandaService.TLS_SERVER_P12_FILE}"
            )
            self.logger.debug("P12 file is binary encoded")
            node.account.mkdirs(
                os.path.dirname(RedpandaService.TLS_SERVER_P12_FILE))
            node.account.copy_to(cert.p12_file,
                                 RedpandaService.TLS_SERVER_P12_FILE)

            self.logger.info(
                f"Writing Redpanda node tls ca cert file: {RedpandaService.TLS_CA_CRT_FILE}"
            )
            self.logger.debug(open(ca.crt, "r").read())
            node.account.mkdirs(
                os.path.dirname(RedpandaService.TLS_CA_CRT_FILE))
            node.account.copy_to(ca.crt, RedpandaService.TLS_CA_CRT_FILE)
            node.account.ssh(f"chmod 755 {RedpandaService.TLS_CA_CRT_FILE}")

            assert ca.crl is not None, "Missing CRL file"
            self.write_crl_file(node, ca)

            node.account.copy_to(ca.crt,
                                 RedpandaService.SYSTEM_TLS_CA_CRT_FILE)
            node.account.ssh(
                f"chmod 755 {RedpandaService.SYSTEM_TLS_CA_CRT_FILE}")
            node.account.ssh(f"update-ca-certificates")

            if self._pandaproxy_config is not None:
                self._pandaproxy_config.maybe_write_client_certs(
                    node, self.logger, PandaproxyConfig.PP_TLS_CLIENT_KEY_FILE,
                    PandaproxyConfig.PP_TLS_CLIENT_CRT_FILE)
                self._pandaproxy_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._pandaproxy_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._pandaproxy_config.truststore_file = RedpandaService.TLS_CA_CRT_FILE
                self._pandaproxy_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

            if self._schema_registry_config is not None:
                self._schema_registry_config.maybe_write_client_certs(
                    node, self.logger,
                    SchemaRegistryConfig.SR_TLS_CLIENT_KEY_FILE,
                    SchemaRegistryConfig.SR_TLS_CLIENT_CRT_FILE)
                self._schema_registry_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._schema_registry_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._schema_registry_config.truststore_file = RedpandaService.TLS_CA_CRT_FILE
                self._schema_registry_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

            if self._audit_log_config is not None:
                self._audit_log_config.maybe_write_client_certs(
                    node, self.logger,
                    AuditLogConfig.AUDIT_LOG_TLS_CLIENT_KEY_FILE,
                    AuditLogConfig.AUDIT_LOG_TLS_CLIENT_CRT_FILE)
                self._audit_log_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._audit_log_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._audit_log_config.truststore_file = RedpandaService.TLS_CA_CRT_FILE
                self._audit_log_config.crl_file = RedpandaService.TLS_CA_CRL_FILE

    def start_redpanda(self, node, extra_cli: list[str] = []):
        preamble, res_args = self._resource_settings.to_cli(
            dedicated_node=self._dedicated_nodes)

        # each node will create its own copy of the .profraw file
        # since each node creates a redpanda broker.
        if self.cov_enabled():
            self._environment.update(
                dict(LLVM_PROFILE_FILE=
                     f"\"{RedpandaService.COVERAGE_PROFRAW_CAPTURE}\""))

        env_preamble = self.redpanda_env_preamble()

        cmd = (
            f"{preamble} {env_preamble} nohup {self.find_binary('redpanda')}"
            f" --redpanda-cfg {RedpandaService.NODE_CONFIG_FILE}"
            f" {self._log_config.to_args()} "
            " --abort-on-seastar-bad-alloc "
            " --dump-memory-diagnostics-on-alloc-failure-kind=all "
            f" {res_args} "
            f" {' '.join(extra_cli)}"
            f" >> {RedpandaService.STDOUT_STDERR_CAPTURE} 2>&1 &")

        node.account.ssh(cmd)

    def check_node(self, node):
        pid = self.redpanda_pid(node)
        if not pid:
            self.logger.warn(f"No redpanda PIDs found on {node.name}")
            return False

        if not node.account.exists(f"/proc/{pid}"):
            self.logger.warn(f"PID {pid} (node {node.name}) dead")
            return False

        # fall through
        return True

    def all_up(self):
        def check_node(node):
            pid = self.redpanda_pid(node)
            if not pid:
                self.logger.warn(f"No redpanda PIDs found on {node.name}")
                return False

            if not node.account.exists(f"/proc/{pid}"):
                self.logger.warn(f"PID {pid} (node {node.name}) dead")
                return False

            # fall through
            return True

        return all(self.for_nodes(self._started, check_node))

    def signal_redpanda(self, node, signal=signal.SIGKILL, idempotent=False):
        """
        :param idempotent: if true, then kill-like signals are ignored if
                           the process is already gone.
        """
        pid = self.redpanda_pid(node)
        if pid is None:
            if idempotent and signal in {signal.SIGKILL, signal.SIGTERM}:
                return
            else:
                raise RuntimeError(
                    f"Can't signal redpanda on node {node.name}, it isn't running"
                )

        node.account.signal(pid, signal, allow_fail=False)

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

    def lsof_node(self, node: ClusterNode, filter: Optional[str] = None):
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

    def is_node_ready(self, node):
        """
        Calls Admin API's v1/status/ready endpoint to verify if the node
        is ready
        """
        status = None
        try:
            status = Admin.ready(node).get("status")
        except requests.exceptions.ConnectionError:
            self.logger.debug(
                f"node {node.name} not yet accepting connections")
            return False
        except:
            self.logger.exception(
                f"error on getting status from {node.account.hostname}")
            raise
        if status != "ready":
            self.logger.debug(
                f"status of {node.account.hostname} isn't ready: {status}")
            return False
        return True

    def start_node(self,
                   node,
                   override_cfg_params=None,
                   timeout=None,
                   write_config=True,
                   first_start=False,
                   expect_fail: bool = False,
                   auto_assign_node_id: bool = False,
                   omit_seeds_on_idx_one: bool = True,
                   skip_readiness_check: bool = False,
                   node_id_override: int | None = None,
                   extra_cli: list[str] = []):
        """
        Start a single instance of redpanda. This function will not return until
        redpanda appears to have started successfully. If redpanda does not
        start within a timeout period the service will fail to start. Thus this
        function also acts as an implicit test that redpanda starts quickly.
        """
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        self.write_openssl_config_file(node)

        if write_config:
            self.write_node_conf_file(
                node,
                override_cfg_params,
                auto_assign_node_id=auto_assign_node_id,
                omit_seeds_on_idx_one=omit_seeds_on_idx_one,
                node_id_override=node_id_override)

        if timeout is None:
            timeout = self.node_ready_timeout_s

        if self.dedicated_nodes:
            # When running on dedicated nodes, we should always be running on XFS.  If we
            # aren't, it's probably an accident that can easily cause spurious failures
            # and confusion, so be helpful and fail out early.
            fs = node.account.ssh_output(
                f"stat -f -c %T {self.PERSISTENT_ROOT}",
                timeout_sec=10).strip()
            if fs != b'xfs':
                raise RuntimeError(
                    f"Non-XFS filesystem {fs} at {self.PERSISTENT_ROOT} on {node.name}"
                )

        def start_rp():
            self.start_redpanda(node, extra_cli=extra_cli)

            if expect_fail:
                wait_until(
                    lambda: self.redpanda_pid(node) == None,
                    timeout_sec=10,
                    backoff_sec=0.2,
                    err_msg=
                    f"Redpanda processes did not terminate on {node.name} during startup as expected"
                )
            elif not skip_readiness_check:
                wait_until(
                    lambda: self.is_node_ready(node),
                    timeout_sec=timeout,
                    backoff_sec=self._startup_poll_interval(first_start),
                    err_msg=
                    f"Redpanda service {node.account.hostname} failed to start within {timeout} sec",
                    retry_on_exc=True)

        self.logger.debug(f"Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        if not expect_fail:
            self._started.add(node)

    def start_node_with_rpk(self, node, additional_args="", clean_node=True):
        """
        Start a single instance of redpanda using rpk. similar to start_node,
        this function will not return until redpanda appears to have started
        successfully.
        """
        self.logger.debug(
            self.who_am_i() +
            ": killing processes and attempting to clean up before starting")
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

        env_vars = " ".join(
            [f"{k}={v}" for (k, v) in self._environment.items()])
        rpk = RpkRemoteTool(self, node)

        _, args = self._resource_settings.to_cli(
            dedicated_node=self._dedicated_nodes)
        additional_args += " " + args

        def start_rp():
            rpk.redpanda_start(RedpandaService.STDOUT_STDERR_CAPTURE,
                               additional_args, env_vars)

            wait_until(
                lambda: self.is_node_ready(node),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=
                f"Redpanda service {node.account.hostname} failed to start within 60 sec using rpk",
                retry_on_exc=True)

        self.logger.debug(f"Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        self._started.add(node)

        # We need to manually read the config from the file and add it
        # to _node_configs since we use rpk to write the file instead of
        # the write_node_conf_file method.
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)
            with open(os.path.join(d, "redpanda.yaml")) as f:
                actual_config = yaml.full_load(f.read())
                self._node_configs[node] = actual_config

    def _log_node_process_state(self, node):
        """
        For debugging issues around starting and stopping processes: log
        which processes are running and which ports are in use. Additionally,
        capture detailed memory usage for the top 3 memory-consuming processes.
        """

        self.logger.debug(
            f"Gathering process and port usage information on {node.name}...")

        # Capture general process information
        process_lines = []
        for line in node.account.ssh_capture("ps aux --sort=-%mem",
                                             timeout_sec=30):
            process_lines.append(line.strip())
            self.logger.debug(line.strip())

        # Capture network information
        for line in node.account.ssh_capture("netstat -panelot",
                                             timeout_sec=30):
            self.logger.debug(line.strip())

        # Analyze memory usage in detail for the top 3 processes
        self.logger.debug(
            "Gathering detailed memory usage for the top 3 memory-consuming processes..."
        )
        for process_line in process_lines[
                1:4]:  # Skip header, get top 3 processes
            fields = process_line.split()
            pid = fields[1]
            mem_usage = fields[3]
            self.logger.debug(
                f"Process PID: {pid}, Memory Usage: {mem_usage}%")
            self.logger.debug(f"Memory map for PID {pid}:")
            for pmap_line in node.account.ssh_capture(f"pmap {pid}",
                                                      timeout_sec=30):
                self.logger.debug(pmap_line.strip())

    def start_service(self, node, start):
        # Maybe the service collides with something that wasn't cleaned up
        # properly: let's peek at what's going on on the node before starting it.
        self._log_node_process_state(node)

        try:
            start()
        except:
            # In case our failure to start is something like an "address in use", we
            # would like to know what else is going on on this node.
            self.logger.warn(
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
                signature_version=self.si_settings.
                cloud_storage_signature_version,
                before_call_headers=self.si_settings.before_call_headers,
                use_fips_endpoint=self.si_settings.use_fips_endpoint(),
                addressing_style=self.si_settings.addressing_style)

            self.logger.debug(
                f"Creating S3 bucket: {self.si_settings.cloud_storage_bucket}")
        elif self.si_settings.cloud_storage_type == CloudStorageType.ABS:
            # Make sure that use_bucket_cleanup_policy if False for ABS
            self.logger.warning("Turning off use_bucket_cleanup_policy "
                                "as it is not implemented for Azure/ABS")
            self.si_settings.use_bucket_cleanup_policy = False
            self._cloud_storage_client = ABSClient(
                logger=self.logger,
                storage_account=self.si_settings.
                cloud_storage_azure_storage_account,
                shared_key=self.si_settings.cloud_storage_azure_shared_key,
                endpoint=self.si_settings.endpoint_url)
            self.logger.debug(
                f"Creating ABS container: {self.si_settings.cloud_storage_bucket}"
            )
        else:
            raise RuntimeError(
                f"Unsuported cloud_storage_type: {self.si_settings.cloud_storage_type}"
            )

        if not self.si_settings.bypass_bucket_creation:
            assert self.si_settings.cloud_storage_bucket, "No SI bucket configured"
            self.cloud_storage_client.create_bucket(
                self.si_settings.cloud_storage_bucket)

        # If the test has requested to use a bucket cleanup policy then we
        # attempt to create one which will remove everything from the bucket
        # after one day.
        # This is a time optimization to avoid waiting hours cleaning up tiny
        # objects created by scale tests.
        if self.si_settings.use_bucket_cleanup_policy:
            self.cloud_storage_client.create_expiration_policy(
                bucket=self.si_settings.cloud_storage_bucket, days=1)

    @property
    def cloud_storage_client(self):
        assert self._cloud_storage_client, 'cloud storage client not available - did you call start_si()?'
        return self._cloud_storage_client

    def delete_bucket_from_si(self):
        self.logger.info(
            f"cloud_storage_cleanup_strategy = {self.si_settings.cloud_storage_cleanup_strategy}"
        )

        if self.si_settings.cloud_storage_cleanup_strategy == CloudStorageCleanupStrategy.ALWAYS_SMALL_BUCKETS_ONLY:
            bucket_is_small = True
            max_object_count = 3000

            # See if the bucket is small enough
            t = time.time()
            for i, m in enumerate(
                    self.cloud_storage_client.list_objects(
                        self.si_settings.cloud_storage_bucket)):
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

        elif self.si_settings.cloud_storage_cleanup_strategy == CloudStorageCleanupStrategy.IF_NOT_USING_LIFECYCLE_RULE:
            if self.si_settings.use_bucket_cleanup_policy:
                self.logger.info(
                    f"Skipping deletion of bucket/container: {self.si_settings.cloud_storage_bucket}. "
                    "Using a cleanup policy instead.")
                return
        else:
            raise ValueError(
                f"Unimplemented cloud storage cleanup strategy {self.si_settings.cloud_storage_cleanup_strategy}"
            )

        self.logger.debug(
            f"Deleting bucket/container: {self.si_settings.cloud_storage_bucket}"
        )

        assert self.si_settings.cloud_storage_bucket, f"missing bucket : {self.si_settings.cloud_storage_bucket}"
        t = time.time()
        self.cloud_storage_client.empty_and_delete_bucket(
            self.si_settings.cloud_storage_bucket,
            parallel=self.dedicated_nodes)

        self.logger.info(
            f"Emptying and deleting bucket {self.si_settings.cloud_storage_bucket} took {time.time() - t}s"
        )

    def get_objects_from_si(self):
        assert self.cloud_storage_client and self._si_settings and self._si_settings.cloud_storage_bucket, \
                f"bad si config {self.cloud_storage_client} : {self._si_settings.cloud_storage_bucket if self._si_settings else self._si_settings}"
        return self.cloud_storage_client.list_objects(
            self._si_settings.cloud_storage_bucket)

    def partitions(self, topic_name=None):
        """
        Return partition metadata for the topic.
        """
        kc = KafkaCat(self)
        md = kc.metadata()

        result = []

        def make_partition(topic_name, p):
            index = p["partition"]
            leader_id = p["leader"]
            leader = None if leader_id == -1 else self.get_node_by_id(
                leader_id)
            replicas = [self.get_node_by_id(r["id"]) for r in p["replicas"]]
            return Partition(topic_name, index, leader, replicas)

        for topic in md["topics"]:
            if topic["topic"] == topic_name or topic_name is None:
                result.extend(
                    make_partition(topic["topic"], p)
                    for p in topic["partitions"])

        return result

    def set_cluster_config_to_null(self, name: str, **kwargs):
        self.set_cluster_config(values={name: None}, **kwargs)

    def set_cluster_config(self,
                           values: dict,
                           expect_restart: bool = False,
                           admin_client: Optional[Admin] = None,
                           timeout: int = 10,
                           tolerate_stopped_nodes=False):
        """
        Update cluster configuration and wait for all nodes to report that they
        have seen the new config.

        :param values: dict of property name to value.
        :param expect_restart: set to true if you wish to permit a node restart for needs_restart=yes properties.
                               If you set such a property without this flag, an assertion error will be raised.
        """
        if admin_client is None:
            admin_client = self._admin

        patch_result = admin_client.patch_cluster_config(upsert=values,
                                                         remove=[])
        new_version = patch_result['config_version']

        self._wait_for_config_version(
            new_version,
            expect_restart,
            timeout,
            admin_client=admin_client,
            tolerate_stopped_nodes=tolerate_stopped_nodes)

    def enable_development_feature_support(self, key: Optional[int] = None):
        """
        Enable experimental feature support.

        The key must be equal to the current broker time expressed as unix epoch
        in seconds, and be within 1 hour.
        """
        key = int(time.time()) if key == None else key
        self.set_cluster_config(
            dict(
                enable_developmental_unrecoverable_data_corrupting_features=key,
            ))

    def _wait_for_config_version(self,
                                 config_version,
                                 expect_restart: bool,
                                 timeout: int,
                                 admin_client: Optional[Admin] = None,
                                 tolerate_stopped_nodes=False):
        admin_client = admin_client or self._admin
        if tolerate_stopped_nodes:
            started_node_ids = {self.node_id(n) for n in self.started_nodes()}

        def is_ready():
            status = admin_client.get_cluster_config_status(
                node=self.controller())
            ready = all([
                n['config_version'] >= config_version for n in status if
                not tolerate_stopped_nodes or n['node_id'] in started_node_ids
            ])

            return ready, status

        # The version check is >= to permit other config writes to happen in
        # the background, including the write to cluster_id that happens
        # early in the cluster's lifetime
        config_status = wait_until_result(
            is_ready,
            timeout_sec=timeout,
            backoff_sec=0.5,
            err_msg=
            f"Config status versions did not converge on {config_version}")

        any_restarts = any(n['restart'] for n in config_status)
        if any_restarts and expect_restart:
            self.restart_nodes(self.nodes)
            # Having disrupted the cluster with a restart, wait for the controller
            # to be available again before returning to the caller, so that they do
            # not have to worry about subsequent configuration actions failing.
            admin_client.await_stable_leader(namespace="redpanda",
                                             topic="controller",
                                             partition=0)
        elif any_restarts:
            raise AssertionError(
                "Nodes report restart required but expect_restart is False")

    def set_feature_active(self,
                           feature_name: str,
                           active: bool,
                           *,
                           timeout_sec: int = 15):
        target_state = 'active' if active else 'disabled'
        cur_state = self.get_feature_state(feature_name)
        if active and cur_state == 'unavailable':
            # If we have just restarted after an upgrade, wait for cluster version
            # to progress and for the feature to become available.
            self.await_feature(feature_name,
                               'available',
                               timeout_sec=timeout_sec)
        self._admin.put_feature(feature_name, {"state": target_state})
        self.await_feature(feature_name, target_state, timeout_sec=timeout_sec)

    def get_feature_state(self,
                          feature_name: str,
                          node: ClusterNode | None = None):
        f = self._admin.get_features(node=node)
        by_name = dict((f['name'], f) for f in f['features'])
        try:
            state = by_name[feature_name]['state']
        except KeyError:
            state = None
        return state

    def await_feature(self,
                      feature_name: str,
                      await_state: str,
                      *,
                      timeout_sec: int,
                      nodes: list[ClusterNode] | None = None):
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

    def monitor_log(self, node):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        return node.account.monitor_log(RedpandaService.STDOUT_STDERR_CAPTURE)

    def raise_on_crash(self,
                       log_allow_list: list[str | re.Pattern] | None = None):
        """
        Check if any redpanda nodes are unexpectedly not running,
        or if any logs contain segfaults or assertions.

        Call this after a test fails, to generate a more useful
        error message, rather than having failures on "timeouts" which
        are actually redpanda crashes.
        """

        allow_list = []
        if log_allow_list:
            for a in log_allow_list:
                if should_compile(a):
                    a = re.compile(a)
                allow_list.append(a)

        def is_allowed_log_line(line: str) -> bool:
            for a in allow_list:
                if a.search(line) is not None:
                    return True
            return False

        crashes = []
        # We log long encoded AWS/GCP headers that occasionally have 'SEGV' in
        # them by chance
        cloud_header_strings = [
            'x-amz-id', 'x-amz-request', 'x-guploader-uploadid'
        ]
        for node in self.nodes:
            self.logger.info(
                f"Scanning node {node.account.hostname} log for errors...")

            crash_log = None
            for line in node.account.ssh_capture(
                    f"grep -e SEGV -e Segmentation\\ fault -e [Aa]ssert -e Sanitizer {RedpandaService.STDOUT_STDERR_CAPTURE} || true",
                    timeout_sec=30):
                if 'SEGV' in line and any(
                    [h in line.lower() for h in cloud_header_strings]):
                    continue

                if is_allowed_log_line(line):
                    self.logger.warn(
                        f"Ignoring allow-listed log line '{line}'")
                    continue

                if "No such file or directory" not in line:
                    crash_log = line
                    break

            if crash_log:
                crashes.append((node, crash_log))

        if not crashes:
            # Even if there is no assertion or segfault, look for unexpectedly
            # not-running processes
            for node in self._started:
                if not self.redpanda_pid(node):
                    crashes.append(
                        (node, "Redpanda process unexpectedly stopped"))

        if crashes:
            if self._tolerate_crashes:
                self.logger.warn(
                    f"Detected crashes, but RedpandaService is configured to allow them: {crashes}"
                )
            else:
                raise NodeCrash(crashes)

    def raw_metrics(
            self,
            node,
            metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS):
        assert node in self._started, f"Node {node.account.hostname} is not started"

        url = f"http://{node.account.hostname}:9644/{metrics_endpoint.value}"
        resp = requests.get(url, timeout=10)
        assert resp.status_code == 200
        return resp.text

    def metrics(self,
                node,
                metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS):
        '''Parse the prometheus text format metric from a given node.'''
        text = self.raw_metrics(node, metrics_endpoint)
        return text_string_to_metric_families(text)

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
        except:
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

        manifests_to_dump = []
        for o in self.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket):
            key = o.key
            if key_dump_limit > 0:
                self.logger.info(f"  {key} {o.content_length}")
                key_dump_limit -= 1

            # Gather manifest.json and topic_manifest.json files
            if 'manifest.json' in key or 'manifest.bin' in key and manifest_dump_limit > 0:
                manifests_to_dump.append(key)
                manifest_dump_limit -= 1

            if manifest_dump_limit == 0 and key_dump_limit == 0:
                break

        archive_basename = "cloud_diagnostics.zip"
        archive_path = os.path.join(
            TestContext.results_dir(self._context, self._context.test_index),
            archive_basename)
        with zipfile.ZipFile(archive_path, mode='w') as archive:
            for m in manifests_to_dump:
                self.logger.info(f"Fetching manifest {m}")
                body = self.cloud_storage_client.get_object_data(
                    self.si_settings.cloud_storage_bucket, m)
                filename = m.replace("/", "_")

                with archive.open(filename, "w") as outstr:
                    outstr.write(body)

                # Decode binary manifests for convenience, but don't give up
                # if we fail
                if "/manifest.bin" in m:
                    try:
                        decoded = RpStorageTool(
                            self.logger).decode_partition_manifest(body)
                    except Exception as e:
                        self.logger.warn(f"Failed to decode {m}: {e}")
                    else:
                        json_filename = f"{filename}_decoded.json"
                        json_bytes = json.dumps(decoded, indent=2)
                        self.logger.info(
                            f"Decoded manifest {m} to {len(json_bytes)} of JSON from {len(body)} bytes of serde"
                        )
                        with archive.open(json_filename, "w") as outstr:
                            outstr.write(json_bytes.encode())

    def raise_on_storage_usage_inconsistency(self):
        def tracked(fstat):
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
            file, size = fstat
            if len(file.parents) == 1:
                return False
            if file.parents[-2].name == "cloud_storage_cache":
                return False
            if "compaction.staging" in file.name:
                # compaction staging files are temporary and are generally
                # cleaned up after compaction finishes, or at next round of
                # compaction if a file was stranded. during shutdown of any
                # generic test we don't have a good opportunity to force this to
                # happen without placing a lot of restrictions on shutdown. for
                # the time being just ignore these.
                return False
            return True

        def inspect_node(node):
            """
            Fetch reported size from admin interface, query the local file
            system, and compute a percentage difference between reported and
            observed disk usage.
            """
            try:
                observed = list(self.data_stat(node))
                reported = self._admin.get_local_storage_usage(node)

                observed_total = sum(s for _, s in filter(tracked, observed))
                reported_total = reported["data"] + reported[
                    "index"] + reported["compaction"]

                diff = observed_total - reported_total
                return (abs(diff / reported_total),
                        reported["reclaimable_by_retention"] / reported_total,
                        reported, reported_total, observed, observed_total)
            except:
                return 0.0, 0.0, None, None, None, None

        # inspect the node and check that we fall below a 5% + reclaimabled_by_retention%
        # threshold difference. at this point the test is over, but we allow for a couple
        # retries in case things need to settle.
        nodes = [(n, None) for n in self.nodes]
        for _ in range(3):
            retries = []
            results = self.for_nodes(nodes, lambda n: (n, inspect_node(n)))
            for (node, (pct_diff, reclaimable_diff, *deets)) in results:
                if pct_diff > 0.05 + reclaimable_diff:
                    retries.append(
                        (node, (pct_diff, reclaimable_diff, *deets)))

            if not retries:
                # all good
                return
            nodes = retries
            time.sleep(5)

        # if one or more nodes failed the check, then report information about
        # the situation and fail the test by raising an exception.
        nodes = []
        max_node, max_diff = retries[0][0], retries[0][1][0]
        for node, deets in retries:
            node_name = f"{self.idx(node)}:{node.account.hostname}"
            nodes.append(node_name)
            pct_diff, reclaimable_diff, reported, reported_total, observed, observed_total = deets
            if pct_diff > max_diff:
                max_diff = pct_diff
                max_node = node
            diff = observed_total - reported_total
            for file, size in observed:
                self.logger.debug(
                    f"Observed file [{node_name}]: {size:7} {file}")
            self.logger.warn(
                f"Storage usage [{node_name}]: obs {observed_total:7} rep {reported_total:7} diff {diff:7} pct {pct_diff} reclaimable_pct {reclaimable_diff}"
            )

        max_node = f"{self.idx(max_node)}:{max_node.account.hostname}"
        raise RuntimeError(
            f"Storage usage inconsistency on nodes {nodes}: max difference {max_diff} on node {max_node}"
        )

    def decode_backtraces(self):
        """
        Decodes redpanda backtraces if any of them are present
        :return: None
        """

        for node in self.nodes:
            if not node.account.exists(RedpandaService.STDOUT_STDERR_CAPTURE):
                # Log many not exist if node never started
                continue

            self.logger.info(
                f"Decoding backtraces on {node.account.hostname}.")
            cmd = '/opt/scripts/seastar-addr2line'
            cmd += f" -e {self.find_raw_binary('redpanda')}"
            cmd += f" -f {RedpandaService.STDOUT_STDERR_CAPTURE}"
            cmd += f" > {RedpandaService.BACKTRACE_CAPTURE} 2>&1"
            cmd += f" && find {RedpandaService.BACKTRACE_CAPTURE} -type f -size 0 -delete"

            try:
                node.account.ssh(cmd)
            except:
                # We run during teardown on failures, so if something
                # goes wrong we must not raise, or we would usurp
                # the original exception that caused the failure.
                self.logger.exception("Failed to run seastar-addr2line")

    def rp_install_path(self):
        if self._installer._started:
            # The installer sets up binaries to always use /opt/redpanda.
            return "/opt/redpanda"
        return self._context.globals.get("rp_install_path_root", None)

    def find_binary(self, name):
        rp_install_path_root = self.rp_install_path()
        return f"{rp_install_path_root}/bin/{name}"

    def find_raw_binary(self, name):
        """
        Like `find_binary`, but find the underlying executable rather tha
        a shell wrapper.
        """
        rp_install_path_root = self.rp_install_path()
        return f"{rp_install_path_root}/libexec/{name}"

    def get_version(self, node):
        """
        Returns the redpanda binary version as a string.
        """
        env_preamble = self.redpanda_env_preamble()
        version_cmd = f"{env_preamble} {self.find_binary('redpanda')} --version"
        VERSION_LINE_RE = re.compile(".*(v\\d+\\.\\d+\\.\\d+).*")
        # NOTE: not all versions of Redpanda support the --version field, even
        # though they print out the version.
        version_lines = [
            l for l in node.account.ssh_capture(version_cmd, allow_fail=True, timeout_sec=10) \
                if VERSION_LINE_RE.match(l)
        ]
        assert len(version_lines) == 1, version_lines
        return VERSION_LINE_RE.findall(version_lines[0])[0]

    def get_version_int_tuple(self, node):
        version_str = self.get_version(node)
        return ri_int_tuple(RI_VERSION_RE.findall(version_str)[0])

    def stop(self, **kwargs):
        """
        Override default stop() to execude stop_node in parallel
        """
        self._stop_time = time.time()  # The last time stop is invoked
        self.logger.info("%s: exporting cluster config" % self.who_am_i())

        service_dir = os.path.join(
            TestContext.results_dir(self._context, self._context.test_index),
            self.service_id)
        cluster_config_filename = os.path.join(service_dir,
                                               "cluster_config.yaml")
        self.logger.debug("%s: cluster_config_filename %s" %
                          (self.who_am_i(), cluster_config_filename))

        if not os.path.isdir(service_dir):
            mkdir_p(service_dir)

        try:
            rpk = RpkTool(self)
            rpk.cluster_config_export(cluster_config_filename, True)
        except Exception as e:
            # Configuration is optional: if redpanda has e.g. crashed, you
            # will not be able to get it from the admin API
            self.logger.info(f"{self.who_am_i()}: error getting config: {e}")

        self.logger.info("%s: stopping service" % self.who_am_i())

        self.for_nodes(self.nodes, lambda n: self.stop_node(n, **kwargs))

        self._stop_duration_seconds = time.time() - self._stop_time

    def _set_trace_loggers_and_sleep(self, node, time_sec=10):
        """
        For debugging issues around stopping processes: set the log level to
        trace on all loggers.
        """
        # These tend to be exceptionally chatty, or don't provide much value.
        keep_existing = ['exception', 'io', 'seastar_memory', 'assert']
        try:
            loggers = self._admin.get_loggers(node)
            for logger in loggers:
                if logger in keep_existing:
                    continue
                self._admin.set_log_level(logger, 'trace', time_sec)
            time.sleep(time_sec)
        except Exception as e:
            self.logger.warn(f"Error setting trace loggers: {e}")

    def stop_node(self, node, timeout=None, forced=False):
        pid = self.redpanda_pid(node)
        if pid is not None:
            node.account.signal(pid,
                                signal.SIGKILL if forced else signal.SIGTERM,
                                allow_fail=False)

        if timeout is None:
            timeout = 30

        try:
            wait_until(
                lambda: self.redpanda_pid(node) == None,
                timeout_sec=timeout,
                err_msg=
                f"Redpanda node {node.account.hostname} failed to stop in {timeout} seconds"
            )
        except TimeoutError:
            sleep_sec = 10
            self.logger.warn(
                f"Timed out waiting for stop on {node.name}, setting log_level to 'trace' and sleeping for {sleep_sec}s"
            )
            self._set_trace_loggers_and_sleep(node, time_sec=sleep_sec)
            self.logger.warn(f"Node {node.name} status:")
            self._log_node_process_state(node)
            raise

        self.remove_from_started_nodes(node)

    def remove_from_started_nodes(self, node):
        if node in self._started:
            self._started.remove(node)

    def add_to_started_nodes(self, node):
        self._started.add(node)

    def clean(self, **kwargs):
        super().clean(**kwargs)
        # If we bypassed bucket creation, there is no need to try to delete it.
        if self._si_settings and self._si_settings.bypass_bucket_creation:
            self.logger.info(
                f"Skipping deletion of bucket/container: {self.si_settings.cloud_storage_bucket},"
                "because its creation was bypassed.")
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

    def clean_node(self,
                   node,
                   preserve_logs=False,
                   preserve_current_install=False):
        # These are allow_fail=True to allow for a race where kill_process finds
        # the PID, but then the process has died before it sends the SIGKILL.  This
        # should be safe against actual failures to of the process to stop, because
        # we're using SIGKILL which does not require the process's cooperation.
        node.account.kill_process("redpanda",
                                  clean_shutdown=False,
                                  allow_fail=True)
        if node.account.exists(RedpandaService.PERSISTENT_ROOT):
            hidden_cache_folder = f'{RedpandaService.PERSISTENT_ROOT}/.cache'
            self.logger.debug(
                f"Checking for presence of {hidden_cache_folder}")
            if node.account.exists(hidden_cache_folder):
                self.logger.debug(
                    f"Seeing {hidden_cache_folder}, removing that specifically first"
                )
                node.account.remove(hidden_cache_folder)
            if node.account.sftp_client.listdir(
                    RedpandaService.PERSISTENT_ROOT):
                if not preserve_logs:
                    node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/*")
                else:
                    node.account.remove(
                        f"{RedpandaService.PERSISTENT_ROOT}/data/*")
        if node.account.exists(RedpandaService.NODE_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.NODE_CONFIG_FILE}")
        if node.account.exists(RedpandaService.RPK_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.RPK_CONFIG_FILE}")
        if node.account.exists(RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE):
            node.account.remove(
                f"{RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE}")
        if not preserve_logs and node.account.exists(
                self.EXECUTABLE_SAVE_PATH):
            node.account.remove(self.EXECUTABLE_SAVE_PATH)

        if node.account.exists(RedpandaService.SYSTEM_TLS_CA_CRT_FILE):
            node.account.remove(RedpandaService.SYSTEM_TLS_CA_CRT_FILE)
            node.account.ssh(f"update-ca-certificates")

        if node.account.exists(RedpandaService.TEMP_OSSL_CONFIG_FILE):
            node.account.remove(RedpandaService.TEMP_OSSL_CONFIG_FILE)

        if not preserve_current_install or not self._installer._started:
            # Reset the binaries to use the original binaries.
            # NOTE: if the installer hasn't been started, there is no
            # installation to preserve!
            self._installer.reset_current_install([node])

    def remove_local_data(self, node):
        node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/data/*")

    def redpanda_pid(self, node):
        try:
            cmd = "pgrep --list-full --exact redpanda"
            for line in node.account.ssh_capture(cmd, timeout_sec=10):
                # Ignore SSH commands that lookup the version of redpanda
                # by running `redpanda --version` like in `self.get_version(node)`
                if "--version" in line:
                    continue

                self.logger.debug(f"pgrep output: {line}")

                # The pid is listed first, that's all we need
                return int(line.split()[0])
            return None
        except RemoteCommandError as e:
            # 1 - No processes matched or none of them could be signalled.
            if e.exit_status == 1:
                return None

            raise e

    def started_nodes(self) -> List[ClusterNode]:
        return list(self._started)

    def render(self, path, **kwargs):
        with self.config_file_lock:
            return super(RedpandaService, self).render(path, **kwargs)

    @staticmethod
    def get_node_fqdn(node):
        ip = socket.gethostbyname(node.account.hostname)
        hostname = node.account.ssh_output(
            cmd=f"dig -x {ip} +short",
            timeout_sec=10).decode('utf-8').split('\n')[0].removesuffix(".")
        fqdn = node.account.ssh_output(
            cmd=f"host {hostname}",
            timeout_sec=10).decode('utf-8').split(' ')[0]
        return fqdn

    def write_openssl_config_file(self, node):
        conf = self.render("openssl.cnf",
                           fips_conf_file=os.path.join(
                               self.rp_install_path(),
                               "openssl/fipsmodule.cnf"))
        self.logger.debug(
            f'Writing {RedpandaService.TEMP_OSSL_CONFIG_FILE} to {node.name}:\n{conf}'
        )
        node.account.create_file(RedpandaService.TEMP_OSSL_CONFIG_FILE, conf)

    def get_openssl_config_file_path(self) -> str:
        path = os.path.join(self.rp_install_path(),
                            self.OPENSSL_CONFIG_FILE_BASE)
        if self.rp_install_path() != "/opt/redpanda":
            # If we aren't using an 'installed' Redpanda instance, the openssl config file
            # located in the install path will not point to the correct location of the FIPS
            # module config file.  We generate an openssl config file just for this purpose
            # see write_openssl_config_file above
            path = RedpandaService.TEMP_OSSL_CONFIG_FILE

        self.logger.debug(
            f'OpenSSL Config File Path: {path} ({self.rp_install_path()})')
        return path

    def get_openssl_modules_directory(self) -> str:
        path = os.path.join(self.rp_install_path(),
                            self.OPENSSL_MODULES_PATH_BASE)

        self.logger.debug(
            f'OpenSSL Modules Directory: {path} ({self.rp_install_path()})')
        return path

    def write_node_conf_file(self,
                             node,
                             override_cfg_params=None,
                             auto_assign_node_id=False,
                             omit_seeds_on_idx_one=True,
                             node_id_override: int | None = None):
        """
        Write the node config file for a redpanda node: this is the YAML representation
        of Redpanda's `node_config` class.  Distinct from Redpanda's _cluster_ configuration
        which is written separately.
        """
        node_info = {self.idx(n): n for n in self.nodes}

        include_seed_servers = True
        if node_id_override:
            assert auto_assign_node_id == False, "Can not use node id override when auto assigning node ids"
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

        try:
            cur_ver = self.get_version_int_tuple(node)
        except:
            cur_ver = None

        if self._security.tls_provider and self._rpk_node_config is not None:
            self._rpk_node_config.ca_file = RedpandaService.TLS_CA_CRT_FILE

        conf = self.render("redpanda.yaml",
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
                           rpk_node_config=self._rpk_node_config)

        def is_fips_capable(node) -> bool:
            cur_ver = self._installer.installed_version(node)
            return cur_ver == RedpandaInstaller.HEAD or cur_ver >= (24, 2, 1)

        if in_fips_environment() and is_fips_capable(node):
            self.logger.info(
                "Operating in FIPS environment, enabling FIPS mode for Redpanda"
            )
            doc = yaml.full_load(conf)
            doc["redpanda"].update(
                dict(fips_mode="enabled",
                     openssl_config_file=self.get_openssl_config_file_path(),
                     openssl_module_directory=self.
                     get_openssl_modules_directory()))
            conf = yaml.dump(doc)

        if override_cfg_params or node in self._extra_node_conf:
            doc = yaml.full_load(conf)
            doc["redpanda"].update(self._extra_node_conf[node])
            self.logger.debug(
                f"extra_node_conf[{node.name}]: {self._extra_node_conf[node]}")
            if override_cfg_params:
                self.logger.debug(
                    "Setting custom node configuration options: {}".format(
                        override_cfg_params))
                doc["redpanda"].update(override_cfg_params)
            conf = yaml.dump(doc)

        if self._security.tls_provider:
            p12_password = self._security.tls_provider.p12_password(
                node) if self._security.tls_provider.use_pkcs12_file(
                ) else None
            tls_config = [
                dict(
                    enabled=True,
                    require_client_auth=self.require_client_auth(),
                    name=n,
                    truststore_file=RedpandaService.TLS_CA_CRT_FILE,
                    crl_file=RedpandaService.TLS_CA_CRL_FILE,
                ) for n in ["dnslistener", "iplistener"]
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

        self.logger.info("Writing Redpanda node config file: {}".format(
            RedpandaService.NODE_CONFIG_FILE))
        self.logger.debug(conf)
        node.account.create_file(RedpandaService.NODE_CONFIG_FILE, conf)

        self._node_configs[node] = yaml.full_load(conf)

    def find_path_to_rpk(self) -> str:
        return f'{self._context.globals.get("rp_install_path_root", None)}/bin/rpk'

    def write_bootstrap_cluster_config(self):
        conf = copy.deepcopy(self.CLUSTER_CONFIG_DEFAULTS)

        cur_ver = self._installer.installed_version(self.nodes[0])
        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (22, 2, 7):
            # this configuration property was introduced in 22.2, ensure
            # it doesn't appear in older configurations
            del conf['log_segment_size_jitter_percent']

        if self._extra_rp_conf:
            self.logger.debug(
                "Setting custom cluster configuration options: {}".format(
                    self._extra_rp_conf))
            conf.update(self._extra_rp_conf)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (24, 2, 1):
            # this configuration property was introduced in 24.2, ensure
            # it doesn't appear in older configurations
            conf.pop("cloud_storage_url_style", None)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (23, 3, 1):
            # this configuration property was introduced in 23.3, ensure
            # it doesn't appear in older configurations
            conf.pop('cloud_storage_enable_scrubbing', None)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (23, 2, 1):
            # this configuration property was introduced in 23.2, ensure
            # it doesn't appear in older configurations
            conf.pop('retention_local_strict', None)

        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (22, 2, 1):
            # this configuration property was introduced in 22.2.1, ensure
            # it doesn't appear in older configurations
            conf.pop('cloud_storage_credentials_source', None)

        if self._security.enable_sasl:
            self.logger.debug("Enabling SASL in cluster configuration")
            conf.update(dict(enable_sasl=True))
        if self._security.kafka_enable_authorization is not None:
            self.logger.debug(
                f"Setting kafka_enable_authorization: {self._security.kafka_enable_authorization} in cluster configuration"
            )
            conf.update(
                dict(kafka_enable_authorization=self._security.
                     kafka_enable_authorization))
        if self._security.sasl_mechanisms is not None:
            self.logger.debug(
                f"Setting sasl_mechanisms: {self._security.sasl_mechanisms} in cluster configuration"
            )
            conf.update(dict(sasl_mechanisms=self._security.sasl_mechanisms))

        if self._security.http_authentication is not None:
            self.logger.debug(
                f"Setting http_authentication: {self._security.http_authentication} in cluster configuration"
            )
            conf.update(
                dict(http_authentication=self._security.http_authentication))

        # Only override `rpk_path` if not already provided
        if (cur_ver == RedpandaInstaller.HEAD
                or cur_ver >= (24, 3, 1)) and 'rpk_path' not in conf:
            # Introduced rpk_path to v24.3
            rpk_path = self.find_path_to_rpk()
            conf.update(dict(rpk_path=rpk_path))

        conf_yaml = yaml.dump(conf)
        for node in self.nodes:
            self.logger.info(
                "Writing bootstrap cluster config file {}:{}".format(
                    node.name, RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE))
            node.account.mkdirs(
                os.path.dirname(RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE))
            node.account.create_file(
                RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE, conf_yaml)

    def get_node_by_id(self, node_id):
        """
        Returns a node that has requested id or None if node is not found
        """
        for n in self.nodes:
            if self.node_id(n) == node_id:
                return n

        return None

    def registered(self, node):
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
                if b['node_id'] == node_id:
                    found = b
                    break

            if not found:
                self.logger.info(
                    f"registered: node {node.name} not yet found in peer {peer.name}'s broker list ({admin_brokers})"
                )
                return False
            else:
                if not found['is_alive']:
                    self.logger.info(
                        f"registered: node {node.name} found in {peer.name}'s broker list ({admin_brokers}) but not yet marked as alive"
                    )
                    return False
                self.logger.debug(
                    f"registered: node {node.name} now visible in peer {peer.name}'s broker list ({admin_brokers})"
                )

        if self.brokers():  # Conditional in case Kafka API turned off
            auth_args = {}
            if self.sasl_enabled():
                auth_args = {
                    'username': self._superuser.username,
                    'password': self._superuser.password,
                    'algorithm': self._superuser.algorithm
                }
            client = PythonLibrdkafka(self,
                                      tls_cert=self._tls_cert,
                                      **auth_args)
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
                    timeout=10)
            except requests.exceptions.RequestException:
                continue

            if r.status_code != 200:
                continue
            else:
                resp_leader_id = r.json()['leader_id']
                if resp_leader_id != -1:
                    return self.get_node_by_id(resp_leader_id)

        return None

    @property
    def cache_dir(self):
        return os.path.join(RedpandaService.DATA_DIR, "cloud_storage_cache")

    def node_storage(self,
                     node,
                     sizes: bool = False,
                     scan_cache: bool = True) -> NodeStorage:
        """
        Retrieve a summary of storage on a node.

        :param sizes: if true, stat each segment file and record its size in the
                      `size` attribute of Segment.
        :param scan_cache: if false, skip scanning the tiered storage cache; use
                           this if you are only interested in raft storage.
        """

        self.logger.debug(
            f"Starting storage checks for {node.name} sizes={sizes}")
        store = NodeStorage(node.name, RedpandaService.DATA_DIR,
                            self.cache_dir)
        script_path = inject_remote_script(node, "compute_storage.py")
        cmd = [
            "python3", script_path, f"--data-dir={RedpandaService.DATA_DIR}"
        ]
        if sizes:
            cmd.append("--sizes")
        output = node.account.ssh_output(shlex.join(cmd),
                                         combine_stderr=False,
                                         timeout_sec=10)
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
                    if not sizes:
                        continue
                    for segment, data in segments.items():
                        partition.set_segment_size(segment, data["size"])

        if scan_cache and self._si_settings is not None and node.account.exists(
                store.cache_dir):
            bytes = int(
                node.account.ssh_output(
                    f"du -s \"{store.cache_dir}\"",
                    combine_stderr=False).strip().split()[0])
            objects = int(
                node.account.ssh_output(
                    f"find \"{store.cache_dir}\" -type f | wc -l",
                    combine_stderr=False).strip())
            indices = int(
                node.account.ssh_output(
                    f"find \"{store.cache_dir}\" -type f -name \"*.index\" | wc -l",
                    combine_stderr=False).strip())
            store.set_cache_stats(NodeCacheStorage(bytes, objects, indices))

        self.logger.debug(
            f"Finished storage checks for {node.name} sizes={sizes}")

        return store

    def storage(self,
                all_nodes: bool = False,
                sizes: bool = False,
                scan_cache: bool = True):
        """
        :param all_nodes: if true, report on all nodes, otherwise only report
                          on started nodes.
        :param sizes: if true, stat each segment file and record its size in the
                      `size` attribute of Segment.
        :param scan_cache: if false, skip scanning the tiered storage cache; use
                           this if you are only interested in raft storage.

        :returns: instances of ClusterStorage
        """
        store = ClusterStorage()
        self.logger.debug(
            f"Starting storage checks all_nodes={all_nodes} sizes={sizes}")
        nodes = self.nodes if all_nodes else self._started

        def compute_node_storage(node):
            s = self.node_storage(node, sizes=sizes, scan_cache=scan_cache)
            store.add_node(s)

        self.for_nodes(nodes, compute_node_storage)
        self.logger.debug(
            f"Finished storage checks all_nodes={all_nodes} sizes={sizes}")
        return store

    def copy_data(self, dest, node):
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
        found = []
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
        cmd = f"find {RedpandaService.DATA_DIR} -type f -exec stat -c '%n %s' '{{}}' \;"
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

    def broker_address(self, node, listener: str = "dnslistener"):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        assert node in self._started
        cfg = self._node_configs[node]
        if cfg['redpanda']['kafka_api']:
            if isinstance(cfg['redpanda']['kafka_api'], list):
                for entry in cfg['redpanda']['kafka_api']:
                    if entry['name'] == listener:
                        return f'{entry["address"]}:{entry["port"]}'
            else:
                entry = cfg["redpanda"]["kafka_api"]
                return f'{entry["address"]}:{entry["port"]}'
        else:
            return None

    def admin_endpoint(self, node):
        assert node in self.nodes, f"Node {node.account.hostname} is not started"
        return f"{node.account.hostname}:9644"

    def admin_endpoints_list(self):
        brokers = [self.admin_endpoint(n) for n in self._started]
        random.shuffle(brokers)
        return brokers

    def admin_endpoints(self):
        return ",".join(self.admin_endpoints_list())

    def brokers(self, limit=None, listener: str = "dnslistener") -> str:
        return ",".join(self.brokers_list(limit, listener))

    def brokers_list(self,
                     limit=None,
                     listener: str = "dnslistener") -> list[str]:
        brokers = [
            self.broker_address(n, listener)
            for n in list(self._started)[:limit]
        ]
        brokers = [b for b in brokers if b is not None]
        random.shuffle(brokers)
        return brokers

    def schema_reg(self, limit=None) -> str:
        schema_reg = [
            f"http://{n.account.hostname}:8081"
            for n in list(self._started)[:limit]
        ]
        return ",".join(schema_reg)

    def _extract_samples(self, metrics, sample_pattern: str,
                         node: ClusterNode) -> list[MetricSamples]:
        '''Override superclass method by ensuring node is type ClusterNode.'''

        return super()._extract_samples(metrics, sample_pattern, node)

    def metrics_sample(
        self,
        sample_pattern: str,
        nodes=None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> Optional[MetricSamples]:
        '''
        Query metrics for a single sample using fuzzy name matching. This
        interface matches the sample pattern against sample names, and requires
        that exactly one (family, sample) match the query. All values for the
        sample across the requested set of nodes are returned in a flat array.

        None will be returned if less than one (family, sample) matches.
        An exception will be raised if more than one (family, sample) matches.

        For example, the query:

            redpanda.metrics_sample("under_replicated")

        will return an array containing MetricSample instances for each node and
        core/shard in the cluster. Each entry will correspond to a value from:

            family = vectorized_cluster_partition_under_replicated_replicas
            sample = vectorized_cluster_partition_under_replicated_replicas
        '''

        if nodes is None:
            nodes = self.nodes

        return self._metrics_sample(sample_pattern, nodes, metrics_endpoint)

    def metrics_samples(
        self,
        sample_patterns: list[str],
        nodes=None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> dict[str, MetricSamples]:
        '''
        Query metrics for multiple sample names using fuzzy matching.
        Similar to metrics_sample(), but works with multiple patterns.
        '''
        nodes = nodes or self.nodes
        return self._metrics_samples(sample_patterns, nodes, metrics_endpoint)

    def shards(self):
        """
        Fetch the max shard id for each node.
        """
        shards_per_node = {}
        for node in self._started:
            num_shards = 0
            metrics = self.metrics(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_reactor_utilization":
                        num_shards = max(num_shards,
                                         int(sample.labels["shard"]))
            assert num_shards > 0
            shards_per_node[self.idx(node)] = num_shards
        return shards_per_node

    def cov_enabled(self):
        cov_option = self._context.globals.get(self.COV_KEY,
                                               self.DEFAULT_COV_OPT)
        if cov_option == "ON":
            return True
        elif cov_option == "OFF":
            return False

        self.logger.warn(f"{self.COV_KEY} should be one of 'ON', or 'OFF'")
        return False

    def count_log_node(self, node: ClusterNode, pattern: str):
        accum = 0
        for line in node.account.ssh_capture(
                f"grep \"{pattern}\" {RedpandaService.STDOUT_STDERR_CAPTURE} || true",
                timeout_sec=60):
            # We got a match
            self.logger.debug(f"Found {pattern} on node {node.name}: {line}")
            accum += 1

        return accum

    def search_log_node(self, node: ClusterNode, pattern: str):
        for line in node.account.ssh_capture(
                f"grep \"{pattern}\" {RedpandaService.STDOUT_STDERR_CAPTURE} || true",
                timeout_sec=60):
            # We got a match
            self.logger.debug(f"Found {pattern} on node {node.name}: {line}")
            return True

        return False

    def search_log_any(self,
                       pattern: str,
                       nodes: Optional[list[ClusterNode]] = None):
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

    def search_log_all(self,
                       pattern: str,
                       nodes: Optional[list[ClusterNode]] = None):
        # Test helper for grepping the redpanda log
        # The design follows python's  built-in all() function.
        # https://docs.python.org/3/library/functions.html#all

        # :param pattern: the string to search for
        # :param nodes: a list of nodes to run grep on
        # :return:  true if `pattern` is found in all nodes
        if nodes is None:
            nodes = self.nodes

        for node in nodes:
            exit_status = node.account.ssh(
                f"grep \"{pattern}\" {RedpandaService.STDOUT_STDERR_CAPTURE}",
                allow_fail=True)

            # Match not found
            if exit_status != 0:
                self.logger.debug(
                    f"Did not find {pattern} on node {node.name}")
                return False

        # Fall through, match on all nodes
        return True

    def wait_for_controller_snapshot(self,
                                     node,
                                     prev_mtime=0,
                                     prev_start_offset=0,
                                     timeout_sec=30):
        def check():
            snap_path = os.path.join(self.DATA_DIR,
                                     'redpanda/controller/0_0/snapshot')
            try:
                stat = node.account.sftp_client.stat(snap_path)
                mtime = stat.st_mtime
                size = stat.st_size
            except FileNotFoundError:
                mtime = 0
                size = 0

            controller_status = self._admin.get_controller_status(node)
            self.logger.info(f"node {node.account.hostname}: "
                             f"controller status: {controller_status}, "
                             f"snapshot size: {size}, mtime: {mtime}")

            so = controller_status['start_offset']
            return (mtime > prev_mtime and so > prev_start_offset, (mtime, so))

        return wait_until_result(check, timeout_sec=timeout_sec, backoff_sec=1)

    def _get_object_storage_report(self,
                                   tolerate_empty_object_storage=False,
                                   timeout=300) -> dict[str, Any]:
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
        vars = {"RUST_LOG": "warn"}
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
                vars[
                    "AWS_ACCESS_KEY_ID"] = self.si_settings.cloud_storage_access_key
                vars[
                    "AWS_SECRET_ACCESS_KEY"] = self.si_settings.cloud_storage_secret_key
        elif self.si_settings.cloud_storage_type == CloudStorageType.ABS:
            backend = "azure"
            if self.si_settings.cloud_storage_azure_storage_account == SISettings.ABS_AZURITE_ACCOUNT:
                vars["AZURE_STORAGE_USE_EMULATOR"] = "true"
                # We do not use the SISettings.endpoint_url, because that includes the account
                # name in the URL, and the `object_store` crate used in the scanning tool
                # assumes that when the emulator is used, the account name should always
                # appear in the path.
                vars[
                    "AZURITE_BLOB_STORAGE_URL"] = f"http://{AZURITE_HOSTNAME}:{AZURITE_PORT}"
            else:
                vars["AZURE_STORAGE_CONNECTION_STRING"] = cast(
                    ABSClient, self.cloud_storage_client).conn_str
                vars[
                    "AZURE_STORAGE_ACCOUNT_KEY"] = self.si_settings.cloud_storage_azure_shared_key
                vars[
                    "AZURE_STORAGE_ACCOUNT_NAME"] = self.si_settings.cloud_storage_azure_storage_account

        if self.si_settings.endpoint_url and self.si_settings.endpoint_url == 'https://storage.googleapis.com':
            backend = "gcp"
        # Pick an arbitrary node to run the scrub from
        node = self.nodes[0]

        bucket = self.si_settings.cloud_storage_bucket
        environment = ' '.join(f'{k}=\"{v}\"' for k, v in vars.items())
        bucket_arity = sum(1 for _ in self.get_objects_from_si())
        effective_timeout = min(max(bucket_arity * 5, 20), timeout)
        self.logger.info(
            f"num objects in the {bucket=}: {bucket_arity}, will apply {effective_timeout=}"
        )
        output, stderr = ssh_output_stderr(
            self,
            node,
            f"{environment} rp-storage-tool --backend {backend} scan-metadata --source {bucket}",
            allow_fail=True,
            timeout_sec=effective_timeout)

        # if stderr contains a WARN logline, log it as DEBUG, since this is mostly related to debugging rp-storage-tool itself
        if re.search(b'\[\S+ WARN', stderr) is not None:
            self.logger.debug(f"rp-storage-tool stderr output: {stderr}")

        report = {}
        try:
            report = json.loads(output)
        except:
            self.logger.error(
                f"Error running bucket scrub: {output=} {stderr=}")
            if not tolerate_empty_object_storage:
                raise
        else:
            self.logger.info(json.dumps(report, indent=2))

        return report

    def raise_on_cloud_storage_inconsistencies(self,
                                               inconsistencies: list[str],
                                               run_timeout=300):
        """
        like stop_and_scrub_object_storage, use rp-storage-tool to explicitly check for inconsistencies,
        but without stopping the cluster.
        """
        report = self._get_object_storage_report(
            tolerate_empty_object_storage=True, timeout=run_timeout)
        fatal_anomalies = set(k for k, v in report.items()
                              if len(v) > 0 and k in inconsistencies)
        if fatal_anomalies:
            self.logger.error(
                f"Found fatal inconsistencies in object storage: {json.dumps(report, indent=2)}"
            )
            raise RuntimeError(
                f"Object storage reports fatal anomalies of type {fatal_anomalies}"
            )

    def stop_and_scrub_object_storage(self, run_timeout=300):
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
        report = self._get_object_storage_report(timeout=scrub_timeout)

        # It is legal for tiered storage to leak objects under
        # certain circumstances: this will remain the case until
        # we implement background scrub + modify test code to
        # insist on Redpanda completing its own scrub before
        # we externally validate
        # (https://github.com/redpanda-data/redpanda/issues/9072)
        # see https://github.com/redpanda-data/redpanda/issues/17502 for ntr_no_topic_manifest, remove it as soon as it's fixed
        permitted_anomalies = {
            "segments_outside_manifest", "ntr_no_topic_manifest"
        }

        # Whether any anomalies were found
        any_anomalies = any(len(v) for v in report['anomalies'].values())

        # List of fatal anomalies found
        fatal_anomalies = set(k for k, v in report['anomalies'].items()
                              if len(v) and k not in permitted_anomalies)

        if not any_anomalies:
            self.logger.info(f"No anomalies in object storage scrub")
        elif not fatal_anomalies:
            self.logger.info(
                f"Non-fatal anomalies in remote storage: {json.dumps(report, indent=2)}"
            )
        else:
            # Tests may declare that they expect some anomalies, e.g. if they
            # intentionally damage the data.
            if self.si_settings.is_damage_expected(fatal_anomalies):
                self.logger.warn(
                    f"Tolerating anomalies in remote storage: {json.dumps(report, indent=2)}"
                )
            else:
                self.logger.error(
                    f"Fatal anomalies in remote storage: {json.dumps(report, indent=2)}"
                )
                raise RuntimeError(
                    f"Object storage scrub detected fatal anomalies of type {fatal_anomalies}"
                )

    def maybe_do_internal_scrub(self):
        if not self._si_settings:
            return

        cloud_partitions = self.wait_for_manifest_uploads()
        results = self.wait_for_internal_scrub(cloud_partitions)

        if results:
            self.logger.error("Fatal anomalies reported by internal scrub: "
                              f"{json.dumps(results, indent=2)}")
            raise RuntimeError(
                f"Internal object storage scrub detected fatal anomalies: {results}"
            )
        else:
            self.logger.info(f"No anomalies in internal object storage scrub")

    def wait_for_manifest_uploads(self) -> set[Partition]:
        cloud_storage_partitions: set[Partition] = set()

        def all_partitions_uploaded_manifest():
            manifest_not_uploaded = []
            for p in self.partitions():
                try:
                    status = self._admin.get_partition_cloud_storage_status(
                        p.topic, p.index, node=p.leader)
                except HTTPError as he:
                    if he.response.status_code == 404:
                        # Old redpanda, doesn't have this endpoint.  We can't
                        # do our upload check.
                        continue
                    else:
                        raise

                remote_write = status["cloud_storage_mode"] in {
                    "full", "write_only"
                }

                if remote_write:
                    # TODO(vlad): do this differently?
                    # Create new partition tuples since the replicas list is not hashable
                    cloud_storage_partitions.add(
                        Partition(topic=p.topic,
                                  index=p.index,
                                  leader=p.leader,
                                  replicas=None))

                has_uploaded_manifest = status[
                    "metadata_update_pending"] is False or status.get(
                        'ms_since_last_manifest_upload', None) is not None
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
            wait_until(all_partitions_uploaded_manifest,
                       timeout_sec=30 + timeout,
                       backoff_sec=1)

        return cloud_storage_partitions

    def wait_for_internal_scrub(self, cloud_storage_partitions):
        """
        Configure the scrubber such that it will run aggresively
        until the entire partition is scrubbed. Once that happens,
        the scrubber will pause due to the `cloud_storage_full_scrub_interval_ms`
        config. Returns the aggregated anomalies for all partitions after applying
        filtering for expected damage.
        """
        if not cloud_storage_partitions:
            return None

        self.set_cluster_config(
            {
                "cloud_storage_enable_scrubbing": True,
                "cloud_storage_partial_scrub_interval_ms": 100,
                "cloud_storage_full_scrub_interval_ms": 1000 * 60 * 10,
                "cloud_storage_scrubbing_interval_jitter_ms": 100,
                "cloud_storage_background_jobs_quota": 5000,
                "cloud_storage_housekeeping_interval_ms": 100,
                # Segment merging may resolve gaps in the log, so disable it
                "cloud_storage_enable_segment_merging": False,
                # Leadership moves may perturb the scrub, so disable it to
                # streamline the actions below.
                "enable_leader_balancer": False
            },
            tolerate_stopped_nodes=True)

        unavailable = set()
        for p in cloud_storage_partitions:
            try:
                leader_id = self._admin.await_stable_leader(topic=p.topic,
                                                            partition=p.index)

                self._admin.reset_scrubbing_metadata(
                    namespace="kafka",
                    topic=p.topic,
                    partition=p.index,
                    node=self.get_node_by_id(leader_id))
            except HTTPError as he:
                if he.response.status_code == 404:
                    # Old redpanda, doesn't have this endpoint.  We can't
                    # do our upload check.
                    unavailable.add(p)
                    continue
                else:
                    raise

        cloud_storage_partitions -= unavailable
        scrubbed = set()
        all_anomalies = []

        allowed_keys = set([
            "ns", "topic", "partition", "revision_id", "last_complete_scrub_at"
        ])

        expected_damage = self.si_settings.get_expected_damage()

        def filter_anomalies(detected):
            bad_delta_types = set(
                ["non_monotonical_delta", "mising_delta", "end_delta_smaller"])

            for anomaly_type in expected_damage:
                if anomaly_type == "ntpr_no_manifest":
                    detected.pop("missing_partition_manifest", None)
                if anomaly_type == "missing_segments":
                    detected.pop("missing_segments", None)
                if anomaly_type == "missing_spillover_manifests":
                    detected.pop("missing_spillover_manifests", None)
                if anomaly_type == "ntpr_bad_deltas":
                    if metas := detected.get("segment_metadata_anomalies"):
                        metas = [
                            m for m in metas
                            if m["type"] not in bad_delta_types
                        ]
                        if metas:
                            detected["segment_metadata_anomalies"] = metas
                        else:
                            detected.pop("segment_metadata_anomalies", None)
                if anomaly_type == "ntpr_overlap_offsets":
                    if metas := detected.get("segment_metadata_anomalies"):
                        metas = [
                            m for m in metas if m["type"] != "offset_overlap"
                        ]
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
                f"Waiting for {len(waiting_for)} partitions to be scrubbed")
            for p in waiting_for:
                result = self._admin.get_cloud_storage_anomalies(
                    namespace="kafka", topic=p.topic, partition=p.index)
                if "last_complete_scrub_at" in result:
                    scrubbed.add(p)

                    filter_anomalies(result)
                    if set(result.keys()) != allowed_keys:
                        all_anomalies.append(result)

            return len(waiting_for) == 0

        n_partitions = len(cloud_storage_partitions)
        timeout = (n_partitions // 100) * 60 + 120
        wait_until(all_partitions_scrubbed, timeout_sec=timeout, backoff_sec=5)

        return all_anomalies

    def set_expected_controller_records(self, max_records: Optional[int]):
        self._expect_max_controller_records = max_records

    def set_up_failure_injection(self, finject_cfg: FailureInjectionConfig,
                                 enabled: bool, nodes: list[ClusterNode],
                                 tolerate_crashes: bool):
        """
        Deploy the given failure injection configuration to the
        requested nodes. Should be called before
        `RedpandaService.start`.
        """
        tmp_file = "/tmp/failure_injection_config.json"
        finject_cfg.write_to_file(tmp_file)
        for node in nodes:
            node.account.mkdirs(
                os.path.dirname(RedpandaService.FAILURE_INJECTION_CONFIG_PATH))
            node.account.copy_to(tmp_file,
                                 RedpandaService.FAILURE_INJECTION_CONFIG_PATH)

            self._extra_node_conf[node].update({
                "storage_failure_injection_enabled":
                enabled,
                "storage_failure_injection_config_path":
                RedpandaService.FAILURE_INJECTION_CONFIG_PATH
            })

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

    def validate_controller_log(self):
        """
        This method is for use at end of tests, to detect issues that might
        lead to huge numbers of writes to the controller log (e.g. rogue
        loops/retries).

        Any test that intentionally
        """
        max_length = None
        for node in self.started_nodes():
            try:
                status = self._admin.get_controller_status(node=node)
                node_length = status['committed_index'] - max(
                    0, status['start_offset'] - 1)
            except Exception as e:
                self.logger.warn(
                    f"Failed to read controller status from {node.name}: {e}")
            else:
                if max_length is None or node_length > max_length:
                    max_length = node_length

        if max_length is None:
            self.logger.warn(
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

    def estimate_bytes_written(self):
        try:
            samples = self.metrics_sample(
                "vectorized_io_queue_total_write_bytes_total",
                nodes=self.started_nodes())
        except Exception as e:
            self.logger.warn(
                f"Cannot check metrics, did a test finish with all nodes down? ({e})"
            )
            return None

        if samples is not None and samples.samples:
            return sum(s.value for s in samples.samples)
        else:
            return None

    def wait_node_add_rebalance_finished(self,
                                         new_nodes,
                                         admin=None,
                                         min_partitions=5,
                                         progress_timeout=30,
                                         timeout=300,
                                         backoff=2):
        """Waits until the rebalance triggered by adding new nodes is finished."""

        new_node_names = [n.name for n in new_nodes]

        if admin is None:
            admin = Admin(self)
        started_at = time.monotonic()
        last_reconfiguring = set()
        last_bytes_moved = 0
        last_update = started_at

        while True:
            time.sleep(backoff)

            if time.monotonic() - last_update > progress_timeout:
                raise TimeoutError(
                    f"rebalance after adding nodes {new_node_names} "
                    "stopped making progress")

            if time.monotonic() - started_at > timeout:
                raise TimeoutError(
                    f"rebalance after adding nodes {new_node_names} "
                    "timed out")

            cur_reconfiguring = set()
            cur_bytes_moved = 0
            for p in admin.list_reconfigurations():
                cur_reconfiguring.add(
                    f"{p['ns']}/{p['topic']}/{p['partition']}")
                cur_bytes_moved += p['bytes_moved']

            if (cur_reconfiguring != last_reconfiguring
                    or cur_bytes_moved != last_bytes_moved):
                last_update = time.monotonic()

            last_reconfiguring = cur_reconfiguring
            last_bytes_moved = cur_bytes_moved

            if len(cur_reconfiguring) > 0:
                continue

            partition_counts = [
                len(admin.get_partitions(node=n)) for n in new_nodes
            ]
            if any(pc < min_partitions for pc in partition_counts):
                continue

            return

    def install_license(self):
        """Install a sample Enterprise License for testing Enterprise features during upgrades"""
        self.logger.debug("Installing an Enterprise License")
        license = sample_license(assert_exists=True)
        assert self._admin.put_license(license).status_code == 200, \
            "Configuring the Enterprise license failed (required for feature upgrades)"

        def license_observable():
            for node in self.started_nodes():
                license = self._admin.get_license(node)
                if license is None or license['loaded'] is not True:
                    return False
            return True

        self.logger.debug(
            f"Waiting for license to be observable by {len(self.started_nodes())} nodes"
        )
        wait_until(license_observable,
                   timeout_sec=15,
                   backoff_sec=1,
                   err_msg="Inserted license not observable in time")
        self.logger.debug("Enterprise License installed successfully")


def make_redpanda_service(context: TestContext,
                          num_brokers: int | None,
                          *,
                          extra_rp_conf=None,
                          **kwargs) -> RedpandaService:
    """Factory function for instatiating the appropriate RedpandaServiceBase subclass."""

    # https://github.com/redpanda-data/core-internal/issues/1002
    assert not is_redpanda_cloud(context), 'make_redpanda_service '  \
        + 'should not be called in a cloud test context'

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

    return RedpandaService(context,
                           num_brokers,
                           extra_rp_conf=extra_rp_conf,
                           **kwargs)


def make_redpanda_cloud_service(
        context: TestContext,
        *,
        min_brokers: int | None = None) -> RedpandaServiceCloud:
    """Create a RedpandaServiceCloud service. This can only be used in a test
    running against Redpanda Cloud or else it will throw."""

    assert is_redpanda_cloud(context), 'make_redpanda_cloud_service '  \
        + 'called but not in a cloud context (missing cloud_cluster in globals)'

    cloud_config = context.globals[
        RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG]

    config_profile_name = get_config_profile_name(cloud_config)

    return RedpandaServiceCloud(context,
                                config_profile_name=config_profile_name,
                                min_brokers=min_brokers if min_brokers else 1)


def make_redpanda_mixed_service(context: TestContext,
                                *,
                                min_brokers: int | None = 3):
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
