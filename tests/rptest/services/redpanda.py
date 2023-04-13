# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import concurrent.futures
import copy

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
from enum import Enum, IntEnum
from typing import Mapping, Optional, Tuple, Union, Any

import yaml
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from requests.exceptions import HTTPError
from rptest.archival.s3_client import S3Client
from rptest.archival.abs_client import ABSClient
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.local_filesystem_utils import mkdir_p
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode
from prometheus_client.parser import text_string_to_metric_families
from ducktape.errors import TimeoutError
from ducktape.tests.test import TestContext

from rptest.archival.abs_client import build_connection_string
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.services import tls
from rptest.services.admin import Admin
from rptest.services.redpanda_installer import RedpandaInstaller, VERSION_RE as RI_VERSION_RE, int_tuple as ri_int_tuple
from rptest.services.rolling_restarter import RollingRestarter
from rptest.services.storage import ClusterStorage, NodeStorage
from rptest.services.utils import BadLogLines, NodeCrash
from rptest.util import wait_until_result

Partition = collections.namedtuple('Partition',
                                   ['topic', 'index', 'leader', 'replicas'])

MetricSample = collections.namedtuple(
    'MetricSample', ['family', 'sample', 'node', 'value', 'labels'])

SaslCredentials = collections.namedtuple("SaslCredentials",
                                         ["username", "password", "algorithm"])
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

    # This is expected when tests are intentionally run on low memory configurations
    re.compile(r"Memory: '\d+' below recommended"),
    # A client disconnecting is not bad behaviour on redpanda's part
    re.compile(r"kafka rpc protocol.*(Connection reset by peer|Broken pipe)")
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
]

# Log errors emitted by refresh credentials system when cloud storage is enabled with IAM roles
# without a corresponding mock service set up to return credentials
IAM_ROLES_API_CALL_ALLOW_LIST = [
    re.compile(r'cloud_roles - .*api request failed'),
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
    "raft - .*recovery append entries error.*client_request_timeout"
]


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
    METRICS = 1
    PUBLIC_METRICS = 2


class CloudStorageType(IntEnum):
    # Use AWS S3 on dedicated nodes, or minio in docker
    S3 = 1
    # Use Azure ABS on dedicated nodes, or azurite in docker
    ABS = 2


def one_or_many(value):
    """
    Helper for reading `one_or_many_property` configs when
    we only care about getting one value out
    """
    if isinstance(value, list):
        return value[0]
    else:
        return value


def get_cloud_storage_type(applies_only_on: list(CloudStorageType) = None,
                           docker_use_arbitrary=False):
    """
    Returns a list(CloudStorageType) based on the "CLOUD_PROVIDER"
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

    cloud_provider = os.getenv("CLOUD_PROVIDER", "docker")
    if cloud_provider == "docker":
        if docker_use_arbitrary:
            cloud_storage_type = [CloudStorageType.S3]
        else:
            cloud_storage_type = [CloudStorageType.S3, CloudStorageType.ABS]
    elif cloud_provider in ("aws", "gcp"):
        cloud_storage_type = [CloudStorageType.S3]
    elif cloud_provider == "azure":
        cloud_storage_type = [CloudStorageType.ABS]

    if applies_only_on:
        cloud_storage_type = list(
            set(applies_only_on).intersection(cloud_storage_type))
    return cloud_storage_type


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

    GLOBAL_ABS_STORAGE_ACCOUNT = "abs_storage_account"
    GLOBAL_ABS_SHARED_KEY = "abs_shared_key"
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
                 cloud_storage_credentials_source: str = 'config_file',
                 cloud_storage_access_key: str = 'panda-user',
                 cloud_storage_secret_key: str = 'panda-secret',
                 cloud_storage_region: str = 'panda-region',
                 cloud_storage_api_endpoint: str = 'minio-s3',
                 cloud_storage_api_endpoint_port: int = 9000,
                 cloud_storage_cache_size: int = 160 * 1000000,
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
                 cloud_storage_housekeeping_interval_ms: Optional[int] = None,
                 fast_uploads=False):
        """
        :param fast_uploads: if true, set low upload intervals to help tests run
                             quickly when they wait for uploads to complete.
        """

        self.cloud_storage_type = get_cloud_storage_type()[0]
        if hasattr(test_context, 'injected_args') \
        and test_context.injected_args is not None \
        and 'cloud_storage_type' in test_context.injected_args:
            self.cloud_storage_type = test_context.injected_args[
                'cloud_storage_type']

        if self.cloud_storage_type == CloudStorageType.S3:
            self.cloud_storage_credentials_source = cloud_storage_credentials_source
            self.cloud_storage_access_key = cloud_storage_access_key
            self.cloud_storage_secret_key = cloud_storage_secret_key
            self.cloud_storage_region = cloud_storage_region
            self._cloud_storage_bucket = f'panda-bucket-{uuid.uuid1()}'

            self.cloud_storage_api_endpoint = cloud_storage_api_endpoint
            if test_context.globals.get(self.GLOBAL_CLOUD_PROVIDER,
                                        'aws') == 'gcp':
                self.cloud_storage_api_endpoint = 'storage.googleapis.com'
            self.cloud_storage_api_endpoint_port = cloud_storage_api_endpoint_port
        elif self.cloud_storage_type == CloudStorageType.ABS:
            self.cloud_storage_azure_shared_key = self.ABS_AZURITE_KEY
            self.cloud_storage_azure_storage_account = self.ABS_AZURITE_ACCOUNT

            self._cloud_storage_azure_container = f'panda-container-{uuid.uuid1()}'
            self.cloud_storage_api_endpoint = f'{self.cloud_storage_azure_storage_account}.blob.localhost'
            self.cloud_storage_api_endpoint_port = AZURITE_PORT
        else:
            assert False, f"Unexpected value provided for 'cloud_storage_type' injected arg: {self.cloud_storage_type}"

        self.log_segment_size = log_segment_size
        self.cloud_storage_cache_size = cloud_storage_cache_size
        self.cloud_storage_enable_remote_read = cloud_storage_enable_remote_read
        self.cloud_storage_enable_remote_write = cloud_storage_enable_remote_write
        self.cloud_storage_max_connections = cloud_storage_max_connections
        self.cloud_storage_disable_tls = cloud_storage_disable_tls
        self.cloud_storage_segment_max_upload_interval_sec = cloud_storage_segment_max_upload_interval_sec
        self.cloud_storage_manifest_max_upload_interval_sec = cloud_storage_manifest_max_upload_interval_sec
        self.cloud_storage_readreplica_manifest_sync_timeout_ms = cloud_storage_readreplica_manifest_sync_timeout_ms
        self.endpoint_url = f'http://{self.cloud_storage_api_endpoint}:{self.cloud_storage_api_endpoint_port}'
        self.bypass_bucket_creation = bypass_bucket_creation
        self.cloud_storage_housekeeping_interval_ms = cloud_storage_housekeeping_interval_ms

        if fast_uploads:
            self.cloud_storage_segment_max_upload_interval_sec = 10
            self.cloud_storage_manifest_max_upload_interval_sec = 1

        self._expected_damage_types = set()

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
        cloud_storage_credentials_source = test_context.globals.get(
            self.GLOBAL_CLOUD_STORAGE_CRED_SOURCE_KEY, 'config_file')
        cloud_storage_access_key = test_context.globals.get(
            self.GLOBAL_S3_ACCESS_KEY, None)
        cloud_storage_secret_key = test_context.globals.get(
            self.GLOBAL_S3_SECRET_KEY, None)
        cloud_storage_region = test_context.globals.get(
            self.GLOBAL_S3_REGION_KEY, None)

        # Enable S3 if AWS creds were given at globals
        if cloud_storage_credentials_source == 'aws_instance_metadata':
            logger.info("Running on AWS S3, setting IAM roles")
            self.cloud_storage_credentials_source = cloud_storage_credentials_source
            self.cloud_storage_access_key = None
            self.cloud_storage_secret_key = None
            self.endpoint_url = None  # None so boto auto-gens the endpoint url
            self.cloud_storage_disable_tls = False  # SI will fail to create archivers if tls is disabled
            self.cloud_storage_region = cloud_storage_region
        elif cloud_storage_credentials_source == 'config_file' and cloud_storage_access_key and cloud_storage_secret_key:
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
        else:
            logger.info('No AWS credentials supplied, assuming minio defaults')

    @property
    def cloud_storage_bucket(self):
        if self.cloud_storage_type == CloudStorageType.S3:
            return self._cloud_storage_bucket
        elif self.cloud_storage_type == CloudStorageType.ABS:
            return self._cloud_storage_azure_container

    # Call this to update the extra_rp_conf
    def update_rp_conf(self, conf) -> dict[str, Any]:
        if self.cloud_storage_type == CloudStorageType.S3:
            conf[
                "cloud_storage_credentials_source"] = self.cloud_storage_credentials_source
            conf["cloud_storage_access_key"] = self.cloud_storage_access_key
            conf["cloud_storage_secret_key"] = self.cloud_storage_secret_key
            conf["cloud_storage_region"] = self.cloud_storage_region
            conf["cloud_storage_bucket"] = self._cloud_storage_bucket
        elif self.cloud_storage_type == CloudStorageType.ABS:
            conf[
                'cloud_storage_azure_storage_account'] = self.cloud_storage_azure_storage_account
            conf[
                'cloud_storage_azure_container'] = self._cloud_storage_azure_container
            conf[
                'cloud_storage_azure_shared_key'] = self.cloud_storage_azure_shared_key

        conf["log_segment_size"] = self.log_segment_size
        conf["cloud_storage_enabled"] = True
        conf["cloud_storage_cache_size"] = self.cloud_storage_cache_size
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
        self.endpoint_authn_method: Optional[str] = None
        self.tls_provider: Optional[TLSProvider] = None
        self.require_client_auth: bool = True
        self.auto_auth: Optional[bool] = None

        # The rules to extract principal from mtls
        self.principal_mapping_rules = self.__DEFAULT_PRINCIPAL_MAPPING_RULES

    # sasl is required
    def sasl_enabled(self):
        return (self.kafka_enable_authorization is None and self.enable_sasl
                and self.endpoint_authn_method is None
                ) or self.endpoint_authn_method == "sasl"

    # principal is extracted from mtls distinguished name
    def mtls_identity_enabled(self):
        return self.endpoint_authn_method == "mtls_identity"


class LoggingConfig:
    def __init__(self, default_level: str, logger_levels={}):
        self.default_level = default_level
        self.logger_levels = logger_levels

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
        self.client_key: Optional[str] = None
        self.client_crt: Optional[str] = None
        self.require_client_auth: bool = True

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


class SchemaRegistryConfig(TlsConfig):
    SR_TLS_CLIENT_KEY_FILE = "/etc/redpanda/sr_client.key"
    SR_TLS_CLIENT_CRT_FILE = "/etc/redpanda/sr_client.crt"

    def __init__(self):
        super(SchemaRegistryConfig, self).__init__()


class RedpandaServiceBase(Service):
    def __init__(
        self,
        context,
        num_brokers,
        extra_rp_conf=None,
        resource_settings=None,
        si_settings=None,
    ):
        super(RedpandaServiceBase, self).__init__(context,
                                                  num_nodes=num_brokers)
        self._context = context
        self._extra_rp_conf = extra_rp_conf or dict()

        if si_settings is not None:
            self.set_si_settings(si_settings)
        else:
            self._si_settings = None

        if resource_settings is None:
            resource_settings = ResourceSettings()
        self._resource_settings = resource_settings

    def start_node(self, node, **kwargs):
        pass

    def stop_node(self, node, **kwargs):
        pass

    def clean_node(self, node, **kwargs):
        pass

    def restart_nodes(self,
                      nodes,
                      override_cfg_params=None,
                      start_timeout=None,
                      stop_timeout=None,
                      auto_assign_node_id=False,
                      omit_seeds_on_idx_one=True):

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
                        override_cfg_params,
                        timeout=start_timeout,
                        auto_assign_node_id=auto_assign_node_id,
                        omit_seeds_on_idx_one=omit_seeds_on_idx_one), nodes))

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

    def get_node_memory_mb(self):
        pass

    def get_node_cpu_count(self):
        pass

    def get_node_disk_free(self):
        pass

    def lsof_node(self, node: ClusterNode, filter: Optional[str] = None):
        pass

    def metrics(self,
                node,
                metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS):
        assert node in self._started, f"where node is {node.name}"

        metrics_endpoint = ("/metrics" if metrics_endpoint
                            == MetricsEndpoint.METRICS else "/public_metrics")
        url = f"http://{node.account.hostname}:9644{metrics_endpoint}"
        resp = requests.get(url)
        assert resp.status_code == 200
        return text_string_to_metric_families(resp.text)

    def metric_sum(self,
                   metric_name,
                   metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
                   ns=None,
                   topic=None):
        """
        Pings the 'metrics_endpoint' of each node and returns the summed values
        of the given metric, optionally filtering by namespace and topic.
        """
        count = 0
        for n in self.nodes:
            metrics = self.metrics(n, metrics_endpoint=metrics_endpoint)
            for family in metrics:
                for sample in family.samples:
                    if ns and sample.labels["namespace"] != ns:
                        continue
                    if topic and sample.labels["topic"] != topic:
                        continue
                    if sample.name == metric_name:
                        count += int(sample.value)
        return count

    def healthy(self):
        """
        A primitive health check on all the nodes which returns True when all
        nodes report that no under replicated partitions exist. This should
        later be replaced by a proper / official start-up probe type check on
        the health of a node after a restart.
        """
        counts = {self.idx(node): None for node in self.nodes}
        for node in self.nodes:
            try:
                metrics = self.metrics(node)
            except:
                return False
            idx = self.idx(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_cluster_partition_under_replicated_replicas":
                        if counts[idx] is None:
                            counts[idx] = 0
                        counts[idx] += int(sample.value)
        return all(map(lambda count: count == 0, counts.values()))

    def node_id(self, node, force_refresh=False, timeout_sec=30):
        pass

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
            leader = None if leader_id == -1 else self.get_node(leader_id)
            replicas = [self.get_node(r["id"]) for r in p["replicas"]]
            return Partition(topic_name, index, leader, replicas)

        for topic in md["topics"]:
            if topic["topic"] == topic_name or topic_name is None:
                result.extend(
                    make_partition(topic["topic"], p)
                    for p in topic["partitions"])

        return result

    def rolling_restart_nodes(self,
                              nodes,
                              override_cfg_params=None,
                              start_timeout=None,
                              stop_timeout=None,
                              use_maintenance_mode=True,
                              omit_seeds_on_idx_one=True):
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        restarter = RollingRestarter(self)
        restarter.restart_nodes(nodes,
                                override_cfg_params=override_cfg_params,
                                start_timeout=start_timeout,
                                stop_timeout=stop_timeout,
                                use_maintenance_mode=use_maintenance_mode,
                                omit_seeds_on_idx_one=omit_seeds_on_idx_one)

    def set_cluster_config(self,
                           values: dict,
                           expect_restart: bool = False,
                           admin_client: Optional[Admin] = None,
                           timeout: int = 10):
        pass

    def set_resource_settings(self, rs):
        self._resource_settings = rs


class RedpandaService(RedpandaServiceBase):
    PERSISTENT_ROOT = "/var/lib/redpanda"
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    NODE_CONFIG_FILE = "/etc/redpanda/redpanda.yaml"
    CLUSTER_BOOTSTRAP_CONFIG_FILE = "/etc/redpanda/.bootstrap.yaml"
    TLS_SERVER_KEY_FILE = "/etc/redpanda/server.key"
    TLS_SERVER_CRT_FILE = "/etc/redpanda/server.crt"
    TLS_CA_CRT_FILE = "/etc/redpanda/ca.crt"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.log")
    BACKTRACE_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda_backtrace.log")
    COVERAGE_PROFRAW_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                            "redpanda.profraw")

    DEFAULT_NODE_READY_TIMEOUT_SEC = 20

    DEDICATED_NODE_KEY = "dedicated_nodes"

    RAISE_ON_ERRORS_KEY = "raise_on_error"

    TRIM_LOGS_KEY = "trim_logs"

    LOG_LEVEL_KEY = "redpanda_log_level"
    DEFAULT_LOG_LEVEL = "info"

    SUPERUSER_CREDENTIALS: SaslCredentials = SaslCredentials(
        "admin", "admin", "SCRAM-SHA-256")

    COV_KEY = "enable_cov"
    DEFAULT_COV_OPT = "OFF"

    # Where we put a compressed binary if saving it after failure
    EXECUTABLE_SAVE_PATH = "/tmp/redpanda.gz"

    # When configuring multiple listeners for testing, a secondary port to use
    # instead of the default.
    KAFKA_ALTERNATE_PORT = 9093
    KAFKA_KERBEROS_PORT = 9094
    ADMIN_ALTERNATE_PORT = 9647

    CLUSTER_CONFIG_DEFAULTS = {
        'join_retry_timeout_ms': 200,
        'default_topic_partitions': 4,
        'enable_metrics_reporter': False,
        'superusers': [SUPERUSER_CREDENTIALS[0]],
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

    def __init__(self,
                 context,
                 num_brokers,
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
                 disable_cloud_storage_diagnostics=False):
        super(RedpandaService,
              self).__init__(context, num_brokers, extra_rp_conf,
                             resource_settings, si_settings)
        self._security = security
        self._installer: RedpandaInstaller = RedpandaInstaller(self)
        self._pandaproxy_config = pandaproxy_config
        self._schema_registry_config = schema_registry_config

        if superuser is None:
            superuser = self.SUPERUSER_CREDENTIALS
            self._skip_create_superuser = False
        else:
            # When we are passed explicit superuser credentials, presume that the caller
            # is taking care of user creation themselves (e.g. when testing credential bootstrap)
            self._skip_create_superuser = True

        self._superuser = superuser

        if node_ready_timeout_s is None:
            node_ready_timeout_s = RedpandaService.DEFAULT_NODE_READY_TIMEOUT_SEC
        self.node_ready_timeout_s = node_ready_timeout_s

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
            self._log_config = LoggingConfig(self._log_level, {
                'exception': 'debug',
                'io': 'debug',
                'seastar_memory': 'debug'
            })

        self._admin = Admin(self,
                            auth=(self._superuser.username,
                                  self._superuser.password))
        self._started = []
        self._security_config = dict()

        self._raise_on_errors = self._context.globals.get(
            self.RAISE_ON_ERRORS_KEY, True)

        self._dedicated_nodes = self._context.globals.get(
            self.DEDICATED_NODE_KEY, False)

        self._trim_logs = self._context.globals.get(self.TRIM_LOGS_KEY, True)

        self.logger.info(
            f"ResourceSettings: dedicated_nodes={self._dedicated_nodes}")

        # Disable saving cloud storage diagnostics. This may be useful for
        # tests that generate millions of objecst, as collecting diagnostics
        # may take a significant amount of time.
        self._disable_cloud_storage_diagnostics = disable_cloud_storage_diagnostics

        self.cloud_storage_client: Optional[S3Client] = None

        # enable asan abort / core dumps by default
        self._environment = dict(
            ASAN_OPTIONS=
            "abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1")
        if environment is not None:
            self._environment.update(environment)

        self.config_file_lock = threading.Lock()

        self._saved_executable = False

        self._tls_cert = None
        self._init_tls()

        self._skip_if_no_redpanda_log = skip_if_no_redpanda_log

        # Each time we start a node and write out its node_config (redpanda.yaml),
        # stash a copy here so that we can quickly look up e.g. addresses later.
        self._node_configs = {}

        self._node_id_by_idx = {}

        self._seed_servers = [self.nodes[0]] if len(self.nodes) > 0 else []

    def set_seed_servers(self, node_list):
        assert len(node_list) > 0
        self._seed_servers = node_list

    def set_skip_if_no_redpanda_log(self, v: bool):
        self._skip_if_no_redpanda_log = v

    def set_environment(self, environment: dict[str, str]):
        self._environment.update(environment)

    @property
    def si_settings(self):
        return self._si_settings

    def set_extra_node_conf(self, node, conf):
        assert node in self.nodes, f"where node is {node.name}"
        self._extra_node_conf[node] = conf

    def set_security_settings(self, settings):
        self._security = settings
        self._init_tls()

    def set_pandaproxy_settings(self, settings: PandaproxyConfig):
        self._pandaproxy_config = settings

    def set_schema_registry_settings(self, settings: SchemaRegistryConfig):
        self._schema_registry_config = settings

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

    @property
    def dedicated_nodes(self):
        """
        If true, the nodes are dedicated linux servers, e.g. EC2 instances.

        If false, the nodes are containers that share CPUs and memory with
        one another.
        :return:
        """
        return self._dedicated_nodes

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
            line = node.account.ssh_output("cat /proc/meminfo | grep MemTotal")
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
                "cat /proc/cpuinfo | grep ^processor | wc -l")
            return int(core_count_str.strip())

    def get_node_disk_free(self):
        # Assume nodes are symmetric, so we can just ask one
        node = self.nodes[0]

        if node.account.exists(self.PERSISTENT_ROOT):
            df_path = self.PERSISTENT_ROOT
        else:
            # If dir doesn't exist yet, use the parent.
            df_path = os.path.dirname(self.PERSISTENT_ROOT)

        df_out = node.account.ssh_output(f"df --output=avail {df_path}")

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
                f"df --block-size 1 {self.PERSISTENT_ROOT}"):
            self.logger.debug(line.strip())
            if self.PERSISTENT_ROOT in line:
                return int(line.split()[2])
        assert False, "couldn't parse df output"

    def _for_nodes(self, nodes, cb: callable, *, parallel: bool) -> list:
        if not parallel:
            # Trivial case: just loop and call
            for n in nodes:
                cb(n)
            return list(map(cb, nodes))

        n_workers = len(nodes)
        if n_workers > 0:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=n_workers) as executor:
                # The list() wrapper is to cause futures to be evaluated here+now
                # (including throwing any exceptions) and not just spawned in background.
                return list(executor.map(cb, nodes))
        else:
            return []

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
        expected = set(self._started)

        wait_until(lambda: {n
                            for n in self._started
                            if self.registered(n)} == expected,
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
        azurite_dns = f"{self._si_settings.cloud_storage_azure_storage_account}.blob.localhost"

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
        self._for_nodes(self.nodes, setup_node_dns, parallel=True)

    def start(self,
              nodes=None,
              clean_nodes=True,
              start_si=True,
              parallel: bool = True,
              expect_fail: bool = False,
              auto_assign_node_id: bool = False,
              omit_seeds_on_idx_one: bool = True,
              node_config_overrides={}):
        """
        Start the service on all nodes.

        By default, nodes are started in serial: this makes logs easier to
        read and simplifies debugging.  For tests starting larger numbers of
        nodes where serialized startup becomes annoying, pass parallel=True.

        :param parallel: if true, run clean and start operations in parallel
                         for the nodes being started.
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

        self._for_nodes(to_start, clean_one, parallel=parallel)

        if first_start:
            self.write_tls_certs()
            self.write_bootstrap_cluster_config()

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

        self._for_nodes(to_start, start_one, parallel=parallel)

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

        if start_si and self._si_settings is not None:
            self.start_si()

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
                f"Writing Redpanda node tls ca cert file: {RedpandaService.TLS_CA_CRT_FILE}"
            )
            self.logger.debug(open(ca.crt, "r").read())
            node.account.mkdirs(
                os.path.dirname(RedpandaService.TLS_CA_CRT_FILE))
            node.account.copy_to(ca.crt, RedpandaService.TLS_CA_CRT_FILE)

            if self._pandaproxy_config is not None:
                self._pandaproxy_config.maybe_write_client_certs(
                    node, self.logger, PandaproxyConfig.PP_TLS_CLIENT_KEY_FILE,
                    PandaproxyConfig.PP_TLS_CLIENT_CRT_FILE)
                self._pandaproxy_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._pandaproxy_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._pandaproxy_config.truststore_file = RedpandaService.TLS_CA_CRT_FILE

            if self._schema_registry_config is not None:
                self._schema_registry_config.maybe_write_client_certs(
                    node, self.logger,
                    SchemaRegistryConfig.SR_TLS_CLIENT_KEY_FILE,
                    SchemaRegistryConfig.SR_TLS_CLIENT_CRT_FILE)
                self._schema_registry_config.server_key = RedpandaService.TLS_SERVER_KEY_FILE
                self._schema_registry_config.server_crt = RedpandaService.TLS_SERVER_CRT_FILE
                self._schema_registry_config.truststore_file = RedpandaService.TLS_CA_CRT_FILE

    def security_config(self):
        return self._security_config

    def start_redpanda(self, node):
        preamble, res_args = self._resource_settings.to_cli(
            dedicated_node=self._dedicated_nodes)

        # each node will create its own copy of the .profraw file
        # since each node creates a redpanda broker.
        if self.cov_enabled():
            self._environment.update(
                dict(LLVM_PROFILE_FILE=
                     f"\"{RedpandaService.COVERAGE_PROFRAW_CAPTURE}\""))

        # Pass environment variables via FOO=BAR shell expressions
        env_preamble = " ".join(
            [f"{k}={v}" for (k, v) in self._environment.items()])

        cmd = (
            f"{preamble} {env_preamble} nohup {self.find_binary('redpanda')}"
            f" --redpanda-cfg {RedpandaService.NODE_CONFIG_FILE}"
            f" {self._log_config.to_args()} "
            " --abort-on-seastar-bad-alloc "
            " --dump-memory-diagnostics-on-alloc-failure-kind=all "
            f" {res_args} "
            f" >> {RedpandaService.STDOUT_STDERR_CAPTURE} 2>&1 &")

        node.account.ssh(cmd)

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

        return all(self._for_nodes(self._started, check_node, parallel=True))

    def wait_until(self, fn, timeout_sec, backoff_sec, err_msg=None):
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
                assert self.all_up()
            return r

        wait_until(wrapped,
                   timeout_sec=timeout_sec,
                   backoff_sec=backoff_sec,
                   err_msg=err_msg)

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

    def sockets_clear(self, node):
        """
        Check that high-numbered redpanda ports (in practice, just the internal
        RPC port) are clear on the node, to avoid TIME_WAIT sockets from previous
        tests interfering with redpanda startup.

        In principle, redpanda should not have a problem with TIME_WAIT sockets
        on its port (redpanda binds with SO_REUSEADDR), but in practice we have
        seen "Address in use" errors:
        https://github.com/redpanda-data/redpanda/pull/3754
        """
        for line in node.account.ssh_capture("netstat -ant"):
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
        for line in node.account.ssh_capture(cmd):
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

    def __is_status_ready(self, node):
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
                   skip_readiness_check: bool = False):
        """
        Start a single instance of redpanda. This function will not return until
        redpanda appears to have started successfully. If redpanda does not
        start within a timeout period the service will fail to start. Thus this
        function also acts as an implicit test that redpanda starts quickly.
        """
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        if write_config:
            self.write_node_conf_file(
                node,
                override_cfg_params,
                auto_assign_node_id=auto_assign_node_id,
                omit_seeds_on_idx_one=omit_seeds_on_idx_one)

        if timeout is None:
            timeout = self.node_ready_timeout_s

        if self.dedicated_nodes:
            # When running on dedicated nodes, we should always be running on XFS.  If we
            # aren't, it's probably an accident that can easily cause spurious failures
            # and confusion, so be helpful and fail out early.
            fs = node.account.ssh_output(
                f"stat -f -c %T {self.PERSISTENT_ROOT}").strip()
            if fs != b'xfs':
                raise RuntimeError(
                    f"Non-XFS filesystem {fs} at {self.PERSISTENT_ROOT} on {node.name}"
                )

        def start_rp():
            self.start_redpanda(node)

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
                    lambda: self.__is_status_ready(node),
                    timeout_sec=timeout,
                    backoff_sec=self._startup_poll_interval(first_start),
                    err_msg=
                    f"Redpanda service {node.account.hostname} failed to start within {timeout} sec",
                    retry_on_exc=True)

        self.logger.debug(f"Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        if not expect_fail:
            self._started.append(node)

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

        def start_rp():
            rpk.redpanda_start(RedpandaService.STDOUT_STDERR_CAPTURE,
                               additional_args, env_vars)

            wait_until(
                lambda: self.__is_status_ready(node),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=
                f"Redpanda service {node.account.hostname} failed to start within 60 sec using rpk",
                retry_on_exc=True)

        self.logger.debug(f"Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        self._started.append(node)

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
        which processes are running and which ports are in use.
        """

        for line in node.account.ssh_capture("ps aux"):
            self.logger.debug(line.strip())
        for line in node.account.ssh_capture("netstat -panelot"):
            self.logger.debug(line.strip())

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
        if self._si_settings.cloud_storage_type == CloudStorageType.S3:
            self.cloud_storage_client = S3Client(
                region=self._si_settings.cloud_storage_region,
                access_key=self._si_settings.cloud_storage_access_key,
                secret_key=self._si_settings.cloud_storage_secret_key,
                endpoint=self._si_settings.endpoint_url,
                logger=self.logger,
            )

            self.logger.debug(
                f"Creating S3 bucket: {self._si_settings.cloud_storage_bucket}"
            )
        elif self._si_settings.cloud_storage_type == CloudStorageType.ABS:
            self.cloud_storage_client = ABSClient(
                logger=self.logger,
                storage_account=self._si_settings.
                cloud_storage_azure_storage_account,
                shared_key=self._si_settings.cloud_storage_azure_shared_key,
                endpoint=self._si_settings.endpoint_url)
            self.logger.debug(
                f"Creating ABS container: {self._si_settings.cloud_storage_bucket}"
            )

        if not self._si_settings.bypass_bucket_creation:
            self.cloud_storage_client.create_bucket(
                self._si_settings.cloud_storage_bucket)

    def delete_bucket_from_si(self):
        self.logger.debug(
            f"Deleting bucket/container: {self._si_settings.cloud_storage_bucket}"
        )
        assert self.cloud_storage_client is not None

        failed_deletions = self.cloud_storage_client.empty_bucket(
            self._si_settings.cloud_storage_bucket,
            # If on dedicate nodes, assume tests may be high scale and do parallel deletion
            parallel=self.dedicated_nodes)
        assert len(failed_deletions) == 0
        self.cloud_storage_client.delete_bucket(
            self._si_settings.cloud_storage_bucket)

    def get_objects_from_si(self):
        assert self.cloud_storage_client is not None
        return self.cloud_storage_client.list_objects(
            self._si_settings.cloud_storage_bucket)

    def set_cluster_config(self,
                           values: dict,
                           expect_restart: bool = False,
                           admin_client: Optional[Admin] = None,
                           timeout: int = 10):
        """
        Update cluster configuration and wait for all nodes to report that they
        have seen the new config.

        :param values: dict of property name to value. if value is None, key will be removed from cluster config
        :param expect_restart: set to true if you wish to permit a node restart for needs_restart=yes properties.
                               If you set such a property without this flag, an assertion error will be raised.
        """
        if admin_client is None:
            admin_client = self._admin

        patch_result = admin_client.patch_cluster_config(
            upsert={k: v
                    for k, v in values.items() if v is not None},
            remove=[k for k, v in values.items() if v is None])
        new_version = patch_result['config_version']

        def is_ready():
            status = admin_client.get_cluster_config_status(
                node=self.controller())
            ready = all([n['config_version'] >= new_version for n in status])

            return ready, status

        # The version check is >= to permit other config writes to happen in
        # the background, including the write to cluster_id that happens
        # early in the cluster's lifetime
        config_status = wait_until_result(
            is_ready,
            timeout_sec=timeout,
            backoff_sec=0.5,
            err_msg=f"Config status versions did not converge on {new_version}"
        )

        any_restarts = any(n['restart'] for n in config_status)
        if any_restarts and expect_restart:
            self.restart_nodes(self.nodes)
            # Having disrupted the cluster with a restart, wait for the controller
            # to be available again before returning to the caller, so that they do
            # not have to worry about subsequent configuration actions failing.
            self._admin.await_stable_leader(namespace="redpanda",
                                            topic="controller",
                                            partition=0)
        elif any_restarts:
            raise AssertionError(
                "Nodes report restart required but expect_restart is False")

    def await_feature_active(self, feature_name: str, *, timeout_sec: int):
        """
        For use during upgrade tests, when after upgrade yo uwould like to block
        until a particular feature is active (e.g. if it does migrations)
        """
        def is_active():
            for n in self.nodes:
                f = self._admin.get_features(node=n)
                by_name = dict((f['name'], f) for f in f['features'])
                try:
                    state = by_name[feature_name]['state']
                except KeyError:
                    state = None

                if state != 'active':
                    self.logger.info(
                        f"Feature {feature_name} not yet active on {n.name} (state {state})"
                    )
                    return False

            self.logger.info(f"Feature {feature_name} is now active")
            return True

        wait_until(is_active, timeout_sec=timeout_sec, backoff_sec=1)

    def monitor_log(self, node):
        assert node in self.nodes, f"where node is {node.name}"
        return node.account.monitor_log(RedpandaService.STDOUT_STDERR_CAPTURE)

    def raise_on_crash(self):
        """
        Check if any redpanda nodes are unexpectedly not running,
        or if any logs contain segfaults or assertions.

        Call this after a test fails, to generate a more useful
        error message, rather than having failures on "timeouts" which
        are actually redpanda crashes.
        """
        crashes = []
        for node in self.nodes:
            self.logger.info(
                f"Scanning node {node.account.hostname} log for errors...")

            crash_log = None
            for line in node.account.ssh_capture(
                    f"grep -e SEGV -e Segmentation\ fault -e [Aa]ssert {RedpandaService.STDOUT_STDERR_CAPTURE} || true"
            ):
                if 'SEGV' in line and ('x-amz-id' in line
                                       or 'x-amz-request' in line):
                    # We log long encoded AWS headers that occasionally have 'SEGV' in them by chance
                    continue

                if "No such file or directory" not in line:
                    crash_log = line
                    break

            if crash_log:
                crashes.append((node, line))

        if not crashes:
            # Even if there is no assertion or segfault, look for unexpectedly
            # not-running processes
            for node in self._started:
                if not self.redpanda_pid(node):
                    crashes.append(
                        (node, "Redpanda process unexpectedly stopped"))

        if crashes:
            raise NodeCrash(crashes)

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
            f"Gathering cloud storage diagnostics in bucket {self._si_settings.cloud_storage_bucket}"
        )

        manifests_to_dump = []
        for o in self.cloud_storage_client.list_objects(
                self._si_settings.cloud_storage_bucket):
            key = o.key
            if key_dump_limit > 0:
                self.logger.info(f"  {key}")
                key_dump_limit -= 1

            # Gather manifest.json and topic_manifest.json files
            if key.endswith('manifest.json') and manifest_dump_limit > 0:
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
                    self._si_settings.cloud_storage_bucket, m)
                filename = m.replace("/", "_")
                with archive.open(filename, "w") as outstr:
                    outstr.write(body)

    def raise_on_bad_logs(self, allow_list=None):
        """
        Raise a BadLogLines exception if any nodes' logs contain errors
        not permitted by `allow_list`

        :param logger: the test's logger, so that reports of bad lines are
                       prefixed with test name.
        :param allow_list: list of compiled regexes, or None for default
        :return: None
        """

        if allow_list is None:
            allow_list = DEFAULT_LOG_ALLOW_LIST
        else:
            combined_allow_list = DEFAULT_LOG_ALLOW_LIST
            # Accept either compiled or string regexes
            for a in allow_list:
                if not isinstance(a, re.Pattern):
                    a = re.compile(a)
                combined_allow_list.append(a)
            allow_list = combined_allow_list

        test_name = self._context.function_name

        bad_lines = collections.defaultdict(list)
        for node in self.nodes:

            if self._skip_if_no_redpanda_log and not node.account.exists(
                    RedpandaService.STDOUT_STDERR_CAPTURE):
                self.logger.info(
                    f"{RedpandaService.STDOUT_STDERR_CAPTURE} not found on {node.account.hostname}. Skipping log scan."
                )
                continue

            self.logger.info(
                f"Scanning node {node.account.hostname} log for errors...")

            # List of regexes that will fail the test on if they appear in the log
            match_terms = [
                "Segmentation fault", "[Aa]ssert", "Exceptional future ignored"
            ]
            if self._raise_on_errors:
                match_terms.append("^ERROR")
            match_expr = " ".join(f"-e \"{t}\"" for t in match_terms)

            for line in node.account.ssh_capture(
                    f"grep {match_expr} {RedpandaService.STDOUT_STDERR_CAPTURE} || true"
            ):
                line = line.strip()

                allowed = False
                for a in allow_list:
                    if a.search(line) is not None:
                        self.logger.warn(
                            f"Ignoring allow-listed log line '{line}'")
                        allowed = True
                        break

                if 'LeakSanitizer' in line:
                    # Special case for LeakSanitizer errors, where tiny leaks
                    # are permitted, as they can occur during Seastar shutdown.
                    # See https://github.com/redpanda-data/redpanda/issues/3626
                    for summary_line in node.account.ssh_capture(
                            f"grep -e \"SUMMARY: AddressSanitizer:\" {RedpandaService.STDOUT_STDERR_CAPTURE} || true"
                    ):
                        m = re.match(
                            "SUMMARY: AddressSanitizer: (\d+) byte\(s\) leaked in (\d+) allocation\(s\).",
                            summary_line.strip())
                        if m and int(m.group(1)) < 1024:
                            self.logger.warn(
                                f"Ignoring memory leak, small quantity: {summary_line}"
                            )
                            allowed = True
                            break

                if not allowed:
                    bad_lines[node].append(line)
                    self.logger.warn(
                        f"[{test_name}] Unexpected log line on {node.account.hostname}: {line}"
                    )

        if bad_lines:
            raise BadLogLines(bad_lines)

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
        Returns the version as a string.
        """
        version_cmd = f"{self.find_binary('redpanda')} --version"
        VERSION_LINE_RE = re.compile(".*(v\\d+\\.\\d+\\.\\d+).*")
        # NOTE: not all versions of Redpanda support the --version field, even
        # though they print out the version.
        version_lines = [
            l for l in node.account.ssh_capture(version_cmd, allow_fail=True) \
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

        self._for_nodes(self.nodes,
                        lambda n: self.stop_node(n, **kwargs),
                        parallel=True)

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
        if node not in self._started:
            self._started.append(node)

    def clean(self, **kwargs):
        super().clean(**kwargs)
        if self.cloud_storage_client:
            self.delete_bucket_from_si()

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
            if node.account.sftp_client.listdir(
                    RedpandaService.PERSISTENT_ROOT):
                if not preserve_logs:
                    node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/*")
                else:
                    node.account.remove(
                        f"{RedpandaService.PERSISTENT_ROOT}/data/*")
        if node.account.exists(RedpandaService.NODE_CONFIG_FILE):
            node.account.remove(f"{RedpandaService.NODE_CONFIG_FILE}")
        if node.account.exists(RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE):
            node.account.remove(
                f"{RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE}")
        if not preserve_logs and node.account.exists(
                self.EXECUTABLE_SAVE_PATH):
            node.account.remove(self.EXECUTABLE_SAVE_PATH)

        if not preserve_current_install or not self._installer._started:
            # Reset the binaries to use the original binaries.
            # NOTE: if the installer hasn't been started, there is no
            # installation to preserve!
            self._installer.reset_current_install([node])

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

        self._for_nodes(self.nodes, prune, parallel=True)

    def remove_local_data(self, node):
        node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/data/*")

    def redpanda_pid(self, node):
        try:
            cmd = "ps ax | grep -i 'redpanda' | grep -v grep | grep -v 'version'| grep -v \"]\" | awk '{print $1}'"
            for p in node.account.ssh_capture(cmd,
                                              allow_fail=True,
                                              callback=int):
                return p

        except (RemoteCommandError, ValueError):
            return None

    def started_nodes(self):
        return self._started

    def render(self, path, **kwargs):
        with self.config_file_lock:
            return super(RedpandaService, self).render(path, **kwargs)

    @staticmethod
    def get_node_fqdn(node):
        ip = socket.gethostbyname(node.account.hostname)
        hostname = node.account.ssh_output(cmd=f"dig -x {ip} +short").decode(
            'utf-8').split('\n')[0].removesuffix(".")
        fqdn = node.account.ssh_output(
            cmd=f"host {hostname}").decode('utf-8').split(' ')[0]
        return fqdn

    def write_node_conf_file(self,
                             node,
                             override_cfg_params=None,
                             auto_assign_node_id=False,
                             omit_seeds_on_idx_one=True):
        """
        Write the node config file for a redpanda node: this is the YAML representation
        of Redpanda's `node_config` class.  Distinct from Redpanda's _cluster_ configuration
        which is written separately.
        """
        node_info = {self.idx(n): n for n in self.nodes}

        include_seed_servers = True
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

        # This node property isn't available on versions of RP older than 23.2.
        if cur_ver and cur_ver >= (23, 2, 0):
            memory_allocation_warning_threshold_bytes = 256 * 1024  # 256 KiB
        else:
            memory_allocation_warning_threshold_bytes = None

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
                           superuser=self._superuser,
                           sasl_enabled=self.sasl_enabled(),
                           endpoint_authn_method=self.endpoint_authn_method(),
                           auto_auth=self._security.auto_auth,
                           memory_allocation_warning_threshold=
                           memory_allocation_warning_threshold_bytes)

        if override_cfg_params or self._extra_node_conf[node]:
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
            tls_config = [
                dict(
                    enabled=True,
                    require_client_auth=self.require_client_auth(),
                    name=n,
                    key_file=RedpandaService.TLS_SERVER_KEY_FILE,
                    cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
                    truststore_file=RedpandaService.TLS_CA_CRT_FILE,
                ) for n in ["dnslistener", "iplistener"]
            ]
            doc = yaml.full_load(conf)
            doc["redpanda"].update(dict(kafka_api_tls=tls_config))
            conf = yaml.dump(doc)

        self.logger.info("Writing Redpanda node config file: {}".format(
            RedpandaService.NODE_CONFIG_FILE))
        self.logger.debug(conf)
        node.account.create_file(RedpandaService.NODE_CONFIG_FILE, conf)

        self._node_configs[node] = yaml.full_load(conf)

    def write_bootstrap_cluster_config(self):
        conf = copy.deepcopy(self.CLUSTER_CONFIG_DEFAULTS)

        cur_ver = self._installer.installed_version
        if cur_ver != RedpandaInstaller.HEAD and cur_ver < (22, 2, 7):
            # this configuration property was introduced in 22.2, ensure
            # it doesn't appear in older configurations
            del conf['log_segment_size_jitter_percent']

        if self._extra_rp_conf:
            self.logger.debug(
                "Setting custom cluster configuration options: {}".format(
                    self._extra_rp_conf))
            conf.update(self._extra_rp_conf)

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
        for node in self.nodes:
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
                    return self.get_node(resp_leader_id)

        return None

    def node_storage(self, node, sizes: bool = False) -> NodeStorage:
        """
        Retrieve a summary of storage on a node.

        :param sizes: if true, stat each segment file and record its size in the
                      `size` attribute of Segment.  This is expensive, only use it
                      for small-ish numbers of segments.
        """
        def listdir(path, only_dirs=False):
            try:
                ents = node.account.sftp_client.listdir(path)
            except FileNotFoundError:
                # Perhaps the directory has been deleted since we saw it.
                # This is normal if doing a listing concurrently with topic deletion.
                return []

            if not only_dirs:
                return ents
            paths = map(lambda fn: (fn, os.path.join(path, fn)), ents)

            def safe_isdir(path):
                try:
                    return node.account.isdir(path)
                except FileNotFoundError:
                    # Things that no longer exist are also no longer directories
                    return False

            return [p[0] for p in paths if safe_isdir(p[1])]

        def get_sizes(partition_path, segment_names, partition):
            for s in segment_names:
                try:
                    stat = node.account.sftp_client.lstat(
                        os.path.join(partition_path, s))
                    partition.set_segment_size(s, stat.st_size)
                except FileNotFoundError:
                    # It is legal for a file to be deleted between a listdir
                    # and a stat: update the partition object to reflect this,
                    # rather than leaving a None size that could trip up sum()
                    partition.delete_segment(s)

        store = NodeStorage(node.name, RedpandaService.DATA_DIR)
        for ns in listdir(store.data_dir, True):
            if ns == '.coprocessor_offset_checkpoints':
                continue
            if ns == 'cloud_storage_cache':
                # Default cache dir is sub-path of data dir
                continue

            ns = store.add_namespace(ns, os.path.join(store.data_dir, ns))
            for topic in listdir(ns.path):
                topic = ns.add_topic(topic, os.path.join(ns.path, topic))
                for num in listdir(topic.path):
                    partition_path = os.path.join(topic.path, num)
                    partition = topic.add_partition(num, node, partition_path)
                    segment_names = listdir(partition.path)
                    partition.add_files(segment_names)
                    if sizes:
                        get_sizes(partition_path, segment_names, partition)
        return store

    def storage(self, all_nodes: bool = False, sizes: bool = False):
        """
        :param all_nodes: if true, report on all nodes, otherwise only report
                          on started nodes.

        :returns: instances of ClusterStorage
        """
        store = ClusterStorage()
        for node in (self.nodes if all_nodes else self._started):
            s = self.node_storage(node, sizes=sizes)
            store.add_node(s)
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
        cmd = f"find {RedpandaService.DATA_DIR} -type f -exec md5sum -z '{{}}' \; -exec stat -c ' %s' '{{}}' \;"
        lines = node.account.ssh_output(cmd)
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
            tokens[1].rstrip("\x00"): (tokens[0], int(tokens[2]))
            for tokens in map(lambda l: l.split(), lines)
        }

    def broker_address(self, node, listener: str = "dnslistener"):
        assert node in self.nodes, f"where node is {node.name}"
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
        assert node in self.nodes, f"where node is {node.name}"
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
            self.broker_address(n, listener) for n in self._started[:limit]
        ]
        brokers = [b for b in brokers if b is not None]
        random.shuffle(brokers)
        return brokers

    def schema_reg(self, limit=None) -> str:
        schema_reg = [
            f"http://{n.account.hostname}:8081" for n in self._started[:limit]
        ]
        return ",".join(schema_reg)

    def _extract_samples(self, metrics, sample_pattern: str,
                         node: ClusterNode) -> list[MetricSamples]:
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

    def metrics_sample(
        self,
        sample_pattern,
        nodes=None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> Optional[MetricSamples]:
        """
        Query metrics for a single sample using fuzzy name matching. This
        interface matches the sample pattern against sample names, and requires
        that exactly one (family, sample) match the query. All values for the
        sample across the requested set of nodes are returned in a flat array.

        None will be returned if less than one (family, sample) matches.
        An exception will be raised if more than one (family, sample) matches.

        Example:

           The query:

              redpanda.metrics_sample("under_replicated")

           will return an array containing MetricSample instances for each node and
           core/shard in the cluster. Each entry will correspond to a value from:

              family = vectorized_cluster_partition_under_replicated_replicas
              sample = vectorized_cluster_partition_under_replicated_replicas
        """
        nodes = nodes or self.nodes
        sample_values = []
        for node in nodes:
            metrics = self.metrics(node, metrics_endpoint)
            sample_values += self._extract_samples(metrics, sample_pattern,
                                                   node)

        if not sample_values:
            return None
        else:
            return MetricSamples(sample_values)

    def metrics_samples(
        self,
        sample_patterns: list[str],
        nodes=None,
        metrics_endpoint: MetricsEndpoint = MetricsEndpoint.METRICS,
    ) -> dict[str, MetricSamples]:
        """
        Query metrics for multiple sample names using fuzzy matching.
        The same as metrics_sample, but works with multiple patterns.
        """
        nodes = nodes or self.nodes
        sample_values_per_pattern = {
            pattern: []
            for pattern in sample_patterns
        }

        for node in nodes:
            metrics = self.metrics(node, metrics_endpoint)
            for pattern in sample_patterns:
                sample_values_per_pattern[pattern] += self._extract_samples(
                    metrics, pattern, node)

        return {
            pattern: MetricSamples(values)
            for pattern, values in sample_values_per_pattern.items() if values
        }

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

    def cov_enabled(self):
        cov_option = self._context.globals.get(self.COV_KEY,
                                               self.DEFAULT_COV_OPT)
        if cov_option == "ON":
            return True
        elif cov_option == "OFF":
            return False

        self.logger.warn(f"{self.COV_KEY} should be one of 'ON', or 'OFF'")
        return False

    def search_log_node(self, node: ClusterNode, pattern: str):
        for line in node.account.ssh_capture(
                f"grep \"{pattern}\" {RedpandaService.STDOUT_STDERR_CAPTURE} || true"
        ):
            # We got a match
            self.logger.debug(f"Found {pattern} on node {node.name}: {line}")
            return True

        return False

    def search_log_any(self, pattern: str, nodes: list[ClusterNode] = None):
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

    def search_log_all(self, pattern: str, nodes: list[ClusterNode] = None):
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
                                     prev_start_offset=0):
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

        return wait_until_result(check, timeout_sec=30, backoff_sec=1)

    def stop_and_scrub_object_storage(self):
        # Before stopping, ensure that all tiered storage partitions
        # have uploaded at least a manifest: we do not require that they
        # have uploaded until the head of their log, just that they have
        # some metadata to validate, so that we will not experience
        # e.g. missing topic manifests.
        #
        # This should not need to wait long: even without waiting for
        # manifest upload interval, partitions should upload their initial
        # manifest as soon as they can, and that's all we require.

        def all_partitions_uploaded_manifest():
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
                has_uploaded_manifest = status[
                    "metadata_update_pending"] is False or status.get(
                        'ms_since_last_manifest_upload', None) is not None
                if remote_write and not has_uploaded_manifest:
                    self.logger.info(f"Partition {p} hasn't yet uploaded")
                    return False

            return True

        # If any nodes are up, then we expect to be able to talk to the cluster and
        # check tiered storage status to wait for uploads to complete.
        if self._started:
            # Aggressive retry because almost always this should already be done
            wait_until(all_partitions_uploaded_manifest,
                       timeout_sec=30,
                       backoff_sec=1)

        # We stop because the scrubbing routine would otherwise interpret
        # ongoing uploads as inconsistency.  In future, we may replace this
        # stop with a flush, when Redpanda gets an admin API for explicitly
        # flushing data to remote storage.
        self.stop()

        vars = {}
        backend = ""
        if self.si_settings.cloud_storage_type == CloudStorageType.S3:
            backend = "aws"
            vars["AWS_REGION"] = self.si_settings.cloud_storage_region
            if self.si_settings.endpoint_url:
                vars["AWS_ENDPOINT"] = self.si_settings.endpoint_url
                if self.si_settings.endpoint_url.startswith("http://"):
                    vars["AWS_ALLOW_HTTP"] = "true"

            if self.si_settings.cloud_storage_access_key is not None:
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
                vars[
                    "AZURE_STORAGE_CONNECTION_STRING"] = self.cloud_storage_client.conn_str
                vars[
                    "AZURE_STORAGE_ACCOUNT_KEY"] = self.si_settings.cloud_storage_azure_shared_key
                vars[
                    "AZURE_STORAGE_ACCOUNT_NAME"] = self.si_settings.cloud_storage_azure_storage_account

        # Pick an arbitrary node to run the scrub from
        node = self.nodes[0]

        bucket = self.si_settings.cloud_storage_bucket
        environment = ' '.join(f'{k}=\"{v}\"' for k, v in vars.items())
        output = node.account.ssh_output(
            f"{environment} segments --backend {backend} scan --source {bucket}",
            combine_stderr=False,
            allow_fail=True)

        try:
            report = json.loads(output)
        except:
            self.logger.error(f"Error running bucket scrub: {output}")
            raise

        # Example of a report:
        # {"malformed_manifests":[],
        #  "malformed_topic_manifests":[]
        #  "missing_segments":[
        #    "db0df8df/kafka/test/58_57/0-6-895-1-v1.log.2",
        #    "5c34266a/kafka/test/52_57/0-6-895-1-v1.log.2"
        #  ],
        #  "ntpr_no_manifest":[],
        #  "ntr_no_topic_manifest":[],
        #  "segments_outside_manifest":[],
        #  "unknown_keys":[]}

        # It is legal for tiered storage to leak objects under
        # certain circumstances: this will remain the case until
        # we implement background scrub + modify test code to
        # insist on Redpanda completing its own scrub before
        # we externally validate
        # (https://github.com/redpanda-data/redpanda/issues/9072)
        permitted_anomalies = {"segments_outside_manifest"}

        # Whether any anomalies were found
        any_anomalies = any(len(v) for v in report.values())

        # List of fatal anomalies found
        fatal_anomalies = set(k for k, v in report.items()
                              if len(v) and k not in permitted_anomalies)

        if not any_anomalies:
            self.logger.info(f"No anomalies in object storage scrub")
        elif not fatal_anomalies:
            self.logger.warn(
                f"Non-fatal anomalies in remote storage: {json.dumps(report, indent=2)}"
            )
        else:
            # Tests may declare that they expect some anomalies, e.g. if they
            # intentionally damage the data.
            if self._si_settings.is_damage_expected(fatal_anomalies):
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
