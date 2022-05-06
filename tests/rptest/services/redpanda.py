# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import copy

import time
import os
import socket
import signal
import tempfile
import shutil
import requests
import random
import threading
import collections
import re
import uuid
from typing import Mapping, Optional, Union, Any

import yaml
from ducktape.services.service import Service
from rptest.archival.s3_client import S3Client
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode
from prometheus_client.parser import text_string_to_metric_families

from rptest.clients.kafka_cat import KafkaCat
from rptest.services.storage import ClusterStorage, NodeStorage
from rptest.services.admin import Admin
from rptest.services.utils import BadLogLines, NodeCrash
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.services import tls

Partition = collections.namedtuple('Partition',
                                   ['index', 'leader', 'replicas'])

MetricSample = collections.namedtuple(
    'MetricSample', ['family', 'sample', 'node', 'value', 'labels'])

SaslCredentials = collections.namedtuple("SaslCredentials",
                                         ["username", "password", "algorithm"])

DEFAULT_LOG_ALLOW_LIST = [
    # Tests currently don't run on XFS, although in future they should.
    # https://github.com/redpanda-data/redpanda/issues/2376
    re.compile("not on XFS. This is a non-supported setup."),

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

    # e.g. raft - [group_id:59, {kafka/test-topic-319-1639161306093460/0}] consensus.cc:2301 - unable to replicate updated configuration: raft::errc::replicated_entry_truncated
    re.compile("raft - .*replicated_entry_truncated"),

    # e.g. cluster - controller_backend.cc:466 - exception while executing partition operation: {type: update_finished, ntp: {kafka/test-topic-1944-1639161306808363/1}, offset: 413, new_assignment: { id: 1, group_id: 65, replicas: {{node_id: 3, shard: 2}, {node_id: 4, shard: 2}, {node_id: 1, shard: 0}} }, previous_assignment: {nullopt}} - std::__1::__fs::filesystem::filesystem_error (error system:39, filesystem error: remove failed: Directory not empty [/var/lib/redpanda/data/kafka/test-topic-1944-1639161306808363])
    re.compile("cluster - .*Directory not empty"),
    re.compile("r/heartbeat - .*cannot find consensus group"),

    # raft - [follower: {id: {1}, revision: {9}}] [group_id:1, {kafka/topic-xyeyqcbyxi/0}] - recovery_stm.cc:422 - recovery append entries error: rpc::errc::exponential_backoff
    re.compile("raft - .*recovery append entries error"),

    # rpc - Service handler threw an exception: std::exception (std::exception)
    re.compile("rpc - Service handler threw an exception: std"),

    # rpc - Service handler threw an exception: seastar::broken_promise (broken promise)"
    re.compile("rpc - Service handler threw an exception: seastar"),
    re.compile(
        "cluster - .*exception while executing partition operation:.*std::exception \(std::exception\)"
    ),
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


def one_or_many(value):
    """
    Helper for reading `one_or_many_property` configs when
    we only care about getting one value out
    """
    if isinstance(value, list):
        return value[0]
    else:
        return value


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
                 nfiles: Optional[int] = None):
        self._num_cpus = num_cpus
        self._memory_mb = memory_mb

        if bypass_fsync is None:
            self._bypass_fsync = False
        else:
            self._bypass_fsync = bypass_fsync

        self._nfiles = nfiles

    @property
    def memory_mb(self):
        return self._memory_mb

    def to_cli(self, *, dedicated_node):
        """

        Generate Redpanda CLI flags based on the settings passed in at construction
        time.

        :return: 2 tuple of strings, first goes before the binary, second goes after it
        """
        preamble = f"ulimit -Sn {self._nfiles}; " if self._nfiles else ""

        if self._num_cpus is None and not dedicated_node:
            num_cpus = self.DEFAULT_NUM_CPUS
        else:
            num_cpus = self._num_cpus

        if self._memory_mb is None and not dedicated_node:
            memory_mb = self.DEFAULT_MEMORY_MB
        else:
            memory_mb = self._memory_mb

        if self._bypass_fsync is None and not dedicated_node:
            bypass_fsync = False
        else:
            bypass_fsync = self._bypass_fsync

        args = []
        if not dedicated_node:
            args.extend([
                "--kernel-page-cache=true", "--overprovisioned ",
                "--reserve-memory=0M"
            ])

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
    The defaults are for use with the default minio docker container, these
    settings are altered in RedpandaTest if running on AWS.
    """
    GLOBAL_S3_ACCESS_KEY = "s3_access_key"
    GLOBAL_S3_SECRET_KEY = "s3_secret_key"
    GLOBAL_S3_REGION_KEY = "s3_region"

    def __init__(
            self,
            *,
            log_segment_size: int = 16 * 1000000,
            cloud_storage_access_key: str = 'panda-user',
            cloud_storage_secret_key: str = 'panda-secret',
            cloud_storage_region: str = 'panda-region',
            cloud_storage_bucket: Optional[str] = None,
            cloud_storage_api_endpoint: str = 'minio-s3',
            cloud_storage_api_endpoint_port: int = 9000,
            cloud_storage_cache_size: int = 160 * 1000000,
            cloud_storage_enable_remote_read: bool = True,
            cloud_storage_enable_remote_write: bool = True,
            cloud_storage_reconciliation_interval_ms: Optional[int] = None,
            cloud_storage_max_connections: Optional[int] = None,
            cloud_storage_disable_tls: bool = True,
            cloud_storage_segment_max_upload_interval_sec: Optional[int] = None
    ):
        self.log_segment_size = log_segment_size
        self.cloud_storage_access_key = cloud_storage_access_key
        self.cloud_storage_secret_key = cloud_storage_secret_key
        self.cloud_storage_region = cloud_storage_region
        self.cloud_storage_bucket = f'panda-bucket-{uuid.uuid1()}' if cloud_storage_bucket is None else cloud_storage_bucket
        self.cloud_storage_api_endpoint = cloud_storage_api_endpoint
        self.cloud_storage_api_endpoint_port = cloud_storage_api_endpoint_port
        self.cloud_storage_cache_size = cloud_storage_cache_size
        self.cloud_storage_enable_remote_read = cloud_storage_enable_remote_read
        self.cloud_storage_enable_remote_write = cloud_storage_enable_remote_write
        self.cloud_storage_reconciliation_interval_ms = cloud_storage_reconciliation_interval_ms
        self.cloud_storage_max_connections = cloud_storage_max_connections
        self.cloud_storage_disable_tls = cloud_storage_disable_tls
        self.cloud_storage_segment_max_upload_interval_sec = cloud_storage_segment_max_upload_interval_sec
        self.endpoint_url = f'http://{self.cloud_storage_api_endpoint}:{self.cloud_storage_api_endpoint_port}'

    def load_context(self, logger, test_context):
        """
        Update based on the test context, to e.g. consume AWS access keys in
        the globals dictionary.
        """
        cloud_storage_access_key = test_context.globals.get(
            self.GLOBAL_S3_ACCESS_KEY, None)
        cloud_storage_secret_key = test_context.globals.get(
            self.GLOBAL_S3_SECRET_KEY, None)
        cloud_storage_region = test_context.globals.get(
            self.GLOBAL_S3_REGION_KEY, None)

        # Enable S3 if AWS creds were given at globals
        if cloud_storage_access_key and cloud_storage_secret_key:
            logger.info("Running on AWS S3, setting credentials")
            self.cloud_storage_access_key = cloud_storage_access_key
            self.cloud_storage_secret_key = cloud_storage_secret_key
            self.endpoint_url = None  # None so boto auto-gens the endpoint url
            self.cloud_storage_disable_tls = False  # SI will fail to create archivers if tls is disabled
            self.cloud_storage_region = cloud_storage_region
            self.cloud_storage_api_endpoint_port = 443
        else:
            logger.debug(
                'No AWS credentials supplied, assuming minio defaults')

    # Call this to update the extra_rp_conf
    def update_rp_conf(self, conf) -> dict[str, Any]:
        conf["log_segment_size"] = self.log_segment_size
        conf["cloud_storage_access_key"] = self.cloud_storage_access_key
        conf["cloud_storage_secret_key"] = self.cloud_storage_secret_key
        conf["cloud_storage_region"] = self.cloud_storage_region
        conf["cloud_storage_bucket"] = self.cloud_storage_bucket
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

        if self.cloud_storage_reconciliation_interval_ms:
            conf[
                'cloud_storage_reconciliation_interval_ms'] = self.cloud_storage_reconciliation_interval_ms
        if self.cloud_storage_max_connections:
            conf[
                'cloud_storage_max_connections'] = self.cloud_storage_max_connections
        if self.cloud_storage_segment_max_upload_interval_sec:
            conf[
                'cloud_storage_segment_max_upload_interval_sec'] = self.cloud_storage_segment_max_upload_interval_sec
        return conf


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
    PRINCIPAL_MAPPING_RULES = "RULE:^O=Redpanda,CN=(.*?)$/$1/L, DEFAULT"

    def __init__(self):
        self.enable_sasl = False
        self.tls_provider: Optional[TLSProvider] = None

        # extract principal from mtls distinguished name
        self.enable_mtls_identity = False


class RedpandaService(Service):
    PERSISTENT_ROOT = "/var/lib/redpanda"
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    NODE_CONFIG_FILE = "/etc/redpanda/redpanda.yaml"
    CLUSTER_BOOTSTRAP_CONFIG_FILE = "/etc/redpanda/.bootstrap.yaml"
    TLS_SERVER_KEY_FILE = "/etc/redpanda/server.key"
    TLS_SERVER_CRT_FILE = "/etc/redpanda/server.crt"
    TLS_CA_CRT_FILE = "/etc/redpanda/ca.crt"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.log")
    BACKTRACE_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda_backtrace.log")
    WASM_STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                              "wasm_engine.log")
    COVERAGE_PROFRAW_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                            "redpanda.profraw")

    DEFAULT_NODE_READY_TIMEOUT_SEC = 20
    WASM_READY_TIMEOUT_SEC = 10

    DEDICATED_NODE_KEY = "dedicated_nodes"

    RAISE_ON_ERRORS_KEY = "raise_on_error"

    LOG_LEVEL_KEY = "redpanda_log_level"
    DEFAULT_LOG_LEVEL = "info"

    SUPERUSER_CREDENTIALS: SaslCredentials = SaslCredentials(
        "admin", "admin", "SCRAM-SHA-256")

    COV_KEY = "enable_cov"
    DEFAULT_COV_OPT = False

    # Where we put a compressed binary if saving it after failure
    EXECUTABLE_SAVE_PATH = "/tmp/redpanda.tar.gz"

    # When configuring multiple listeners for testing, a secondary port to use
    # instead of the default.
    KAFKA_ALTERNATE_PORT = 9093
    ADMIN_ALTERNATE_PORT = 9647

    CLUSTER_CONFIG_DEFAULTS = {
        'join_retry_timeout_ms': 200,
        'default_topic_partitions': 4,
        'enable_metrics_reporter': False,
        'superusers': [SUPERUSER_CREDENTIALS[0]],
        'enable_auto_rebalance_on_node_add': True
    }

    logs = {
        "redpanda_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        },
        "wasm_engine_start_stdout_stderr": {
            "path": WASM_STDOUT_STDERR_CAPTURE,
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
                 enable_rp=True,
                 extra_rp_conf=None,
                 extra_node_conf=None,
                 enable_pp=False,
                 enable_sr=False,
                 resource_settings=None,
                 si_settings=None,
                 log_level: Optional[str] = None,
                 environment: Optional[dict[str, str]] = None,
                 security: SecurityConfig = SecurityConfig(),
                 node_ready_timeout_s=None):
        super(RedpandaService, self).__init__(context, num_nodes=num_brokers)
        self._context = context
        self._enable_rp = enable_rp
        self._extra_rp_conf = extra_rp_conf or dict()
        self._enable_pp = enable_pp
        self._enable_sr = enable_sr
        self._security = security

        if node_ready_timeout_s is None:
            node_ready_timeout_s = RedpandaService.DEFAULT_NODE_READY_TIMEOUT_SEC
        self.node_ready_timeout_s = node_ready_timeout_s

        self._extra_node_conf = {}
        for node in self.nodes:
            self._extra_node_conf[node] = extra_node_conf or dict()

        if log_level is None:
            self._log_level = self._context.globals.get(
                self.LOG_LEVEL_KEY, self.DEFAULT_LOG_LEVEL)
        else:
            self._log_level = log_level

        self._admin = Admin(self)
        self._admin = Admin(self,
                            auth=(self.SUPERUSER_CREDENTIALS.username,
                                  self.SUPERUSER_CREDENTIALS.password))
        self._started = []
        self._security_config = dict()

        self._raise_on_errors = self._context.globals.get(
            self.RAISE_ON_ERRORS_KEY, True)

        self._dedicated_nodes = self._context.globals.get(
            self.DEDICATED_NODE_KEY, False)

        if resource_settings is None:
            resource_settings = ResourceSettings()
        self._resource_settings = resource_settings
        self.logger.info(
            f"ResourceSettings: dedicated_nodes={self._dedicated_nodes}")

        if si_settings is not None:
            self._extra_rp_conf = si_settings.update_rp_conf(
                self._extra_rp_conf)
        self._si_settings = si_settings
        self._s3client = None

        if environment is None:
            environment = dict()
        self._environment = environment

        self.config_file_lock = threading.Lock()

        self._saved_executable = False

        self._tls_cert = None
        self._init_tls()

    def set_environment(self, environment: dict[str, str]):
        self._environment = environment

    def set_resource_settings(self, rs):
        self._resource_settings = rs

    def set_extra_rp_conf(self, conf):
        self._extra_rp_conf = conf

    def set_extra_node_conf(self, node, conf):
        assert node in self.nodes
        self._extra_node_conf[node] = conf

    def set_security_settings(self, settings):
        self._security = settings
        self._init_tls()

    def _init_tls(self):
        """
        Call this if tls setting may have changed.
        """
        if self._security.tls_provider:
            # build a cert for clients used internally to the service
            self._tls_cert = self._security.tls_provider.create_service_client_cert(
                self, "redpanda.service.admin")

    def sasl_enabled(self):
        return self._security.enable_sasl

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

    def start(self, nodes=None, clean_nodes=True):
        """Start the service on all nodes."""
        to_start = nodes if nodes is not None else self.nodes
        assert all((node in self.nodes for node in to_start))
        self.logger.info("%s: starting service" % self.who_am_i())

        first_start = self._start_time < 0
        if first_start:
            self._start_time = time.time()

        self.logger.debug(
            self.who_am_i() +
            ": killing processes and attempting to clean up before starting")
        for node in to_start:
            try:
                self.stop_node(node)
            except Exception:
                pass

            try:
                if clean_nodes:
                    self.clean_node(node)
                else:
                    self.logger.debug("%s: skip cleaning node" %
                                      self.who_am_i(node))
            except Exception:
                self.logger.exception(
                    f"Error cleaning node {node.account.hostname}:")
                raise

        if first_start:
            self.write_tls_certs()
            self.write_bootstrap_cluster_config()

        for node in to_start:
            self.logger.debug("%s: starting node" % self.who_am_i(node))
            self.start_node(node)

        if self._start_duration_seconds < 0:
            self._start_duration_seconds = time.time() - self._start_time

        self._admin.create_user(*self.SUPERUSER_CREDENTIALS)

        self.logger.info("Waiting for all brokers to join cluster")
        expected = set(self._started)
        wait_until(lambda: {n
                            for n in self._started
                            if self.registered(n)} == expected,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Cluster membership did not stabilize")

        self.logger.info("Verifying storage is in expected state")
        storage = self.storage()
        for node in storage.nodes:
            if not set(node.ns) == {"redpanda"} or not set(
                    node.ns["redpanda"].topics) == {"controller", "kvstore"}:
                self.logger.error(
                    f"Unexpected files: ns={node.ns} redpanda topics={node.ns['redpanda'].topics}"
                )
                raise RuntimeError("Unexpected files in data directory")

        if self.sasl_enabled():
            username, password, algorithm = self.SUPERUSER_CREDENTIALS
            self._security_config = dict(security_protocol='SASL_PLAINTEXT',
                                         sasl_mechanism=algorithm,
                                         sasl_plain_username=username,
                                         sasl_plain_password=password,
                                         request_timeout_ms=30000,
                                         api_version_auto_timeout_ms=3000)

        if self._si_settings is not None:
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

    def security_config(self):
        return self._security_config

    def start_redpanda(self, node):
        preamble, res_args = self._resource_settings.to_cli(
            dedicated_node=self._dedicated_nodes)

        # Pass environment variables via FOO=BAR shell expressions
        env_preamble = " ".join(
            [f"{k}={v}" for (k, v) in self._environment.items()])

        cmd = (
            f"{preamble} {env_preamble} nohup {self.find_binary('redpanda')}"
            f" --redpanda-cfg {RedpandaService.NODE_CONFIG_FILE}"
            f" --default-log-level {self._log_level}"
            f" --logger-log-level=exception=debug:archival=debug:io=debug:cloud_storage=debug "
            f" {res_args} "
            f" >> {RedpandaService.STDOUT_STDERR_CAPTURE} 2>&1 &")

        # set llvm_profile var for code coverae
        # each node will create its own copy of the .profraw file
        # since each node creates a redpanda broker.
        if self.cov_enabled():
            cmd = f"LLVM_PROFILE_FILE=\"{RedpandaService.COVERAGE_PROFRAW_CAPTURE}\" " + cmd

        node.account.ssh(cmd)

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

    def start_node(self,
                   node,
                   override_cfg_params=None,
                   timeout=None,
                   write_config=True):
        """
        Start a single instance of redpanda. This function will not return until
        redpanda appears to have started successfully. If redpanda does not
        start within a timeout period the service will fail to start. Thus this
        function also acts as an implicit test that redpanda starts quickly.
        """
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        if write_config:
            self.write_node_conf_file(node, override_cfg_params)

        if timeout is None:
            timeout = self.node_ready_timeout_s

        if self.coproc_enabled():
            self.start_wasm_engine(node)

        def is_status_ready():
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

        def start_rp():
            self.start_redpanda(node)

            wait_until(
                is_status_ready,
                timeout_sec=timeout,
                backoff_sec=1,
                err_msg=
                f"Redpanda service {node.account.hostname} failed to start within {timeout} sec",
                retry_on_exc=True)

        self.logger.debug(f"Node status prior to redpanda startup:")
        self.start_service(node, start_rp)
        self._started.append(node)

    def start_service(self, node, start):
        def log_node_stats():
            for line in node.account.ssh_capture("ps aux"):
                self.logger.debug(line.strip())
            for line in node.account.ssh_capture("netstat -ant"):
                self.logger.debug(line.strip())

        # Maybe the service collides with something that wasn't cleaned up
        # properly: let's peek at what's going on on the node before starting it.
        log_node_stats()

        try:
            start()
        except:
            # In case our failure to start is something like an "address in use", we
            # would like to know what else is going on on this node.
            self.logger.warn(
                f"Failed to start on {node.name}, gathering node ps and netstat..."
            )
            log_node_stats()
            raise

    def coproc_enabled(self):
        coproc = self._extra_rp_conf.get('enable_coproc')
        dev_mode = self._extra_node_conf.get('developer_mode', True)
        return coproc is True and dev_mode is True

    def start_wasm_engine(self, node):
        wcmd = (f"nohup {self.find_binary('node')}"
                f" {self.find_wasm_root()}/main.js"
                f" {RedpandaService.NODE_CONFIG_FILE} "
                f" >> {RedpandaService.WASM_STDOUT_STDERR_CAPTURE} 2>&1 &")

        self.logger.info(
            f"Starting wasm engine on {node.account} with command: {wcmd}")

        # wait until the wasm engine has finished booting up
        wasm_port = 43189
        conf_value = self._extra_node_conf[node].get(
            'coproc_supervisor_server')
        if conf_value is not None:
            wasm_port = conf_value['port']

        up_re = re.compile(f'.*:{wasm_port}')

        def wasm_service_up():
            def is_up(line):
                self.logger.debug(line.strip())
                return up_re.search(line) is not None

            nsr = node.account.ssh_capture("netstat -ant")
            return any([is_up(line) for line in nsr])

        if wasm_service_up() is True:
            self.logger.warn(f"Waiting for {wasm_port} to be available")
            wait_until(
                lambda: not wasm_service_up(),
                timeout_sec=RedpandaService.WASM_READY_TIMEOUT_SEC,
                err_msg=
                f"Wasm engine server shutdown within {RedpandaService.WASM_READY_TIMEOUT_SEC}s timeout",
                retry_on_exc=True)

        def start_wasm_service():
            node.account.ssh(wcmd)

            wait_until(
                wasm_service_up,
                timeout_sec=RedpandaService.WASM_READY_TIMEOUT_SEC,
                err_msg=
                f"Wasm engine server startup within {RedpandaService.WASM_READY_TIMEOUT_SEC}s timeout",
                retry_on_exc=True)

        self.logger.debug(f"Node status prior to wasm_engine startup:")
        self.start_service(node, start_wasm_service)

    def start_si(self):
        self._s3client = S3Client(
            region=self._si_settings.cloud_storage_region,
            access_key=self._si_settings.cloud_storage_access_key,
            secret_key=self._si_settings.cloud_storage_secret_key,
            endpoint=self._si_settings.endpoint_url,
            logger=self.logger,
        )

        self._s3client.create_bucket(self._si_settings.cloud_storage_bucket)

    def list_buckets(self) -> dict[str, Union[list, dict]]:
        assert self._s3client is not None
        return self._s3client.list_buckets()

    @property
    def s3_client(self):
        return self._s3client

    def delete_bucket_from_si(self):
        assert self._s3client is not None

        failed_deletions = self._s3client.empty_bucket(
            self._si_settings.cloud_storage_bucket)
        assert len(failed_deletions) == 0
        self._s3client.delete_bucket(self._si_settings.cloud_storage_bucket)

    def get_objects_from_si(self):
        assert self._s3client is not None
        return self._s3client.list_objects(
            self._si_settings.cloud_storage_bucket)

    def set_cluster_config(self, values: dict, expect_restart: bool = False):
        """
        Update cluster configuration and wait for all nodes to report that they
        have seen the new config.

        :param values: dict of property name to value
        :param expect_restart: set to true if you wish to permit a node restart for needs_restart=yes properties.
                               If you set such a property without this flag, an assertion error will be raised.
        """
        patch_result = self._admin.patch_cluster_config(upsert=values)
        new_version = patch_result['config_version']

        # The version check is >= to permit other config writes to happen in
        # the background, including the write to cluster_id that happens
        # early in the cluster's lifetime
        wait_until(
            lambda: all([
                n['config_version'] >= new_version
                for n in self._admin.get_cluster_config_status()
            ]),
            timeout_sec=10,
            backoff_sec=0.5,
            err_msg=f"Config status versions did not converge on {new_version}"
        )

        any_restarts = any(n['restart']
                           for n in self._admin.get_cluster_config_status())
        if any_restarts and expect_restart:
            self.restart_nodes(self.nodes)
        elif any_restarts:
            raise AssertionError(
                "Nodes report restart required but expect_restart is False")

    def monitor_log(self, node):
        assert node in self._started
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
                if "No such file or directory" not in line:
                    crash_log = line
                    break

            if crash_log:
                crashes.append((node, line))

        if not crashes:
            # Even if there is no assertion or segfault, look for unexpectedly
            # not-running processes
            for node in self._started:
                if not self.pids(node):
                    crashes.append(
                        (node, "Redpanda process unexpectedly stopped"))

        if crashes:
            self.save_executable()
            raise NodeCrash(crashes)

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
            self.logger.info(
                f"Scanning node {node.account.hostname} log for errors...")

            match_errors = "-e ERROR" if self._raise_on_errors else ""
            for line in node.account.ssh_capture(
                    f"grep {match_errors} -e Segmentation\ fault -e [Aa]ssert {RedpandaService.STDOUT_STDERR_CAPTURE} || true"
            ):
                line = line.strip()

                allowed = False
                for a in allow_list:
                    if a.search(line) is not None:
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

        for node, lines in bad_lines.items():
            # LeakSanitizer type errors may include raw backtraces that the devloper
            # needs the binary to decode + investigate
            if any([
                    re.findall("Sanitizer|[Aa]ssert|SEGV|Segmentation fault",
                               l) for l in lines
            ]):
                self.save_executable()
                break

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

    def find_wasm_root(self):
        rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/opt/wasm"

    def find_binary(self, name):
        rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/bin/{name}"

    def find_raw_binary(self, name):
        """
        Like `find_binary`, but find the underlying executable rather tha
        a shell wrapper.
        """
        rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/libexec/{name}"

    def stop_node(self, node, timeout=None):
        pids = self.pids(node)

        for pid in pids:
            node.account.signal(pid, signal.SIGTERM, allow_fail=False)

        if timeout is None:
            timeout = 30

        wait_until(
            lambda: len(self.pids(node)) == 0,
            timeout_sec=timeout,
            err_msg=
            f"Redpanda node {node.account.hostname} failed to stop in {timeout} seconds"
        )
        self.remove_from_started_nodes(node)

    def remove_from_started_nodes(self, node):
        if node in self._started:
            self._started.remove(node)

    def add_to_started_nodes(self, node):
        if node not in self._started:
            self._started.append(node)

    def clean(self, **kwargs):
        super().clean(**kwargs)
        if self._s3client:
            self.delete_bucket_from_si()

    def clean_node(self, node, preserve_logs=False):
        node.account.kill_process("redpanda", clean_shutdown=False)
        node.account.kill_process("bin/node", clean_shutdown=False)
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

    def remove_local_data(self, node):
        node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/data/*")

    def redpanda_pid(self, node):
        # we need to look for redpanda pid. pids() method returns pids of both
        # nodejs server and redpanda
        try:
            cmd = "ps ax | grep -i 'redpanda' | grep -v grep | awk '{print $1}'"
            for p in node.account.ssh_capture(cmd,
                                              allow_fail=True,
                                              callback=int):
                return p

        except (RemoteCommandError, ValueError):
            return None

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "ps ax | grep -i 'redpanda\|node' | grep -v grep | awk '{print $1}'"
            pid_arr = [
                pid for pid in node.account.ssh_capture(
                    cmd, allow_fail=True, callback=int)
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def started_nodes(self):
        return self._started

    def write_node_conf_file(self, node, override_cfg_params=None):
        """
        Write the node config file for a redpanda node: this is the YAML representation
        of Redpanda's `node_config` class.  Distinct from Redpanda's _cluster_ configuration
        which is written separately.
        """
        node_info = {self.idx(n): n for n in self.nodes}

        # Grab the IP to use it as an alternative listener address, to
        # exercise code paths that deal with multiple listeners
        node_ip = socket.gethostbyname(node.account.hostname)

        conf = self.render("redpanda.yaml",
                           node=node,
                           data_dir=RedpandaService.DATA_DIR,
                           nodes=node_info,
                           node_id=self.idx(node),
                           node_ip=node_ip,
                           kafka_alternate_port=self.KAFKA_ALTERNATE_PORT,
                           admin_alternate_port=self.ADMIN_ALTERNATE_PORT,
                           enable_rp=self._enable_rp,
                           enable_pp=self._enable_pp,
                           enable_sr=self._enable_sr,
                           superuser=self.SUPERUSER_CREDENTIALS,
                           sasl_enabled=self.sasl_enabled())

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
            tls_config = dict(
                enabled=True,
                require_client_auth=True,
                name="dnslistener",
                key_file=RedpandaService.TLS_SERVER_KEY_FILE,
                cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
                truststore_file=RedpandaService.TLS_CA_CRT_FILE,
            )
            if self._security.enable_mtls_identity:
                tls_config.update(
                    dict(principal_mapping_rules=SecurityConfig.
                         PRINCIPAL_MAPPING_RULES, ))
            doc = yaml.full_load(conf)
            doc["redpanda"].update(dict(kafka_api_tls=tls_config))
            conf = yaml.dump(doc)

        self.logger.info("Writing Redpanda node config file: {}".format(
            RedpandaService.NODE_CONFIG_FILE))
        self.logger.debug(conf)
        node.account.create_file(RedpandaService.NODE_CONFIG_FILE, conf)

    def write_bootstrap_cluster_config(self):
        conf = copy.deepcopy(self.CLUSTER_CONFIG_DEFAULTS)
        if self._extra_rp_conf:
            self.logger.debug(
                "Setting custom cluster configuration options: {}".format(
                    self._extra_rp_conf))
            conf.update(self._extra_rp_conf)

        if self._security.enable_sasl:
            self.logger.debug("Enabling SASL in cluster configuration")
            conf.update(dict(enable_sasl=True))

        conf_yaml = yaml.dump(conf)
        for node in self.nodes:
            self.logger.info(
                "Writing bootstrap cluster config file {}:{}".format(
                    node.name, RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE))
            node.account.mkdirs(
                os.path.dirname(RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE))
            node.account.create_file(
                RedpandaService.CLUSTER_BOOTSTRAP_CONFIG_FILE, conf_yaml)

    def restart_nodes(self,
                      nodes,
                      override_cfg_params=None,
                      start_timeout=None,
                      stop_timeout=None):
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        for node in nodes:
            self.stop_node(node, timeout=stop_timeout)
        for node in nodes:
            self.start_node(node, override_cfg_params, timeout=start_timeout)

    def registered(self, node):
        """
        Check if a newly added node is fully registered with the cluster, such
        that a kafka metadata request to any node in the cluster will include it.

        We first check the admin API to do a kafka-independent check, and then verify
        that kafka clients see the same thing.
        """
        idx = self.idx(node)
        self.logger.debug(
            f"registered: checking if broker {idx} ({node.name} is registered..."
        )

        # Query all nodes' admin APIs, so that we don't advance during setup until
        # the node is stored in raft0 AND has been replayed on all nodes.  Otherwise
        # a kafka metadata request to the last node to join could return incomplete
        # metadata and cause strange issues within a test.
        admin = Admin(self)
        for peer in self._started:
            try:
                admin_brokers = admin.get_brokers(node=peer)
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
                if b['node_id'] == idx:
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

        client = PythonLibrdkafka(self, tls_cert=self._tls_cert)
        brokers = client.brokers()
        broker = brokers.get(idx, None)
        if broker is None:
            # This should never happen, because we already checked via the admin API
            # that the node of interest had become visible to all peers.
            self.logger.error(
                f"registered: node {node.name} not found in kafka metadata!")
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

    def node_storage(self, node):
        """
        Retrieve a summary of storage on a node.
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

        store = NodeStorage(RedpandaService.DATA_DIR)
        for ns in listdir(store.data_dir, True):
            if ns == '.coprocessor_offset_checkpoints':
                continue
            ns = store.add_namespace(ns, os.path.join(store.data_dir, ns))
            for topic in listdir(ns.path):
                topic = ns.add_topic(topic, os.path.join(ns.path, topic))
                for num in listdir(topic.path):
                    partition = topic.add_partition(
                        num, node, os.path.join(topic.path, num))
                    partition.add_files(listdir(partition.path))
        return store

    def storage(self):
        store = ClusterStorage()
        for node in self._started:
            s = self.node_storage(node)
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

    def data_checksum(self, node):
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

    def broker_address(self, node):
        assert node in self._started
        cfg = self.read_configuration(node)
        return f"{node.account.hostname}:{one_or_many(cfg['redpanda']['kafka_api'])['port']}"

    def admin_endpoint(self, node):
        assert node in self._started
        return f"{node.account.hostname}:9644"

    def admin_endpoints_list(self):
        brokers = [self.admin_endpoint(n) for n in self._started]
        random.shuffle(brokers)
        return brokers

    def admin_endpoints(self):
        return ",".join(self.admin_endpoints_list())

    def brokers(self, limit=None) -> str:
        return ",".join(self.brokers_list(limit))

    def brokers_list(self, limit=None) -> list[str]:
        brokers = [self.broker_address(n) for n in self._started[:limit]]
        random.shuffle(brokers)
        return brokers

    def schema_reg(self, limit=None) -> str:
        schema_reg = [
            f"http://{n.account.hostname}:8081" for n in self._started[:limit]
        ]
        return ",".join(schema_reg)

    def metrics(self, node):
        assert node in self._started
        url = f"http://{node.account.hostname}:9644/metrics"
        resp = requests.get(url)
        assert resp.status_code == 200
        return text_string_to_metric_families(resp.text)

    def metrics_sample(self,
                       sample_pattern,
                       nodes=None) -> Optional[MetricSamples]:
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
        found_sample = None
        sample_values = []
        for node in nodes:
            metrics = self.metrics(node)
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
                        MetricSample(family.name, sample.name, node,
                                     sample.value, sample.labels))
        if not sample_values:
            return None
        return MetricSamples(sample_values)

    def read_configuration(self, node):
        assert node in self._started
        with self.config_file_lock:
            with node.account.open(RedpandaService.NODE_CONFIG_FILE) as f:
                return yaml.full_load(f.read())

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

    def healthy(self):
        """
        A primitive health check on all the nodes which returns True when all
        nodes report that no under replicated partitions exist. This should
        later be replaced by a proper / official start-up probe type check on
        the health of a node after a restart.
        """
        counts = {self.idx(node): None for node in self.nodes}
        for node in self.nodes:
            metrics = self.metrics(node)
            idx = self.idx(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_cluster_partition_under_replicated_replicas":
                        if counts[idx] is None:
                            counts[idx] = 0
                        counts[idx] += int(sample.value)
        return all(map(lambda count: count == 0, counts.values()))

    def partitions(self, topic):
        """
        Return partition metadata for the topic.
        """
        kc = KafkaCat(self)
        md = kc.metadata()
        topic = next(filter(lambda t: t["topic"] == topic, md["topics"]))

        def make_partition(p):
            index = p["partition"]
            leader_id = p["leader"]
            leader = None if leader_id == -1 else self.get_node(leader_id)
            replicas = [self.get_node(r["id"]) for r in p["replicas"]]
            return Partition(index, leader, replicas)

        return [make_partition(p) for p in topic["partitions"]]

    def cov_enabled(self):
        return self._context.globals.get(self.COV_KEY, self.DEFAULT_COV_OPT)

    def save_executable(self):
        """
        For the currently executing test, enable preserving the redpanda
        executable as if it were a log.  This is expensive in storage space:
        only do it if you catch an error that you think the binary will
        be needed to make sense of, like a LeakSanitizer error.

        This function does nothing in non-CI environments: in local development
        environments, the developer already has the binary.
        """

        if self._saved_executable:
            # Only ever do this once per test: the whole test runs with
            # the same binaries.
            return

        if os.environ.get('CI', None) == 'false':
            # We are on a developer workstation
            self.logger.info("Skipping saving executable, not in CI")
            return

        self.logger.info(
            f"Saving executable as {os.path.basename(self.EXECUTABLE_SAVE_PATH)}"
        )

        # Any node will do, they all run the same binary.  May cease to be true
        # for future mixed-version rolling upgrade testing.
        node = self.nodes[0]
        binary = self.find_raw_binary('redpanda')
        save_to = self.EXECUTABLE_SAVE_PATH
        try:
            node.account.ssh(f"cd /tmp ; gzip -c {binary} > {save_to}")
        except Exception as e:
            # Don't obstruct remaining test teardown when trying to save binary during failure
            # handling: eat the exception and log it.
            self.logger.exception(
                f"Error while compressing binary {binary} to {save_to}")
        else:
            self._saved_executable = True
            self._context.log_collect['executable', self] = True
