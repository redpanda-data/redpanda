# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

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
from typing import Optional

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
from rptest.clients.python_librdkafka import PythonLibrdkafka

Partition = collections.namedtuple('Partition',
                                   ['index', 'leader', 'replicas'])

MetricSample = collections.namedtuple(
    'MetricSample', ['family', 'sample', 'node', 'value', 'labels'])

SaslCredentials = collections.namedtuple("SaslCredentials",
                                         ["username", "password", "algorithm"])

DEFAULT_LOG_ALLOW_LIST = [
    # Tests currently don't run on XFS, although in future they should.
    # https://github.com/vectorizedio/redpanda/issues/2376
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
    # cluster - members_manager.cc:118 - Unable to update node configuration - seastar::sleep_aborted (Sleep is aborted)
    re.compile("members_manager.*sleep_aborted"),
    # cluster - members_backend.cc:91 - error waiting for members updates - seastar::abort_requested_exception (abort requested)
    re.compile("members_backend.*abort_requested_exception"),
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

    # Torn disk writes
    re.compile("storage - Could not parse header"),
    re.compile("storage - Cannot continue parsing"),

    # e.g. raft - [group_id:59, {kafka/test-topic-319-1639161306093460/0}] consensus.cc:2301 - unable to replicate updated configuration: raft::errc::replicated_entry_truncated
    re.compile("raft - .*replicated_entry_truncated"),

    # e.g. cluster - controller_backend.cc:466 - exception while executing partition operation: {type: update_finished, ntp: {kafka/test-topic-1944-1639161306808363/1}, offset: 413, new_assignment: { id: 1, group_id: 65, replicas: {{node_id: 3, shard: 2}, {node_id: 4, shard: 2}, {node_id: 1, shard: 0}} }, previous_assignment: {nullopt}} - std::__1::__fs::filesystem::filesystem_error (error system:39, filesystem error: remove failed: Directory not empty [/var/lib/redpanda/data/kafka/test-topic-1944-1639161306808363])
    re.compile("cluster - .*Directory not empty"),
    re.compile("r/heartbeat - .*cannot find consensus group"),

    # raft - [follower: {id: {1}, revision: {9}}] [group_id:1, {kafka/topic-xyeyqcbyxi/0}] - recovery_stm.cc:422 - recovery append entries error: rpc::errc::exponential_backoff
    re.compile("raft - .*recovery append entries error"),

    # rpc - Service handler threw an exception: std::exception (std::exception)
    re.compile("rpc - Service handler threw an exception: std"),
]


class BadLogLines(Exception):
    def __init__(self, node_to_lines):
        self.node_to_lines = node_to_lines

    def __str__(self):
        # Pick the first line from the first node as an example, and include it
        # in the string output so that for single line failures, it isn't necessary
        # for folks to search back in the log to find the culprit.
        example_lines = next(iter(self.node_to_lines.items()))[1]
        example = next(iter(example_lines))

        summary = ','.join([
            f'{i[0].account.hostname}({len(i[1])})'
            for i in self.node_to_lines.items()
        ])
        return f"<BadLogLines nodes={summary} example=\"{example}\">"

    def __repr__(self):
        return self.__str__()


class MetricSamples:
    def __init__(self, samples):
        self.samples = samples

    def label_filter(self, labels):
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
    """
    def __init__(self,
                 *,
                 num_cpus: Optional[int] = None,
                 memory_mb: Optional[int] = None,
                 bypass_fsync: Optional[bool] = None,
                 nfiles: Optional[int] = None):
        if num_cpus is None:
            num_cpus = 3

        if memory_mb is None:
            memory_mb = 6000

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

    @property
    def num_cpus(self):
        return self._num_cpus

    def to_cli(self):
        """
        :return: 2 tuple of strings, first goes before the binary, second goes after it
        """
        preamble = f"ulimit -Sn {self._nfiles}; " if self._nfiles else ""
        args = ("--kernel-page-cache=true "
                "--overprovisioned "
                "--reserve-memory 0M "
                f"--memory {self._memory_mb}M "
                f"--smp {self._num_cpus} "
                f"--unsafe-bypass-fsync={'1' if self._bypass_fsync else '0'}")
        return preamble, args


class SISettings:
    """
    Settings for shadow indexing stuff
    """
    def __init__(self,
                 *,
                 log_segment_size=16 * 1000000,
                 cloud_storage_access_key="panda-user",
                 cloud_storage_secret_key="panda-secret",
                 cloud_storage_region="panda-region",
                 cloud_storage_bucket=f"panda-bucket-{uuid.uuid1()}",
                 cloud_storage_api_endpoint="minio-s3",
                 cloud_storage_api_endpoint_port=9000,
                 cloud_storage_cache_size=160 * 1000000):
        self.log_segment_size = log_segment_size
        self.cloud_storage_access_key = cloud_storage_access_key
        self.cloud_storage_secret_key = cloud_storage_secret_key
        self.cloud_storage_region = cloud_storage_region
        self.cloud_storage_bucket = cloud_storage_bucket
        self.cloud_storage_api_endpoint = cloud_storage_api_endpoint
        self.cloud_storage_api_endpoint_port = cloud_storage_api_endpoint_port
        self.cloud_storage_cache_size = cloud_storage_cache_size

    def update_rp_conf(self, conf):
        conf["log_segment_size"] = self.log_segment_size
        conf["cloud_storage_access_key"] = self.cloud_storage_access_key
        conf["cloud_storage_secret_key"] = self.cloud_storage_secret_key
        conf["cloud_storage_region"] = self.cloud_storage_region
        conf["cloud_storage_bucket"] = self.cloud_storage_bucket
        conf["cloud_storage_api_endpoint"] = self.cloud_storage_api_endpoint
        conf[
            "cloud_storage_api_endpoint_port"] = self.cloud_storage_api_endpoint_port
        conf["cloud_storage_cache_size"] = self.cloud_storage_cache_size
        conf["cloud_storage_enabled"] = True
        conf['cloud_storage_disable_tls'] = True

        return conf


class RedpandaService(Service):
    PERSISTENT_ROOT = "/var/lib/redpanda"
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    CONFIG_FILE = "/etc/redpanda/redpanda.yaml"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.log")
    WASM_STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                              "wasm_engine.log")
    COVERAGE_PROFRAW_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                            "redpanda.profraw")

    CLUSTER_NAME = "my_cluster"
    READY_TIMEOUT_SEC = 10

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
        }
    }

    def __init__(self,
                 context,
                 num_brokers,
                 *,
                 enable_rp=True,
                 extra_rp_conf=None,
                 enable_pp=False,
                 enable_sr=False,
                 resource_settings=None,
                 si_settings=None,
                 log_level: Optional[str] = None):
        super(RedpandaService, self).__init__(context, num_nodes=num_brokers)
        self._context = context
        self._enable_rp = enable_rp
        self._extra_rp_conf = extra_rp_conf or dict()
        self._enable_pp = enable_pp
        self._enable_sr = enable_sr

        if log_level is None:
            self._log_level = self._context.globals.get(
                self.LOG_LEVEL_KEY, self.DEFAULT_LOG_LEVEL)
        else:
            self._log_level = log_level

        self._admin = Admin(self)
        self._started = []
        self._security_config = dict()

        if resource_settings is None:
            resource_settings = ResourceSettings()
        self._resource_settings = resource_settings

        if si_settings is not None:
            self._extra_rp_conf = si_settings.update_rp_conf(
                self._extra_rp_conf)
        self._si_settings = si_settings
        self._s3client = None

        self.config_file_lock = threading.Lock()

    def set_resource_settings(self, rs):
        self._resource_settings = rs

    def set_extra_rp_conf(self, conf):
        self._extra_rp_conf = conf

    def sasl_enabled(self):
        return self._extra_rp_conf and self._extra_rp_conf.get(
            "enable_sasl", False)

    def start(self, nodes=None, clean_nodes=True):
        """Start the service on all nodes."""
        to_start = nodes if nodes is not None else self.nodes
        assert all((node in self.nodes for node in to_start))
        self.logger.info("%s: starting service" % self.who_am_i())
        if self._start_time < 0:
            # Set self._start_time only the first time self.start is invoked
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

    def security_config(self):
        return self._security_config

    def start_redpanda(self, node):
        preamble, res_args = self._resource_settings.to_cli()

        cmd = (
            f"{preamble}nohup {self.find_binary('redpanda')}"
            f" --redpanda-cfg {RedpandaService.CONFIG_FILE}"
            f" --default-log-level {self._log_level}"
            f" --logger-log-level=exception=debug:archival=debug:io=debug:cloud_storage=debug "
            f" {res_args} "
            f" >> {RedpandaService.STDOUT_STDERR_CAPTURE} 2>&1 &")

        # set llvm_profile var for code coverage
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

    def lsof_node(self, node: ClusterNode):
        """
        Get the list of open files for a running node
        :return: yields strings
        """
        first = True
        for line in node.account.ssh_capture(
                f"lsof -nP -p {self.redpanda_pid(node)}"):
            if first:
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
        node.account.mkdirs(os.path.dirname(RedpandaService.CONFIG_FILE))

        if write_config:
            self.write_conf_file(node, override_cfg_params)

        if self.coproc_enabled():
            self.start_wasm_engine(node)

        # Maybe redpanda collides with something that wasn't cleaned up
        # properly: let's peek at what's going on on the node before starting it.
        self.logger.debug(f"Node status prior to redpanda startup:")
        for line in node.account.ssh_capture("ps aux"):
            self.logger.debug(line.strip())
        for line in node.account.ssh_capture("netstat -ant"):
            self.logger.debug(line.strip())

        self.start_redpanda(node)

        if timeout is None:
            timeout = self.READY_TIMEOUT_SEC

        try:
            wait_until(
                lambda: Admin.ready(node).get("status") == "ready",
                timeout_sec=timeout,
                err_msg=
                f"Redpanda service {node.account.hostname} failed to start",
                retry_on_exc=True)
        except:
            # In case our failure to start is something like an "address in use", we
            # would like to know what else is going on on this node.
            self.logger.warn(
                f"Failed to start on {node.name}, gathering node ps and netstat..."
            )
            for line in node.account.ssh_capture("ps aux"):
                self.logger.warn(line.strip())
            for line in node.account.ssh_capture("netstat -ant"):
                self.logger.warn(line.strip())
            raise
        self._started.append(node)

    def coproc_enabled(self):
        coproc = self._extra_rp_conf.get('enable_coproc')
        dev_mode = self._extra_rp_conf.get('developer_mode')
        return coproc is True and dev_mode is True

    def start_wasm_engine(self, node):
        wcmd = (f"nohup {self.find_binary('node')}"
                f" {self.find_wasm_root()}/main.js"
                f" {RedpandaService.CONFIG_FILE} "
                f" >> {RedpandaService.WASM_STDOUT_STDERR_CAPTURE} 2>&1 &")

        self.logger.info(
            f"Starting wasm engine on {node.account} with command: {wcmd}")

        # wait until the wasm engine has finished booting up
        wasm_port = 43189
        conf_value = self._extra_rp_conf.get('coproc_supervisor_server')
        if conf_value is not None:
            wasm_port = conf_value['port']

        with node.account.monitor_log(
                RedpandaService.WASM_STDOUT_STDERR_CAPTURE) as mon:
            node.account.ssh(wcmd)
            mon.wait_until(
                f"Starting redpanda wasm service on port: {wasm_port}",
                timeout_sec=RedpandaService.READY_TIMEOUT_SEC,
                backoff_sec=0.5,
                err_msg=
                f"Wasm engine didn't finish startup in {RedpandaService.READY_TIMEOUT_SEC} seconds",
            )

    def start_si(self):
        self._s3client = S3Client(
            region=self._si_settings.cloud_storage_region,
            access_key=self._si_settings.cloud_storage_access_key,
            secret_key=self._si_settings.cloud_storage_secret_key,
            endpoint=
            f"http://{self._si_settings.cloud_storage_api_endpoint}:{self._si_settings.cloud_storage_api_endpoint_port}",
            logger=self.logger,
        )

        self._s3client.create_bucket(self._si_settings.cloud_storage_bucket)

    def delete_bucket_from_si(self):
        if self._s3client:
            failed_deletions = self._s3client.empty_bucket(
                self._si_settings.cloud_storage_bucket)
            assert len(failed_deletions) == 0
            self._s3client.delete_bucket(
                self._si_settings.cloud_storage_bucket)

    def get_objects_from_si(self):
        if self._s3client:
            return self._s3client.list_objects(
                self._si_settings.cloud_storage_bucket)

    def monitor_log(self, node):
        assert node in self._started
        return node.account.monitor_log(RedpandaService.STDOUT_STDERR_CAPTURE)

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

            for line in node.account.ssh_capture(
                    f"grep -e ERROR -e Segmentation\ fault -e [Aa]ssert {RedpandaService.STDOUT_STDERR_CAPTURE}"
            ):
                line = line.strip()

                allowed = False
                for a in allow_list:
                    if a.search(line) is not None:
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
            if any(['Sanitizer' in l for l in lines]):
                self.save_executable()
                break

        if bad_lines:
            raise BadLogLines(bad_lines)

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
            err_msg=f"Redpanda node failed to stop in {timeout} seconds")
        if node in self._started:
            self._started.remove(node)

    def clean(self, **kwargs):
        super().clean(**kwargs)
        self.delete_bucket_from_si()

    def clean_node(self, node, preserve_logs=False):
        node.account.kill_process("redpanda", clean_shutdown=False)
        if node.account.exists(RedpandaService.PERSISTENT_ROOT):
            if node.account.sftp_client.listdir(
                    RedpandaService.PERSISTENT_ROOT):
                if not preserve_logs:
                    node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/*")
                else:
                    node.account.remove(
                        f"{RedpandaService.PERSISTENT_ROOT}/data/*")
        if node.account.exists(RedpandaService.CONFIG_FILE):
            node.account.remove(f"{RedpandaService.CONFIG_FILE}")
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

    def write_conf_file(self, node, override_cfg_params):
        node_info = {self.idx(n): n for n in self.nodes}

        # Grab the IP to use it as an alternative listener address, to
        # exercise code paths that deal with multiple listeners
        node_ip = socket.gethostbyname(node.account.hostname)

        conf = self.render("redpanda.yaml",
                           node=node,
                           data_dir=RedpandaService.DATA_DIR,
                           cluster=RedpandaService.CLUSTER_NAME,
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

        if self._extra_rp_conf:
            doc = yaml.full_load(conf)
            self.logger.debug(
                "Setting custom Redpanda configuration options: {}".format(
                    self._extra_rp_conf))
            doc["redpanda"].update(self._extra_rp_conf)
            conf = yaml.dump(doc)

        if override_cfg_params:
            doc = yaml.full_load(conf)
            self.logger.debug(
                "Setting custom Redpanda node configuration options: {}".
                format(override_cfg_params))
            doc["redpanda"].update(override_cfg_params)
            conf = yaml.dump(doc)

        self.logger.info("Writing Redpanda config file: {}".format(
            RedpandaService.CONFIG_FILE))
        self.logger.debug(conf)
        node.account.create_file(RedpandaService.CONFIG_FILE, conf)

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

        client = PythonLibrdkafka(self)
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

    def brokers(self, limit=None):
        return ",".join(self.brokers_list(limit))

    def brokers_list(self, limit=None):
        brokers = [self.broker_address(n) for n in self._started[:limit]]
        random.shuffle(brokers)
        return brokers

    def schema_reg(self, limit=None):
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

    def metrics_sample(self, sample_pattern, nodes=None):
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
            with node.account.open(RedpandaService.CONFIG_FILE) as f:
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
            self._context.log_collect['executable', self] = True
