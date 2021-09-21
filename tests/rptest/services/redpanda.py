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
import signal
import tempfile
import shutil
import requests
import random
import threading
import collections
import functools

import yaml
from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from prometheus_client.parser import text_string_to_metric_families

from rptest.clients.kafka_cat import KafkaCat
from rptest.services.storage import ClusterStorage, NodeStorage
from rptest.services.admin import Admin
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.types import TopicSpec
from kafka import KafkaAdminClient

Partition = collections.namedtuple('Partition',
                                   ['index', 'leader', 'replicas'])


class ResourceSettings:
    """
    Control CPU+memory footprint of Redpanda instances.  Pass one
    of these into your RedpandaTest constructor if you want to e.g.
    create low-memory situations.
    """
    def __init__(self,
                 name=None,
                 num_cpus=None,
                 memory_mb=None,
                 bypass_fsync=None):
        """

        :param num_cpus:
        :param memory_mb:
        :param bypass_fsync:
        """
        self._name = name

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

    def to_cli(self):
        return ("--kernel-page-cache=true "
                "--overprovisioned "
                "--reserve-memory 0M "
                f"--memory {self._memory_mb}M "
                f"--smp {self._num_cpus} "
                f"--unsafe-bypass-fsync={'1' if self._bypass_fsync else '0'}")

    @property
    def name(self):
        return self._name

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()

    def __str__(self):
        return f"<ResourceSettings cpus={self._num_cpus} mem={self._memory_mb} bypass-fsync={self._bypass_fsync}>"

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {
            'name': self._name,
            'num_cpus': self._num_cpus,
            'memory_mb': self._memory_mb,
            'bypass_fsync': self._bypass_fsync
        }


import sys
sys.stderr.write(f"resource config waat")
RESOURCE_TINY = ResourceSettings('tiny',
                                 num_cpus=1,
                                 memory_mb=200,
                                 bypass_fsync=True)
RESOURCE_STANDARD = ResourceSettings('standard')

# Resource configs that may be referred to by name when
# parameterizing tests.  This helps keep test output directory
# names concise.
NAMED_RESOURCES = {
    RESOURCE_TINY.name: RESOURCE_TINY,
    RESOURCE_STANDARD.name: RESOURCE_STANDARD
}

# Cluster resource params that will be used for a test if it does
# not specify any of its own.
DEFAULT_RESOURCE_PARAMS = [RESOURCE_TINY, RESOURCE_STANDARD]


def redpanda_cluster(num_nodes=None, resource_configs=None):
    """
    This decorator is an adaptation of the ducktape `cluster` decorator
    that includes parameterizing the redpanda cluster config
    """
    if resource_configs is None:
        resource_configs = DEFAULT_RESOURCE_PARAMS

    # Ducktape test parameters have to be JSON serializable, and will
    # be included in test run names.  For NAMED_RESOURCES configs (the
    # standard ones), pass through by name to keep nice concise testnames).
    # For custom resource settings, pass through as dict.

    resource_configs_serialized = []
    for r in resource_configs:
        import sys
        sys.stderr.write(f"resource config {r}")
        if r.name in NAMED_RESOURCES:
            resource_configs_serialized.append(r.name)
        else:
            resource_configs_serialized.append(r.to_dict())

    if num_nodes is None:
        num_nodes = 3

    def inner(func):
        @functools.wraps(func)
        def wrapped(self, resource_settings, *args, **kwargs):
            if isinstance(resource_settings, dict):
                self.redpanda.set_resource_settings(
                    ResourceSettings(**resource_settings))
            else:
                self.redpanda.set_resource_settings(
                    NAMED_RESOURCES[resource_settings])

            self.redpanda.start()
            return func(self, *args, **kwargs)

        f = wrapped
        for r in resource_configs_serialized:
            f = parametrize(resource_settings=r)(f)
        f = cluster(num_nodes=num_nodes)(f)

        return f

    return inner


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

    SUPERUSER_CREDENTIALS = ("admin", "admin", "SCRAM-SHA-256")

    COV_KEY = "enable_cov"
    DEFAULT_COV_OPT = False

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
        }
    }

    def __init__(self,
                 context,
                 num_brokers,
                 client_type,
                 enable_rp=True,
                 extra_rp_conf=None,
                 enable_pp=False,
                 enable_sr=False,
                 topics=None,
                 resource_settings=None):
        super(RedpandaService, self).__init__(context, num_nodes=num_brokers)
        self._context = context
        self._client_type = client_type
        self._enable_rp = enable_rp
        self._extra_rp_conf = extra_rp_conf or dict()
        self._enable_pp = enable_pp
        self._enable_sr = enable_sr
        self._log_level = self._context.globals.get(self.LOG_LEVEL_KEY,
                                                    self.DEFAULT_LOG_LEVEL)
        self._topics = topics or ()
        self._admin = Admin(self)
        self._started = []

        if resource_settings is None:
            resource_settings = ResourceSettings()
        self._resource_settings = resource_settings

        # client is intiialized after service starts
        self._client = None

        self.config_file_lock = threading.Lock()

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
                pass

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
            assert set(node.ns) == {"redpanda"}
            assert set(node.ns["redpanda"].topics) == {"controller", "kvstore"}

        security_settings = dict()
        if self.sasl_enabled():
            username, password, algorithm = self.SUPERUSER_CREDENTIALS
            security_settings = dict(security_protocol='SASL_PLAINTEXT',
                                     sasl_mechanism=algorithm,
                                     sasl_plain_username=username,
                                     sasl_plain_password=password,
                                     request_timeout_ms=30000,
                                     api_version_auto_timeout_ms=3000)
        self._client = KafkaAdminClient(bootstrap_servers=self.brokers_list(),
                                        **security_settings)

        self._create_initial_topics(security_settings)

    def _create_initial_topics(self, security_settings):
        user = security_settings.get("sasl_plain_username")
        passwd = security_settings.get("sasl_plain_password")

        client = self._client_type(self, user=user, passwd=passwd)
        for spec in self._topics:
            self.logger.debug(f"Creating initial topic {spec}")
            client.create_topic(spec)

    def start_node(self, node, override_cfg_params=None):
        """
        Start a single instance of redpanda. This function will not return until
        redpanda appears to have started successfully. If redpanda does not
        start within a timeout period the service will fail to start. Thus this
        function also acts as an implicit test that redpanda starts quickly.
        """
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.CONFIG_FILE))

        self.write_conf_file(node, override_cfg_params)

        if self.coproc_enabled():
            self.start_wasm_engine(node)

        cmd = (f"nohup {self.find_binary('redpanda')}"
               f" --redpanda-cfg {RedpandaService.CONFIG_FILE}"
               f" --default-log-level {self._log_level}"
               f" --logger-log-level=exception=debug:archival=debug:io=debug "
               f" {self._resource_settings.to_cli()} "
               f" >> {RedpandaService.STDOUT_STDERR_CAPTURE} 2>&1 &")

        # set llvm_profile var for code coverage
        # each node will create its own copy of the .profraw file
        # since each node creates a redpanda broker.
        if self.cov_enabled():
            cmd = f"LLVM_PROFILE_FILE=\"{RedpandaService.COVERAGE_PROFRAW_CAPTURE}\" " + cmd

        node.account.ssh(cmd)

        wait_until(
            lambda: Admin.ready(node).get("status") == "ready",
            timeout_sec=RedpandaService.READY_TIMEOUT_SEC,
            err_msg=f"Redpanda service {node.account.hostname} failed to start",
            retry_on_exc=True)
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

    def monitor_log(self, node):
        assert node in self._started
        return node.account.monitor_log(RedpandaService.STDOUT_STDERR_CAPTURE)

    def find_wasm_root(self):
        rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/opt/wasm"

    def find_binary(self, name):
        rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/bin/{name}"

    def stop_node(self, node):
        pids = self.pids(node)

        for pid in pids:
            node.account.signal(pid, signal.SIGTERM, allow_fail=False)

        timeout_sec = 30
        wait_until(lambda: len(self.pids(node)) == 0,
                   timeout_sec=timeout_sec,
                   err_msg="Redpanda node failed to stop in %d seconds" %
                   timeout_sec)

    def clean_node(self, node):
        node.account.kill_process("redpanda", clean_shutdown=False)
        node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/*")
        node.account.remove(f"{RedpandaService.CONFIG_FILE}")

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

        conf = self.render("redpanda.yaml",
                           node=node,
                           data_dir=RedpandaService.DATA_DIR,
                           cluster=RedpandaService.CLUSTER_NAME,
                           nodes=node_info,
                           node_id=self.idx(node),
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

    def restart_nodes(self, nodes, override_cfg_params=None):
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        for node in nodes:
            self.stop_node(node)
        for node in nodes:
            self.start_node(node, override_cfg_params)

    def registered(self, node):
        idx = self.idx(node)
        self.logger.debug(
            f"Checking if broker {idx} ({node.name} is registered")
        client = PythonLibrdkafka(self)
        brokers = client.brokers()
        broker = brokers.get(idx, None)
        self.logger.debug(f"Found broker info: {broker}")
        return broker is not None

    def controller(self):
        kc = KafkaCat(self)
        cid = kc.metadata()["controllerid"]
        self.logger.debug("Controller reported with id: {}".format(cid))
        if cid != -1:
            node = self.get_node(cid)
            self.logger.debug("Controller node found: {}".format(
                node.account.hostname))
            return node

    def node_storage(self, node):
        """
        Retrieve a summary of storage on a node.
        """
        def listdir(path, only_dirs=False):
            ents = node.account.sftp_client.listdir(path)
            if not only_dirs:
                return ents
            paths = map(lambda fn: (fn, os.path.join(path, fn)), ents)
            return [p[0] for p in paths if node.account.isdir(p[1])]

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
        cmd = f"find {RedpandaService.DATA_DIR} -type f -exec md5sum '{{}}' \; -exec stat -c %s '{{}}' \;"
        lines = node.account.ssh_output(cmd)
        tokens = lines.split()
        return {
            tokens[ix + 1].decode(): (tokens[ix].decode(), int(tokens[ix + 2]))
            for ix in range(0, len(tokens), 3)
        }

    def broker_address(self, node):
        assert node in self._started
        cfg = self.read_configuration(node)
        return f"{node.account.hostname}:{cfg['redpanda']['kafka_api']['port']}"

    def brokers(self, limit=None):
        return ",".join(self.brokers_list(limit))

    def brokers_list(self, limit=None):
        brokers = [self.broker_address(n) for n in self._started[:limit]]
        random.shuffle(brokers)
        return brokers

    def metrics(self, node):
        assert node in self._started
        url = f"http://{node.account.hostname}:9644/metrics"
        resp = requests.get(url)
        assert resp.status_code == 200
        return text_string_to_metric_families(resp.text)

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

    def describe_topics(self, topics=None):
        """
        Describe topics. Pass topics=None to describe all topics, or a pass a
        list of topic names to restrict the call to a set of specific topics.

        Sample return value:
            [
              {'error_code': 0,
               'topic': 'topic-kabn',
               'is_internal': False,
               'partitions': [
                 {'error_code': 0,
                  'partition': 0,
                  'leader': 1,
                  'replicas': [1],
                  'isr': [1],
                  'offline_replicas': []}
               }
            ]
        """
        return self._client.describe_topics(topics)

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

    def create_topic(self, specs):
        if isinstance(specs, TopicSpec):
            specs = [specs]
        client = self._client_type(self)
        for spec in specs:
            self.logger.debug(f"Creating topic {spec}")
            client.create_topic(spec)

    def delete_topic(self, name):
        client = self._client_type(self)
        self.logger.debug(f"Deleting topic {name}")
        client.delete_topic(name)

    def cov_enabled(self):
        return self._context.globals.get(self.COV_KEY, self.DEFAULT_COV_OPT)

    def set_resource_settings(self, rs):
        """
        Modify redpanda resource constraints after initial construction: effectively
        restarts redpanda with different -c/-m options.
        :param rs:
        :return:
        """
        if rs != self._resource_settings:
            for node in self.nodes:
                self.stop_node(node)

            self._resource_settings = rs

            for node in self.nodes:
                self.start_node(node)
