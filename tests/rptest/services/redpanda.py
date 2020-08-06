import os
import signal

import yaml
from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.storage import ClusterStorage, NodeStorage


class RedpandaService(Service):
    PERSISTENT_ROOT = "/mnt/redpanda"
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "redpanda.yaml")
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.log")
    CLUSTER_NAME = "my_cluster"
    READY_TIMEOUT_SEC = 10

    logs = {
        "redpanda_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        },
    }

    def __init__(self, context, num_brokers, extra_rp_conf=None, topics=None):
        super(RedpandaService, self).__init__(context, num_nodes=num_brokers)
        self._extra_rp_conf = extra_rp_conf
        self._topics = topics or dict()

    def start(self):
        super(RedpandaService, self).start()
        self.logger.info("Waiting for all brokers to join cluster")

        expected = set(self.nodes)
        wait_until(lambda: {n
                            for n in self.nodes
                            if self.registered(n)} == expected,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Cluster membership did not stabilize")

        kafka_tools = KafkaCliTools(self)
        for topic, cfg in self._topics.items():
            self.logger.debug("Creating initial topic %s / %s", topic, cfg)
            kafka_tools.create_topic(topic, **cfg)

    def start_node(self, node):
        node.account.mkdirs(RedpandaService.DATA_DIR)
        self.write_conf_file(node)

        cmd = "nohup {} ".format(self.redpanda_binary())
        cmd += "--redpanda-cfg {} ".format(RedpandaService.CONFIG_FILE)
        cmd += ">> {0} 2>&1 &".format(RedpandaService.STDOUT_STDERR_CAPTURE)

        self.logger.info(
            "Starting Redpanda service on {} with command: {}".format(
                node.account, cmd))

        # wait until redpanda has finished booting up
        with node.account.monitor_log(
                RedpandaService.STDOUT_STDERR_CAPTURE) as mon:
            node.account.ssh(cmd)
            mon.wait_until(
                "Successfully started Redpanda!",
                timeout_sec=RedpandaService.READY_TIMEOUT_SEC,
                backoff_sec=0.5,
                err_msg="Redpanda didn't finish startup in {} seconds".format(
                    RedpandaService.READY_TIMEOUT_SEC))

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
        node.account.remove(RedpandaService.PERSISTENT_ROOT)

    def redpanda_binary(self):
        # TODO: i haven't yet figured out what the blessed way of getting
        # parameters into the test are to control which build we use. but they
        # are all available under the /opt/v/build directory.
        return "/opt/v/build/debug/clang/dist/local/bin/redpanda"

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "ps ax | grep -i redpanda | grep -v grep | awk '{print $1}'"
            pid_arr = [
                pid for pid in node.account.ssh_capture(
                    cmd, allow_fail=True, callback=int)
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def write_conf_file(self, node):
        node_info = {self.idx(n): n for n in self.nodes}

        conf = self.render("redpanda.yaml",
                           node=node,
                           data_dir=RedpandaService.DATA_DIR,
                           cluster=RedpandaService.CLUSTER_NAME,
                           nodes=node_info,
                           node_id=self.idx(node))

        if self._extra_rp_conf:
            doc = yaml.load(conf)
            self.logger.debug(
                "Setting custom Redpanda configuration options: {}".format(
                    self._extra_rp_conf))
            doc["redpanda"].update(self._extra_rp_conf)
            conf = yaml.dump(doc)

        self.logger.info("Writing Redpanda config file: {}".format(
            RedpandaService.CONFIG_FILE))
        self.logger.debug(conf)
        node.account.create_file(RedpandaService.CONFIG_FILE, conf)

    def registered(self, node):
        idx = self.idx(node)
        self.logger.debug("Checking if broker %d/%s is registered", idx, node)
        kc = KafkaCat(self)
        brokers = kc.metadata()["brokers"]
        brokers = {b["id"]: b for b in brokers}
        broker = brokers.get(idx, None)
        self.logger.debug("Found broker info: %s", broker)
        return broker is not None

    def controller(self):
        kc = KafkaCat(self)
        cid = kc.metadata()["controllerid"]
        self.logger.debug("Controller reported with id: {}".format(cid))
        if cid != -1:
            node = self.get_node(cid)
            self.logger.debug("Controller node found: {}".format(node))
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
        for node in self.nodes:
            s = self.node_storage(node)
            store.add_node(s)
        return store
