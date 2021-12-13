from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError
from threading import Event


class RpkProducer(BackgroundThreadService):
    """
    Wrap the `rpk topic produce` command.  This is useful if you need
    to write a simple repeated value to a topic, for example to increase
    the topic's size on disk when testing recovery.
    """
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size,
                 msg_count,
                 acks=None,
                 user=None,
                 password=None,
                 quiet=False,
                 nodes=None):
        """

        :param context:
        :param redpanda:
        :param topic: string, topic name
        :param msg_size: integer
        :param msg_count: integer, bytes
        :param acks: 0, 1 or -1
        :param quiet: if true, no per-message logging will be emitted
        :param nodes: list of Node, if set then no nodes will be auto-allocated
        """
        self.custom_nodes = nodes is not None

        super(RpkProducer, self).__init__(context, num_nodes=1)

        if nodes:
            assert not self.nodes
            self.nodes = nodes

        self._redpanda = redpanda
        self._topic = topic
        self._msg_size = msg_size
        self._msg_count = msg_count
        self._acks = acks
        self._user = user
        self._password = password
        self._stopping = Event()
        self._quiet = quiet

    def _worker(self, _idx, node):
        rpk_binary = self._redpanda.find_binary("rpk")

        auth_args = ""
        if self._user:
            # Auth enabled
            auth_args = f"--user {self._user} --password {self._password} --sasl-mechanism SCRAM-SHA-256"

        cmd = f"dd if=/dev/urandom bs={self._msg_size} count={self._msg_count} | {rpk_binary} topic {auth_args} --brokers {self._redpanda.brokers()} produce --compression none --key test {self._topic} -f '%V{{{self._msg_size}}}%v'"
        if self._acks is not None:
            cmd += f" --acks {self._acks}"

        self._stopping.clear()
        try:
            for line in node.account.ssh_capture(cmd, timeout_sec=10):
                if not self._quiet:
                    self.logger.debug(line.rstrip())
        except RemoteCommandError as e:
            if self._stopping.is_set():
                pass
            else:
                raise

    def stop_node(self, node):
        self._stopping.set()
        node.account.kill_process("rpk", clean_shutdown=False)

    def allocate_nodes(self):
        if self.custom_nodes:
            return
        else:
            return super(RpkProducer, self).allocate_nodes()

    def free_all(self):
        if self.custom_nodes:
            return
        else:
            return super(RpkProducer, self).free_all()
