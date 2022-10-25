from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError
from threading import Event
from typing import Optional


class RpkProducer(BackgroundThreadService):
    """
    Wrap the `rpk topic produce` command.  This is useful if you need
    to write a simple repeated value to a topic, for example to increase
    the topic's size on disk when testing recovery.
    """
    def __init__(self,
                 context,
                 redpanda,
                 topic: str,
                 msg_size: int,
                 msg_count: int,
                 acks: Optional[int] = None,
                 printable=False,
                 quiet: bool = False,
                 produce_timeout: Optional[int] = None,
                 *,
                 partition: Optional[int] = None):
        super(RpkProducer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._msg_size = msg_size
        self._msg_count = msg_count
        self._acks = acks
        self._printable = printable
        self._stopping = Event()
        self._quiet = quiet
        self._output_line_count = 0
        self._partition = partition

        if produce_timeout is None:
            produce_timeout = 10
        self._produce_timeout = produce_timeout

    def _worker(self, _idx, node):
        # NOTE: since this runs on separate nodes from the service, the binary
        # path used by each node may differ from that returned by
        # redpanda.find_binary(), e.g. if using a RedpandaInstaller.
        rp_install_path_root = self._redpanda._context.globals.get(
            "rp_install_path_root", None)
        rpk_binary = f"{rp_install_path_root}/bin/rpk"
        key_size = 16
        cmd = f"dd if=/dev/urandom bs={self._msg_size + key_size} count={self._msg_count}"

        if self._printable:
            cmd += ' | hexdump -e "1/1 \\"%02x\\""'

        cmd += f" | {rpk_binary} topic --brokers {self._redpanda.brokers()} produce --compression none {self._topic} -f '%V{{{self._msg_size}}}%K{{{key_size}}}%k%v'"

        if self._acks is not None:
            cmd += f" --acks {self._acks}"

        if self._quiet:
            # Suppress default "Produced to..." output lines by setting output template to empty string
            cmd += " -o \"\""

        if self._partition is not None:
            cmd += f" -p {self._partition}"

        self._stopping.clear()
        try:
            for line in node.account.ssh_capture(
                    cmd, timeout_sec=self._produce_timeout):
                self.logger.debug(line.rstrip())
                self._output_line_count += 1
        except RemoteCommandError:
            if self._stopping.is_set():
                pass
            else:
                raise

        self._redpanda.logger.debug(
            f"Finished sending {self._msg_count} messages")

    @property
    def output_line_count(self):
        return self._output_line_count

    def stop_node(self, node):
        self._stopping.set()
        node.account.kill_process("rpk", clean_shutdown=False)
