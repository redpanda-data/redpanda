# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum
import json
import re
import threading, time
from typing import Any, Callable, Optional
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError, SSHOutputIter
import paramiko.channel


class KcatConsumer(BackgroundThreadService):
    """
    kcat based standalone consumer service

    A standalone kcat consumer allows to run consumers in dedicated nodes,
    which is a desired layout for the tests that involve high throughput cases.
    It is easier to manage that to spawn an instance of ducktape in a separate
    node and still communicate to it via ssh.
    """
    class OffsetMeta(Enum):
        beginning = "beginning"
        end = "end"
        stored = "stored"

    class OffsetDefaultMeta(Enum):
        beginning = "beginning"
        end = "end"
        error = "error"

    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 partition: Optional[int] = None,
                 *,
                 offset: Any = None,
                 first_timestamp: Optional[int] = None,
                 offset_default: Optional[OffsetDefaultMeta] = None,
                 num_msgs: Optional[int] = None,
                 cgroup_name: Optional[str] = None,
                 auto_commit_interval_ms: Optional[int] = None,
                 caption: Optional[str] = None):
        """
        offset : int or OffsetMeta or None
            The offset to start consuming from. Must be None if first_timestamp
            is specified.
        first_timestamp : int, optional
            The timestamp to start consuming at. Must be None if offset is
            specified.
        offset_default : OffsetDefaultMeta, optional
            The offset to fallback if offset or first_timestamp have a specific
            value or the value OffsetMeta.stored, but there is no initial offset
            in offset store or the desired offset is out of range. When not
            specified, the default librdkafka option is 'end'. The value of
            OffsetDefaultMeta.error would trigger an error
            (ERR__AUTO_OFFSET_RESET) if the default needed to be applied.
        """

        super(KcatConsumer, self).__init__(context, num_nodes=1)
        self._stopping = threading.Event()
        self._pid = None
        self._end_of_topic = {}
        self._consumed_count = {}
        self._on_message = KcatConsumer.default_on_message
        self._redpanda = redpanda
        self._topic = topic
        self._partition = partition
        if caption:
            self._redpanda.logger.info(f"{caption} is {self.service_id}")
            self._caption = f"[{caption}] "
        else:
            self._caption = f"[{self.service_id}] "

        self._cmd = ["kcat", "-b", self._redpanda.brokers()]
        trailopts = ["-J"]

        if cgroup_name is None:
            self._cmd += ["-C", "-t", f"{topic}"]
        else:
            assert partition is None, "Partition argument is not used in high-level consumer mode"
            self._cmd += ["-G", cgroup_name]
            trailopts += [topic]

        if auto_commit_interval_ms is None:
            self._cmd += ["-X", "enable.auto.commit=false"]
        else:
            self._cmd += [
                "-X", "enable.auto.commit=true", "-X",
                f"auto.commit.interval.ms={auto_commit_interval_ms}"
            ]

        if partition:
            self._cmd += ["-p", f"{partition}"]
        assert (offset is None) or (
            first_timestamp
            is None), "Cannot specify both offset and timestamp"

        if offset is not None:
            # <value>  (absolute offset)
            if isinstance(offset, int):
                self._cmd += ["-o", f"{offset}"]
            elif isinstance(offset, KcatConsumer.OffsetMeta):
                self._cmd += ["-o", offset.value]
            else:
                assert False, "Offset must be an integer or an OffsetMeta"

        if first_timestamp is not None:
            # s@<value> (timestamp in ms to start at)
            self._cmd += ["-o", f"s@{first_timestamp}"]

        if offset_default is not None:
            if isinstance(offset_default, KcatConsumer.OffsetDefaultMeta):
                self._cmd += [
                    "-X", f"auto.offset.reset={offset_default.value}"
                ]
            else:
                assert False, "offset_default must be an OffsetMeta"

        if num_msgs is not None:
            self._cmd += ["-c", f"{num_msgs}"]

        if getattr(self._redpanda, "sasl_enabled", lambda: False)():
            cfg = self._redpanda.security_config()
            self._cmd += [
                "-X", f"security.protocol={cfg['security_protocol']}", "-X"
                f"sasl.mechanism={cfg['sasl_mechanism']}", "-X",
                f"sasl.username={cfg['sasl_plain_username']}", "-X",
                f"sasl.password={cfg['sasl_plain_password']}"
            ]
            if cfg['sasl_mechanism'] == "GSSAPI":
                self._cmd += [
                    "-X", "sasl.kerberos.service.name=redpanda",
                    '-Xsasl.kerberos.kinit.cmd=kinit client -t /var/lib/redpanda/client.keytab'
                ]

        self._cmd += trailopts

    class SSHCapturedPipes:
        def __init__(self, stdin: paramiko.channel.ChannelStdinFile,
                     stdout: paramiko.channel.ChannelFile,
                     stderr: paramiko.channel.ChannelStderrFile, cmd: str,
                     logger) -> None:
            self.stdin = stdin
            self.stdout = stdout
            self.stderr = stderr
            self._cmd = cmd
            self.logger = logger

        def __enter__(self) -> 'KcatConsumer.SSHCapturedPipes':
            return self

        def __exit__(self, exc_type, exc_value, traceback) -> None:
            try:
                exit_status = self.stdout.channel.recv_exit_status()
                if exit_status != 0:
                    if self.logger is not None:
                        self.logger.debug(
                            "Running ssh command '%s' exited with status %d and message: %s"
                            % (self._cmd, exit_status, self.stderr.read()))
                    else:
                        raise RemoteCommandError(self, self._cmd, exit_status,
                                                 self.stderr.read())
            finally:
                self.stdin.close()
                self.stdout.close()
                self.stderr.close()

    def ssh_capture_with_pipes(self,
                               node,
                               cmd,
                               allow_fail=False,
                               timeout_sec=None):
        self._redpanda.logger.debug("Running ssh command: %s" % cmd)

        client = node.account.ssh_client
        chan = client.get_transport().open_session(timeout=timeout_sec)

        chan.settimeout(timeout_sec)
        chan.exec_command(cmd)
        chan.set_combine_stderr(False)

        stdin = chan.makefile('wb', -1)  # set bufsize to -1
        stdout = chan.makefile('r', -1)
        stderr = chan.makefile_stderr('r', -1)

        return KcatConsumer.SSHCapturedPipes(
            stdin, stdout, stderr, cmd,
            self._redpanda.logger if allow_fail else None)

    def _worker(self, worker_idx, node):
        self._stopping.clear()
        try:
            cmd = "echo $$ ; " + " ".join(self._cmd)
            with self.ssh_capture_with_pipes(node, cmd) as p:
                stderr_reader = threading.Thread(
                    name=self.service_id + "-errrdr-" + str(worker_idx),
                    target=self._stderr_reader,
                    args=(worker_idx, node, p.stderr))
                stderr_reader.start()
                try:
                    self._stdout_reader(p.stdout)
                finally:
                    stderr_reader.join()

        except:
            if self._stopping.is_set():
                # Expect a non-zero exit code when killing during teardown
                pass
            else:
                raise
        finally:
            self.done = True

    def _stdout_reader(self, stdout: paramiko.channel.ChannelFile) -> None:
        #self._redpanda.logger.debug(f"start ({stdout})")
        for line in iter(stdout.readline, ''):
            if self._pid is None:
                self._pid = line.strip()
                continue

            if self._stopping.is_set():
                break

            try:
                j = json.loads(line)
                partition = int(j["partition"])
                self._end_of_topic[partition] = None
                self._consumed_count.setdefault(partition, 0)
                self._consumed_count[partition] += 1
                self._on_message(self, j)
            except:
                self._redpanda.logger.error(
                    f"{self._caption}Exception while processing kcat output line: {line.strip()}"
                )
                raise

    def _stderr_reader(self, node_idx, node,
                       stderr: paramiko.channel.ChannelStderrFile) -> None:
        for line in iter(stderr.readline, ''):
            if self._stopping.is_set():
                break

            if line[:1] == "%":
                m = re.search(
                    "Reached end of topic (?P<topic>.+) \[(?P<partition>\d+)\] at offset (?P<offset>\d+)",
                    line)
                if m:
                    partition = int(m.group("partition"))
                    if m.group("topic") != self._topic:
                        self._redpanda.logger.warning(
                            "{}Topic reported by kcat ({}}) is different from the requested ({}). Line: {}"
                            .format(self._caption, m.group("topic"),
                                    self._topic, line.strip()))
                    elif self._partition is not None and partition != self._partition:
                        self._redpanda.logger.warning(
                            "{}Partition reported by kcat ({}) is different from the requested ({}). Line: {}"
                            .format(self._caption, partition, self._partition,
                                    line.strip()))
                    else:
                        self._end_of_topic[partition] = int(m.group("offset"))
                        self._redpanda.logger.debug(
                            f"{self._caption}Reached end of the topic partition {partition} at offset {self._end_of_topic[partition]}"
                        )
                else:
                    self._redpanda.logger.debug(
                        f"{self._caption}{line.strip()}")
            else:
                self._redpanda.logger.warn(
                    f"{self._caption}Unrecognized kcat output: {line.strip()}")

    def stop_node(self, node):
        self._stopping.set()
        if self._pid is not None:
            self.logger.debug(f"{self._caption}Killing pid {self._pid}")
            node.account.signal(self._pid, 9, allow_fail=True)
        else:
            node.account.kill_process("kcat", clean_shutdown=False)

    def clean_node(self, node, **kwargs):
        pass

    @property
    def consumed_total(self):
        return sum(self._consumed_count.values())

    def set_on_message(
            self, on_message: Callable[['KcatConsumer', dict], None]) -> None:
        self._on_message = on_message

    def default_on_message(self, message: dict) -> None:
        self._redpanda.logger.debug(f"{self._caption}{json.dumps(message)}")
