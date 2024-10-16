# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from __future__ import annotations

import os
import time
import signal
import threading
import requests
from typing import Any, Dict, Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteCommandError

from rptest.services.redpanda import RedpandaService

# Install location, specified by Dockerfile or AMI
TESTS_DIR = os.path.join("/opt", "kgo-verifier")

REMOTE_PORT_BASE = 8080


class KgoVerifierService(Service):
    """
    KgoVerifierService is kgo-verifier service.
    To validate produced record user should run consumer and producer in one node.
    Use ctx.cluster.alloc(ClusterSpec.simple_linux(1)) to allocate node and pass it to constructor
    """
    _status_thread: Optional[StatusThread]
    _stopped: bool

    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size,
                 custom_node,
                 debug_logs,
                 trace_logs,
                 username=None,
                 password=None,
                 enable_tls=False):
        self.use_custom_node = custom_node is not None

        # We should pass num_nodes to allocate for our service in BackgroundThreadService,
        # but if user allocate node by themself, BackgroundThreadService should not allocate any nodes
        nodes_for_allocate = 1
        if self.use_custom_node:
            nodes_for_allocate = 0

        super(KgoVerifierService, self).__init__(context,
                                                 num_nodes=nodes_for_allocate)

        # Should check that BackgroundThreadService did not allocate anything
        # and store allocated nodes by user to self.nodes
        if self.use_custom_node:
            assert not self.nodes
            self.nodes = custom_node

        self._redpanda: RedpandaService = redpanda
        self._topic = topic
        self._msg_size = msg_size
        self._pid = None
        self._remote_port = None
        self._debug_logs = debug_logs
        self._trace_logs = trace_logs
        self._username = username
        self._password = password
        self._enable_tls = enable_tls

        # if testing redpanda cloud, override with default test super user/pass
        if hasattr(redpanda, 'GLOBAL_CLOUD_CLUSTER_CONFIG'):
            security_config = redpanda.security_config()
            self._username = security_config.get('sasl_plain_username', None)
            self._password = security_config.get('sasl_plain_password', None)
            self._enable_tls = security_config.get('enable_tls', False)

        for node in self.nodes:
            if not hasattr(node, "kgo_verifier_ports"):
                node.kgo_verifier_ports = {}

        self._status_thread = None
        self._stopped = False

    def __del__(self):
        self._release_port()

    @classmethod
    def oneshot(cls, *args, **kwargs):
        """
        Convenience method for constructing, running and releasing node.

        Invoke with the same arguments as constructor, and optionally also
        `timeout_sec` if you would like to configure the wait timeout.

        Returns the finished instance, so that one can read its status methods
        to verify message counts etc
        """

        if 'timeout_sec' in kwargs:
            timeout_kwargs = {'timeout_sec': kwargs.pop('timeout_sec')}
        else:
            timeout_kwargs = {}

        inst = cls(*args, **kwargs)
        inst.start()
        inst.wait(**timeout_kwargs)
        inst.free()
        return inst

    def _release_port(self):
        for node in self.nodes:
            port_map = getattr(node, "kgo_verifier_ports", dict())
            if self.who_am_i() in port_map:
                del port_map[self.who_am_i()]

    def _select_port(self, node):
        ports_in_use = set(node.kgo_verifier_ports.values())
        i = REMOTE_PORT_BASE
        while i in ports_in_use:
            i = i + 1

        node.kgo_verifier_ports[self.who_am_i()] = i
        return i

    @property
    def log_path(self):
        return f"/tmp/{self.who_am_i()}.log"

    @property
    def logs(self):
        return {
            "kgo_verifier_output": {
                "path": self.log_path,
                "collect_default": True
            }
        }

    def spawn(self, cmd, node):
        assert self._pid is None

        self._remote_port = self._select_port(node)

        debug = '--debug' if self._debug_logs else ''
        trace = '--trace' if self._trace_logs else ''
        wrapped_cmd = f"nohup {cmd} --remote --remote-port {self._remote_port} {debug} {trace}>> {self.log_path} 2>&1 & echo $!"
        self.logger.debug(f"spawn {self.who_am_i()}: {wrapped_cmd}")
        pid_str = node.account.ssh_output(wrapped_cmd, timeout_sec=10)
        self.logger.debug(
            f"spawned {self.who_am_i()} node={node.name} pid={pid_str} port={self._remote_port}"
        )
        pid = int(pid_str.strip())
        self._pid = pid

        # Wait for status endpoint to respond.
        self._await_ready(node)

        # Because the above command was run with `nohup` we can't be sure that
        # it is the one who actually replied to the `await_ready` calls.
        # Check that the PID we just launched is still running as a confirmation
        # that it is the one.
        self._assert_running(node)

    def _await_ready(self, node):
        """
        Wait for the remote processes http endpoint to come up
        """

        wait_until(
            lambda: self._is_ready(node),
            timeout_sec=5,
            backoff_sec=0.5,
            err_msg=
            f"Timed out waiting for status endpoint {self.who_am_i()} to be available"
        )

    def _is_ready(self, node):
        try:
            r = requests.get(self._remote_url(node, "status"), timeout=10)
        except Exception as e:
            # Broad exception handling for any lower level connection errors etc
            # that might not be properly classed as `requests` exception.
            self.logger.debug(
                f"Status endpoint {self.who_am_i()} not ready: {e}")
            return False
        else:
            return r.status_code == 200

    def _assert_running(self, node):
        node.account.ssh_output(f"ps -p {self._pid}", allow_fail=False)

    def stop_node(self, node, **kwargs):
        if self._status_thread:
            self._status_thread.stop()
            self._status_thread.raise_on_error()
            self._status_thread = None
            # Record that we just stopped, so that we can't wait() after.
            # This is done inside this if statement because stop_node() is also
            # called during the start of the service to potentially stop a previous
            # instance of the service. Here, we know that we are stopping the service
            # that we started because it was us who initialized the _status_thread.
            self._stopped = True

        if self._pid is None:
            return

        self._redpanda.logger.info(f"{self.__class__.__name__}.stop")
        self.logger.debug("Killing pid %s" % {self._pid})
        try:
            node.account.signal(self._pid, signal.SIGKILL, allow_fail=False)
        except RemoteCommandError as e:
            if b"No such process" not in e.msg:
                raise

        self._pid = None
        self._release_port()

    def clean_node(self, node: ClusterNode):
        self._redpanda.logger.info(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process("kgo-verifier", clean_shutdown=False)
        node.account.remove("valid_offsets*json", True)
        node.account.remove(f"/tmp/{self.__class__.__name__}*", True)

    def _remote(self, node, action, timeout=60):
        """
        Send a request to the node to perform the given action, retrying
        periodically up to the given timeout.
        """
        url = self._remote_url(node, action)
        self._redpanda.logger.info(f"{self.who_am_i()} remote call: {url}")
        deadline = time.time() + timeout
        last_error = None
        while time.time() < deadline:
            try:
                r = requests.get(url, timeout=10)
                r.raise_for_status()
                return
            except Exception as e:
                last_error = e
                self._redpanda.logger.warning(
                    f"{self.who_am_i()} remote call failed, {e}")
                time.sleep(3)
        if last_error:
            raise last_error

    def wait_node(self, node, timeout_sec=None):
        """
        Wrapper to catch timeouts on wait, and send a `/print_stack` to the remote
        process in case it is experiencing a hang bug.
        """

        if self._stopped:
            raise RuntimeError(
                f"Can't wait {self.who_am_i()}. It was already stopped."
                f" You can either stop() a service or wait() and then stop() it"
                f" but not the other way around.")

        try:
            return self._do_wait_node(node, timeout_sec)
        except:
            try:
                self._remote(node, "print_stack")
            except Exception as e:
                self._redpanda.logger.warning(
                    f"{self.who_am_i()} failed to print stacks during wait failure: {e}"
                )

            raise

    def _do_wait_node(self, node, timeout_sec):
        """
        Wait for the remote process to gracefully finish: if it is a one-shot
        operation this waits for all work to complete, if it is a looping
        operation then we wait for the current iteration of the loop to finish
        by triggering the /last_pass endpoint and then waiting for active=false.

        When this returns, the remote process is no longer running, and our
        _status member is populated with the final status before the remote process
        process ended.
        """
        if not self._status_thread:
            return True

        self.logger.debug(
            f"wait_node {self.who_am_i()}: waiting for remote endpoint")
        self._await_ready(node)

        # If this is a looping worker, tell it to end after the current loop
        self.logger.debug(f"wait_node {self.who_am_i()}: requesting last_pass")
        self._remote(node, "last_pass")

        # Let the worker fall through to the end of its current iteration
        self.logger.debug(
            f"wait_node {self.who_am_i()}: waiting for worker to complete")
        self._redpanda.wait_until(
            lambda: self._status.active is False or self._status_thread.
            errored,
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg=
            f"{self.who_am_i()} didn't complete in {timeout_sec} seconds")
        self._status_thread.raise_on_error()

        # Read final status
        self.logger.debug(f"wait_node {self.who_am_i()}: reading final status")
        self._status_thread.shutdown()
        self._status_thread = None

        # Permit the subprocess to exit, and wait for it to do so
        self.logger.debug(f"wait_node {self.who_am_i()}: requesting shutdown")
        try:
            self._remote(node, "shutdown")
        except requests.exceptions.ConnectionError:
            # It is permitted for the remote process to abort connection and fail
            # to send a response, as it does not wait for HTTP response to flush
            # before shutting down.
            pass

        self.logger.debug(
            f"wait_node {self.who_am_i()}: waiting node={node.name} pid={self._pid} to terminate"
        )
        wait_until(lambda: not node.account.exists(f"/proc/{self._pid}"),
                   timeout_sec=10,
                   backoff_sec=0.5)
        self._pid = None

        self.logger.debug(
            f"wait_node {self.who_am_i()}: node={node.name} pid={self._pid} terminated"
        )

        self._release_port()

        return True

    def _remote_url(self, node, path):
        assert self._remote_port is not None
        return f"http://{node.account.hostname}:{self._remote_port}/{path}"

    def allocate_nodes(self):
        if self.use_custom_node:
            return
        else:
            return super(KgoVerifierService, self).allocate_nodes()

    def free(self):
        if self.use_custom_node:
            return
        else:
            return super(KgoVerifierService, self).free()


class StatusThread(threading.Thread):
    INTERVAL = 5

    def __init__(self, parent: Service, node, status_cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True

        self._parent = parent
        self._node = node
        self._status_cls = status_cls
        self._ex = None
        self._ready = False

        self._shutdown_requested = threading.Event()
        self._stop_requested = threading.Event()

    @property
    def errored(self):
        return self._ex is not None

    @property
    def who_am_i(self):
        return self._parent.who_am_i()

    @property
    def logger(self):
        return self._parent.logger

    def raise_on_error(self):
        if self._ex is not None:
            raise self._ex

    def run(self):
        try:
            self.poll_status()
        except Exception as ex:
            self._ex = ex
            self.logger.exception(
                f"Error reading status from {self.who_am_i} on {self._node.name}"
            )

    def _ingest_status(self, worker_statuses):
        self.logger.debug(f"{self.who_am_i} status: {worker_statuses}")
        reduced = self._status_cls(**worker_statuses[0])
        for s in worker_statuses[1:]:
            reduced.merge(self._status_cls(**s))

        if self._status_cls == ProduceStatus:
            progress = (worker_statuses[0]['sent'] /
                        float(self._parent._msg_count))
            self.logger.info(
                f"Producer {self.who_am_i} progress: {progress*100:.2f}% {reduced}"
            )
        else:
            self.logger.info(f"Worker {self.who_am_i} status: {reduced}")

        self._parent._status = reduced

    def poll_status(self):
        while not self._stop_requested.is_set():
            drop_out = self._shutdown_requested.is_set()

            r = requests.get(self._parent._remote_url(self._node, "status"),
                             timeout=5)
            r.raise_for_status()
            worker_statuses = r.json()
            self._ingest_status(worker_statuses)

            if drop_out:
                # We were asked to clean shutdown and we have done our final
                # status read
                return
            else:
                self._shutdown_requested.wait(self.INTERVAL)

    def join_with_timeout(self):
        """
        Join thread with a modest timeout, and raise an exception if
        we do not succeed.  We expect to join promptly because all our
        run() is doing is calling to the remote process status endpoint, and
        that requests.get() has a timeout on it, so should not block.

        This is important because otherwise a stuck join() would hang
        the entire ducktape test run.
        """
        self.join(timeout=10)
        if self.is_alive():
            msg = f"Failed to join thread for {self.who_am_i}"
            self.logger.error(msg)
            raise RuntimeError(msg)

    def stop(self):
        """
        Drop out of poll loop as soon as possible, and join.
        """
        self._shutdown_requested.set()
        self._stop_requested.set()
        self.join_with_timeout()

    def shutdown(self):
        """
        Read status one more time, then drop out of poll loop and join.
        """
        self._shutdown_requested.set()
        self.join_with_timeout()


class ValidatorStatus:
    """
    All validating consumers have one of these as part of their status object
    internally to kgo-verifier.  Other parts of consumer status are allowed to
    differ per-worker, although at time of writing they don't.
    """
    lost_offsets: Dict[str, int]

    def __init__(self, name: str, valid_reads: int, invalid_reads: int,
                 out_of_scope_invalid_reads: int,
                 max_offsets_consumed: Optional[int], lost_offsets: Dict[str,
                                                                         int]):
        # Validator name is just a unique name per worker thread in kgo-verifier: useful in logging
        # but we mostly don't care
        self.name = name

        self.valid_reads = valid_reads
        self.invalid_reads = invalid_reads
        self.out_of_scope_invalid_reads = out_of_scope_invalid_reads
        self.max_offsets_consumed = max_offsets_consumed
        self.lost_offsets = lost_offsets

    @property
    def total_reads(self):
        # At time of writing, invalid reads is never nonzero, because the program
        # terminates as soon as it sees an invalid read
        return self.valid_reads + self.out_of_scope_invalid_reads

    def merge(self, rhs: ValidatorStatus):
        # Clear name if we are merging multiple statuses together, to avoid confusion.
        self.name = ""

        # Clear other fields we aren't interested in, to avoid confusion.
        self.max_offsets_consumed = None
        self.lost_offsets = {}

        self.valid_reads += rhs.valid_reads
        self.invalid_reads += rhs.invalid_reads
        self.out_of_scope_invalid_reads += rhs.out_of_scope_invalid_reads

    def __str__(self):
        return f"ValidatorStatus<" \
            f"valid_reads={self.valid_reads}, " \
            f"invalid_reads={self.invalid_reads}, " \
            f"out_of_scope_invalid_reads={self.out_of_scope_invalid_reads}, " \
            f"lost_offsets={self.lost_offsets}>"


class ConsumerStatus:
    def __init__(self,
                 topic: Optional[str] = None,
                 validator: dict[str, Any] | None = None,
                 errors: int = 0,
                 active: bool = True):
        """
        `active` defaults to True, because we use it for deciding when to drop out in `wait()` -- the initial
        state of a worker should be presumed that it is busy, and we must wait to see it go `active=False`
        before proceeding with `wait()`
        """
        if validator is None:
            validator = {
                'valid_reads': 0,
                'invalid_reads': 0,
                'out_of_scope_invalid_reads': 0,
                'name': "",
                'max_offsets_consumed': dict(),
                'lost_offsets': dict()
            }

        self.validator = ValidatorStatus(**validator)
        self.errors = errors
        self.active = active

    def merge(self, rhs: ConsumerStatus):
        self.active = self.active or rhs.active
        self.errors += rhs.errors
        self.validator.merge(rhs.validator)

    def __str__(self):
        return f"ConsumerStatus<{self.active}, {self.errors}, {self.validator}>"


class KgoVerifierProducer(KgoVerifierService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size,
                 msg_count,
                 custom_node=None,
                 batch_max_bytes=None,
                 debug_logs=False,
                 trace_logs=False,
                 fake_timestamp_ms=None,
                 fake_timestamp_step_ms=None,
                 use_transactions=False,
                 transaction_abort_rate=None,
                 msgs_per_transaction=None,
                 rate_limit_bps=None,
                 key_set_cardinality=None,
                 username=None,
                 password=None,
                 enable_tls=False,
                 msgs_per_producer_id=None,
                 max_buffered_records=None,
                 tolerate_data_loss=False):
        super(KgoVerifierProducer,
              self).__init__(context, redpanda, topic, msg_size, custom_node,
                             debug_logs, trace_logs, username, password,
                             enable_tls)
        self._msg_count = msg_count
        self._status = ProduceStatus()
        self._batch_max_bytes = batch_max_bytes
        self._fake_timestamp_ms = fake_timestamp_ms
        self._fake_timestamp_step_ms = fake_timestamp_step_ms
        self._use_transactions = use_transactions
        self._transaction_abort_rate = transaction_abort_rate
        self._msgs_per_transaction = msgs_per_transaction
        self._rate_limit_bps = rate_limit_bps
        self._key_set_cardinality = key_set_cardinality
        self._msgs_per_producer_id = msgs_per_producer_id
        self._max_buffered_records = max_buffered_records
        self._tolerate_data_loss = tolerate_data_loss

    @property
    def produce_status(self):
        return self._status

    def wait_node(self, node, timeout_sec=None):
        if not self._status_thread:
            return True

        what = f"{self.who_am_i()} wait: awaiting message count"
        self.logger.debug(what)
        try:
            self._redpanda.wait_until(lambda: self._status_thread.errored or
                                      self._status.acked >= self._msg_count,
                                      timeout_sec=timeout_sec,
                                      backoff_sec=self._status_thread.INTERVAL,
                                      err_msg=what)
        except:
            self.stop_node(node)
            raise

        self._status_thread.raise_on_error()

        if self._status.bad_offsets != 0:
            # This either means that the test sent multiple producers' traffic to
            # the same topic, or that Redpanda showed a buggy behavior with
            # idempotency: producer records should always land at the next offset
            # after the last record they wrote.
            if self._tolerate_data_loss:
                self._redpanda.logger.warning(
                    f"{self.who_am_i()} observed data loss: {self._status}")
            else:
                raise RuntimeError(
                    f"{self.who_am_i()} possible idempotency bug: {self._status}"
                )

        return super().wait_node(node, timeout_sec=timeout_sec)

    def wait_for_acks(self, count, timeout_sec, backoff_sec):
        self._redpanda.wait_until(
            lambda: self._status_thread.errored or self._status.acked >= count,
            timeout_sec=timeout_sec,
            backoff_sec=backoff_sec)
        self._status_thread.raise_on_error()

    def wait_for_offset_map(self):
        # Producer worker aims to checkpoint every 5 seconds, so we should see this promptly.
        self._redpanda.wait_until(lambda: self._status_thread.errored or all(
            node.account.exists(f"valid_offsets_{self._topic}.json")
            for node in self.nodes),
                                  timeout_sec=15,
                                  backoff_sec=1)
        self._status_thread.raise_on_error()

    def is_complete(self):
        return self._status.acked >= self._msg_count

    def start_node(self, node, clean=False):
        if clean:
            self.clean_node(node)

        cmd = f"{TESTS_DIR}/kgo-verifier --brokers {self._redpanda.brokers()} --topic {self._topic} --msg_size {self._msg_size} --produce_msgs {self._msg_count} --rand_read_msgs 0 --seq_read=0 --client-name {self.who_am_i()}"

        if self._username is not None:
            cmd = cmd + f' --username {self._username}'

        if self._password is not None:
            cmd = cmd + f' --password {self._password}'

        if self._enable_tls:
            cmd = cmd + f' --enable-tls'

        if self._batch_max_bytes is not None:
            cmd = cmd + f' --batch_max_bytes {self._batch_max_bytes}'

        if self._fake_timestamp_ms is not None:
            cmd = cmd + f' --fake-timestamp-ms {self._fake_timestamp_ms}'

        if self._fake_timestamp_step_ms is not None:
            cmd = cmd + f' --fake-timestamp-step-ms {self._fake_timestamp_step_ms}'

        if self._use_transactions:
            cmd = cmd + f' --use-transactions'

            if self._msgs_per_transaction is not None:
                cmd = cmd + f' --msgs-per-transaction {self._msgs_per_transaction}'

            if self._transaction_abort_rate is not None:
                cmd = cmd + f' --transaction-abort-rate {self._transaction_abort_rate}'

        if self._rate_limit_bps is not None:
            cmd = cmd + f' --produce-throughput-bps {self._rate_limit_bps}'

        if self._key_set_cardinality is not None:
            cmd += f" --key-set-cardinality {self._key_set_cardinality}"
        if self._msgs_per_producer_id is not None:
            cmd += f" --msgs-per-producer-id {self._msgs_per_producer_id}"

        if self._max_buffered_records is not None:
            cmd += f" --max-buffered-records {self._max_buffered_records}"

        if self._tolerate_data_loss:
            cmd += " --tolerate-data-loss"

        self.spawn(cmd, node)

        self._status_thread = StatusThread(self, node, ProduceStatus)
        self._status_thread.start()


class AbstractConsumer(KgoVerifierService):
    _status: ConsumerStatus

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._status = ConsumerStatus()

    @property
    def consumer_status(self) -> ConsumerStatus:
        return self._status

    def wait_total_reads(self, count, timeout_sec, backoff_sec):
        self.logger.info("Waiting for total reads to reach %d", count)

        self._redpanda.wait_until(
            lambda: self._status_thread.errored or self.consumer_status.
            validator.total_reads >= count,
            timeout_sec=timeout_sec,
            backoff_sec=backoff_sec)
        self._status_thread.raise_on_error()


class KgoVerifierSeqConsumer(AbstractConsumer):
    def __init__(
            self,
            context,
            redpanda,
            topic,
            msg_size=None,  # TODO: redundant, remove
            max_msgs=None,
            max_throughput_mb=None,
            nodes=None,
            debug_logs=False,
            trace_logs=False,
            loop=True,
            continuous=False,
            tolerate_data_loss=False,
            producer: Optional[KgoVerifierProducer] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            enable_tls: Optional[bool] = False,
            use_transactions: Optional[bool] = False):
        super().__init__(context, redpanda, topic, msg_size, nodes, debug_logs,
                         trace_logs, username, password, enable_tls)
        self._max_msgs = max_msgs
        self._max_throughput_mb = max_throughput_mb
        self._loop = loop
        self._continuous = continuous
        self._tolerate_data_loss = tolerate_data_loss
        self._producer = producer
        self._use_transactions = use_transactions

    def start_node(self, node, clean=False):
        if clean:
            self.clean_node(node)

        loop = "--loop" if self._loop else ""
        cmd = f"{TESTS_DIR}/kgo-verifier --brokers {self._redpanda.brokers()} --topic {self._topic} --produce_msgs 0 --rand_read_msgs 0 --seq_read=1 {loop} --client-name {self.who_am_i()}"
        if self._username is not None:
            cmd = cmd + f' --username {self._username}'
        if self._password is not None:
            cmd = cmd + f' --password {self._password}'
        if self._enable_tls:
            cmd = cmd + f' --enable-tls'
        if self._max_msgs is not None:
            cmd += f" --seq_read_msgs {self._max_msgs}"
        if self._max_throughput_mb is not None:
            cmd += f" --consume-throughput-mb {self._max_throughput_mb}"
        if self._continuous:
            cmd += " --continuous"
        if self._tolerate_data_loss:
            cmd += " --tolerate-data-loss"
        if self._use_transactions:
            cmd += " --use-transactions"
        self.spawn(cmd, node)

        self._status_thread = StatusThread(self, node, ConsumerStatus)
        self._status_thread.start()

    def wait_node(self, node, timeout_sec=None):
        if self._producer:

            def consumed_whole_log():
                producer_done = self._producer._status.sent == self._producer._msg_count
                if not producer_done:
                    self.logger.debug(
                        f"Producer {self._producer.who_am_i()} hasn't finished yet"
                    )
                    return False

                consumed = self._status.validator.max_offsets_consumed
                produced = self._producer._status.max_offsets_produced
                if consumed != produced:
                    self.logger.debug(
                        f"Consumer {self.who_am_i()} hasn't read all produced data yet: {consumed=} {produced=}"
                    )
                    return False
                return True

            wait_until(
                consumed_whole_log,
                timeout_sec=timeout_sec,
                backoff_sec=2,
                err_msg=
                f"Consumer hasn't read all produced data: consumed={self._status.validator.max_offsets_consumed} produced={self._producer._status.max_offsets_produced}"
            )

        return super().wait_node(node, timeout_sec=timeout_sec)


class KgoVerifierRandomConsumer(AbstractConsumer):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size,
                 rand_read_msgs,
                 parallel,
                 nodes=None,
                 debug_logs=False,
                 trace_logs=False,
                 username=None,
                 password=None,
                 enable_tls=False,
                 use_transactions: Optional[bool] = False):
        super().__init__(context, redpanda, topic, msg_size, nodes, debug_logs,
                         trace_logs, username, password, enable_tls)
        self._rand_read_msgs = rand_read_msgs
        self._parallel = parallel
        self._use_transactions = use_transactions

    def start_node(self, node, clean=False):
        if clean:
            self.clean_node(node)

        cmd = f"{TESTS_DIR}/kgo-verifier --brokers {self._redpanda.brokers()} --topic {self._topic} --produce_msgs 0 --rand_read_msgs {self._rand_read_msgs} --parallel {self._parallel} --seq_read=0 --loop --client-name {self.who_am_i()}"
        if self._username is not None:
            cmd = cmd + f' --username {self._username}'
        if self._password is not None:
            cmd = cmd + f' --password {self._password}'
        if self._enable_tls:
            cmd = cmd + f' --enable-tls'
        if self._use_transactions:
            cmd += " --use-transactions"

        self.spawn(cmd, node)

        self._status_thread = StatusThread(self, node, ConsumerStatus)
        self._status_thread.start()


class KgoVerifierConsumerGroupConsumer(AbstractConsumer):
    _status: ConsumerStatus
    _group_name: Optional[str]

    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size,
                 readers,
                 loop=False,
                 max_msgs=None,
                 max_throughput_mb=None,
                 nodes=None,
                 debug_logs=False,
                 trace_logs=False,
                 username=None,
                 password=None,
                 enable_tls=False,
                 continuous=False,
                 tolerate_data_loss=False,
                 group_name=None,
                 use_transactions=False):
        super().__init__(context, redpanda, topic, msg_size, nodes, debug_logs,
                         trace_logs, username, password, enable_tls)

        self._readers = readers
        self._loop = loop
        self._max_msgs = max_msgs
        self._max_throughput_mb = max_throughput_mb
        self._group_name = group_name
        self._continuous = continuous
        self._tolerate_data_loss = tolerate_data_loss
        self._use_transactions = use_transactions

    def start_node(self, node, clean=False):
        if clean:
            self.clean_node(node)

        cmd = f"{TESTS_DIR}/kgo-verifier --brokers {self._redpanda.brokers()} --topic {self._topic} --produce_msgs 0 --rand_read_msgs 0 --seq_read=0 --consumer_group_readers={self._readers} --client-name {self.who_am_i()}"
        if self._username is not None:
            cmd = cmd + f' --username {self._username}'
        if self._password is not None:
            cmd = cmd + f' --password {self._password}'
        if self._enable_tls:
            cmd = cmd + f' --enable-tls'
        if self._loop:
            cmd += " --loop"
        if self._max_msgs is not None:
            cmd += f" --seq_read_msgs {self._max_msgs}"
        if self._max_throughput_mb is not None:
            cmd += f" --consume-throughput-mb {self._max_throughput_mb}"
        if self._continuous:
            cmd += " --continuous"
        if self._tolerate_data_loss:
            cmd += " --tolerate-data-loss"
        if self._group_name is not None:
            cmd += f" --consumer_group_name {self._group_name}"
        if self._use_transactions:
            cmd += " --use-transactions"
        self.spawn(cmd, node)

        self._status_thread = StatusThread(self, node, ConsumerStatus)
        self._status_thread.start()


class ProduceStatus:
    def __init__(self,
                 topic=None,
                 sent=0,
                 acked=0,
                 bad_offsets=0,
                 max_offsets_produced=dict(),
                 restarts=0,
                 latency=None,
                 active=False,
                 failed_transactions=0,
                 aborted_transaction_msgs=0):
        self.topic = topic
        self.sent = sent
        self.acked = acked
        self.bad_offsets = bad_offsets
        self.max_offsets_produced = max_offsets_produced
        self.restarts = restarts
        if latency is None:
            latency = {'p50': 0, 'p90': 0, 'p99': 0}
        self.latency = latency
        self.active = active
        self.failed_transactions = failed_transactions
        self.aborted_transaction_messages = aborted_transaction_msgs

    def __str__(self):
        l = self.latency
        return f"ProduceStatus<{self.sent} {self.acked} {self.bad_offsets} {self.restarts} {self.failed_transactions} {self.aborted_transaction_messages} {l['p50']}/{l['p90']}/{l['p99']}>"
