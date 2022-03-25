# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import threading
from ducktape.services.background_thread import BackgroundThreadService

# The franz-go root directory
TESTS_DIR = os.path.join("/opt", "kgo-verifier")

from enum import Enum


class ServiceStatus(Enum):
    SETUP = 1
    RUNNING = 2
    FINISH = 3


class FranzGoVerifiableService(BackgroundThreadService):
    """
    FranzGoVerifiableService is kgo-verifier service.
    To validate produced record user should run consumer and producer in one node.
    Use ctx.cluster.alloc(ClusterSpec.simple_linux(1)) to allocate node and pass it to constructor
    """
    def __init__(self, context, redpanda, topic, msg_size, custom_node):
        self.use_custom_node = custom_node is not None

        # We should pass num_nodes to allocate for our service in BackgroundThreadService,
        # but if user allocate node by themself, BackgroundThreadService should not allocate any nodes
        nodes_for_allocate = 1
        if self.use_custom_node:
            nodes_for_allocate = 0

        super(FranzGoVerifiableService,
              self).__init__(context, num_nodes=nodes_for_allocate)

        # Should check that BackgroundThreadService did not allocate anything
        # and store allocated nodes by user to self.nodes
        if self.use_custom_node:
            assert not self.nodes
            self.nodes = custom_node

        self._redpanda = redpanda
        self._topic = topic
        self._msg_size = msg_size
        self._stopping = threading.Event()
        self._shutting_down = threading.Event()
        self._exception = None
        self.status = ServiceStatus.SETUP
        self._pid = None

    def _worker(self, idx, node):
        raise NotImplementedError()

    def execute_cmd(self, cmd, node):
        for line in node.account.ssh_capture(cmd):
            if self._pid is None:
                self._pid = line.strip()

            self.logger.debug(line.rstrip())
            if self._stopping.is_set():
                break

    def save_exception(self, ex):
        if self._stopping.is_set():
            pass
        else:
            self._redpanda.logger.exception(
                f"Error from {self.__class__.__name__}:")
            self._exception = ex
            raise ex

    def stop_node(self, node):
        self._redpanda.logger.info(f"{self.__class__.__name__}.stop")
        self._stopping.set()

        if self.status is ServiceStatus.RUNNING:
            try:
                if self._pid is not None:
                    self.logger.debug("Killing pid %s" % {self._pid})
                    node.account.signal(self._pid, 9, allow_fail=True)
                else:
                    self.logger.debug("Killing kgo-verifier")
                    node.account.kill_process("kgo-verifier",
                                              clean_shutdown=False)
            except RemoteCommandError as e:
                if b"No such process" not in e.msg:
                    raise

        if self._exception is not None:
            raise self._exception

    def clean_node(self, node):
        self._redpanda.logger.info(f"{self.__class__.__name__}.clean_node")
        node.account.kill_process("kgo-verifier", clean_shutdown=False)
        node.account.remove("valid_offsets*json", True)

    def start_node(self, node, clean=None):
        # Ignore `clean`, it is processed by Service.start.  We just need to ignore
        # it here because the default BackgroundThreadService start_node doesn't
        # handle it.
        super().start_node(node)

    def shutdown(self):
        """
        Unlike `stop`, which stops immediately, this will let the consumer run until the
        end of its current scan, and does not suppress exceptions from the consumer.
        Follow this with a call to wait().
        """
        self._redpanda.logger.info(f"{self.__class__.__name__}.shutdown")
        self._shutting_down.set()

    def allocate_nodes(self):
        if self.use_custom_node:
            return
        else:
            return super(FranzGoVerifiableService, self).allocate_nodes()

    def free(self):
        if self.use_custom_node:
            return
        else:
            return super(FranzGoVerifiableService, self).free_all()


class FranzGoVerifiableSeqConsumer(FranzGoVerifiableService):
    def __init__(self, context, redpanda, topic, msg_size, nodes=None):
        super(FranzGoVerifiableSeqConsumer,
              self).__init__(context, redpanda, topic, msg_size, nodes)

        self._shutting_down = threading.Event()

    def _worker(self, idx, node):
        self.status = ServiceStatus.RUNNING
        self._stopping.clear()
        try:
            while not self._stopping.is_set(
            ) and not self._shutting_down.is_set():
                cmd = 'echo $$ ; %s --brokers %s --topic %s --msg_size %s --produce_msgs 0 --rand_read_msgs 0 --seq_read=1' % (
                    f"{TESTS_DIR}/kgo-verifier", self._redpanda.brokers(),
                    self._topic, self._msg_size)
                self.execute_cmd(cmd, node)
        except Exception as ex:
            self.save_exception(ex)
        finally:
            self.status = ServiceStatus.FINISH


class FranzGoVerifiableRandomConsumer(FranzGoVerifiableService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size,
                 rand_read_msgs,
                 parallel,
                 nodes=None):
        super(FranzGoVerifiableRandomConsumer,
              self).__init__(context, redpanda, topic, msg_size, nodes)
        self._rand_read_msgs = rand_read_msgs
        self._parallel = parallel

    def _worker(self, idx, node):
        self.status = ServiceStatus.RUNNING
        self._stopping.clear()
        try:
            while not self._stopping.is_set(
            ) and not self._shutting_down.is_set():
                cmd = 'echo $$ ; %s --brokers %s --topic %s --msg_size %s --produce_msgs 0 --rand_read_msgs %s --parallel %s --seq_read=0' % (
                    f"{TESTS_DIR}/kgo-verifier", self._redpanda.brokers(),
                    self._topic, self._msg_size, self._rand_read_msgs,
                    self._parallel)

                self.execute_cmd(cmd, node)
        except Exception as ex:
            self.save_exception(ex)
        finally:
            self.status = ServiceStatus.FINISH


class FranzGoVerifiableProducer(FranzGoVerifiableService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size,
                 msg_count,
                 custom_node=None):
        super(FranzGoVerifiableProducer,
              self).__init__(context, redpanda, topic, msg_size, custom_node)
        self._msg_count = msg_count

    def _worker(self, idx, node):
        self.status = ServiceStatus.RUNNING
        self._stopping.clear()
        try:
            cmd = 'echo $$ ; %s --brokers %s --topic %s --msg_size %s --produce_msgs %s --rand_read_msgs 0 --seq_read=0' % (
                f"{TESTS_DIR}/kgo-verifier", self._redpanda.brokers(),
                self._topic, self._msg_size, self._msg_count)

            self.execute_cmd(cmd, node)
        except Exception as ex:
            self.save_exception(ex)
        finally:
            self.status = ServiceStatus.FINISH
