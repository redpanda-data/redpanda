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
TESTS_DIR = os.path.join("/opt", "si-verifier")

from enum import Enum


class ServiceStatus(Enum):
    SETUP = 1
    RUNNING = 2
    FINISH = 3


class FranzGoVerifiableService(BackgroundThreadService):
    def __init__(self, context, redpanda, topic, msg_size, nodes):
        self.custom_nodes = nodes is not None

        if nodes:
            num_nodes = 0
        else:
            num_nodes = 1

        super(FranzGoVerifiableService, self).__init__(context,
                                                       num_nodes=num_nodes)

        if nodes:
            assert not self.nodes
            self.nodes = nodes

        self._redpanda = redpanda
        self._topic = topic
        self._msg_size = msg_size
        self._stopping = threading.Event()
        self._exception = None
        self.status = ServiceStatus.SETUP
        self._pid = None

    def _worker(self, idx, node):
        pass

    def stop_node(self, node):
        self._stopping.set()

        if self.status is ServiceStatus.RUNNING:
            try:
                if self._pid is not None:
                    self.logger.debug("Killing pid %s" % {self._pid})
                    node.account.signal(self._pid, 9, allow_fail=True)
                else:
                    self.logger.debug("Killing si-verifier")
                    node.account.kill_process("si-verifier",
                                              clean_shutdown=False)
            except RemoteCommandError as e:
                if b"No such process" in e.msg:
                    pass
                else:
                    raise

        if self._exception is not None:
            raise self._exception

    def allocate_nodes(self):
        if self.custom_nodes:
            return
        else:
            return super(FranzGoVerifiableService, self).allocate_nodes()

    def free_all(self):
        if self.custom_nodes:
            return
        else:
            return super(FranzGoVerifiableService, self).free_all()


class FranzGoVerifiableSeqConsumer(FranzGoVerifiableService):
    def __init__(self, context, redpanda, topic, msg_size, nodes=None):
        super(FranzGoVerifiableSeqConsumer,
              self).__init__(context, redpanda, topic, msg_size, nodes)

    def _worker(self, idx, node):
        self.status = ServiceStatus.RUNNING
        self._stopping.clear()
        try:
            while not self._stopping.is_set():
                cmd = 'echo $$ ; %s --brokers %s --topic %s --msg_size %s --produce_msgs 0 --rand_read_msgs 0 --seq_read=1' % (
                    f"{TESTS_DIR}/si-verifier", self._redpanda.brokers(),
                    self._topic, self._msg_size)

                for line in node.account.ssh_capture(cmd):
                    if self._pid is None:
                        self._pid = line.strip()

                    self.logger.debug(line.rstrip())
                    if self._stopping.is_set():
                        break
        except Exception as ex:
            if self._stopping.is_set():
                pass
            else:
                self._exception = ex
                raise ex
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
            while not self._stopping.is_set():
                cmd = 'echo $$ ; %s --brokers %s --topic %s --msg_size %s --produce_msgs 0 --rand_read_msgs %s --parallel %s --seq_read=0' % (
                    f"{TESTS_DIR}/si-verifier", self._redpanda.brokers(),
                    self._topic, self._msg_size, self._rand_read_msgs,
                    self._parallel)

                for line in node.account.ssh_capture(cmd):
                    if self._pid is None:
                        self._pid = line.strip()

                    self.logger.debug(line.rstrip())
                    if self._stopping.is_set():
                        break
        except Exception as ex:
            if self._stopping.is_set():
                pass
            else:
                self._exception = ex
                raise ex
        finally:
            self.status = ServiceStatus.FINISH


class FranzGoVerifiableProducer(FranzGoVerifiableService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size,
                 msg_count,
                 nodes=None):
        super(FranzGoVerifiableProducer,
              self).__init__(context, redpanda, topic, msg_size, nodes)
        self._msg_count = msg_count

    def _worker(self, idx, node):
        self.status = ServiceStatus.RUNNING
        self._stopping.clear()
        try:
            cmd = 'echo $$ ; %s --brokers %s --topic %s --msg_size %s --produce_msgs %s --rand_read_msgs 0 --seq_read=0' % (
                f"{TESTS_DIR}/si-verifier", self._redpanda.brokers(),
                self._topic, self._msg_size, self._msg_count)

            for line in node.account.ssh_capture(cmd):
                if self._pid is None:
                    self._pid = line.strip()

                self.logger.debug(line.rstrip())
                if self._stopping.is_set():
                    break
        except Exception as ex:
            if self._stopping.is_set():
                pass
            else:
                self._exception = ex
                raise ex
        finally:
            self.status = ServiceStatus.FINISH
