# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from ducktape.services.service import Service
from ducktape.utils.util import wait_until


class MirrorMaker2(Service):

    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/mirror_maker"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "mirror_maker2.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "mm2-log4j.properties")
    MM2_CONFIG = os.path.join(PERSISTENT_ROOT, "mirror_maker2.properties")

    logs = {"mirror_maker_log": {"path": LOG_FILE, "collect_default": True}}

    def __init__(self,
                 context,
                 num_nodes,
                 source_cluster,
                 target_cluster,
                 log_level="DEBUG",
                 consumer_group_pattern=None):
        super(MirrorMaker2, self).__init__(context, num_nodes=num_nodes)
        self.log_level = log_level
        self.source = source_cluster
        self.target = target_cluster
        self.cg_pattern = consumer_group_pattern

    def start_cmd(self, node):
        cmd = f"export LOG_DIR={MirrorMaker2.LOG_DIR};"
        cmd += f" export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:{MirrorMaker2.LOG4J_CONFIG}\";"
        cmd += self.path("connect-mirror-maker.sh")
        cmd += f" {MirrorMaker2.MM2_CONFIG}"
        cmd += f" 1>> {MirrorMaker2.LOG_FILE} 2>> {MirrorMaker2.LOG_FILE} &"

        return cmd

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def path(self, script):

        version = '3.0.0'
        return "/opt/kafka-{}/bin/{}".format(version, script)

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % MirrorMaker2.PERSISTENT_ROOT,
                         allow_fail=False)
        node.account.ssh("mkdir -p %s" % MirrorMaker2.LOG_DIR,
                         allow_fail=False)

        mm2_props = self.render("mirror_maker2.properties",
                                source_brokers=self.source.brokers(),
                                target_brokers=self.target.brokers(),
                                cg_pattern=self.cg_pattern)

        node.account.create_file(MirrorMaker2.MM2_CONFIG, mm2_props)
        self.logger.info(f"Mirrormaker config: {mm2_props}")

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties',
                                 log_file=MirrorMaker2.LOG_FILE)
        node.account.create_file(MirrorMaker2.LOG4J_CONFIG, log_config)

        # Run mirror maker
        cmd = self.start_cmd(node)
        self.logger.debug("Mirror maker command: %s", cmd)
        node.account.ssh(cmd, allow_fail=False)
        wait_until(lambda: self.alive(node),
                   timeout_sec=30,
                   backoff_sec=.5,
                   err_msg="Mirror maker took to long to start.")
        self.logger.debug("Mirror maker is alive")

    def stop_node(self, node, clean_shutdown=True):
        node.account.kill_java_processes(self.java_class_name(),
                                         allow_fail=True,
                                         clean_shutdown=clean_shutdown)
        wait_until(lambda: not self.alive(node),
                   timeout_sec=30,
                   backoff_sec=.5,
                   err_msg="Mirror maker took to long to stop.")

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn(
                "%s %s was still alive at cleanup time. Killing forcefully..."
                % (self.__class__.__name__, node.account))
        node.account.kill_java_processes(self.java_class_name(),
                                         clean_shutdown=False,
                                         allow_fail=True)
        node.account.ssh("rm -rf %s" % MirrorMaker2.PERSISTENT_ROOT,
                         allow_fail=False)

    def java_class_name(self):
        return "org.apache.kafka.connect.mirror.MirrorMaker"
