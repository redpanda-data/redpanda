# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import tempfile
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_client import KafkaClient


class KafkaCliTools(KafkaClient):
    """
    Wrapper around the Kafka admin command line tools.
    """

    # See tests/docker/Dockerfile to add new versions
    VERSIONS = ("2.5.0", "2.4.1", "2.3.1")

    def __init__(self, redpanda, version=None, user=None, passwd=None):
        self._redpanda = redpanda
        self._version = version
        assert self._version is None or \
                self._version in KafkaCliTools.VERSIONS
        self._command_config = None
        if user and passwd:
            self._command_config = tempfile.NamedTemporaryFile(mode="w")
            config = f"""
sasl.mechanism=SCRAM-SHA-256
security.protocol=SASL_PLAINTEXT
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{user}" password="{passwd}";
"""
            self._command_config.write(config)
            self._command_config.flush()

    @classmethod
    def instances(cls):
        def make_factory(version):
            return lambda redpanda: cls(redpanda, version)

        return list(map(make_factory, cls.VERSIONS))

    def create_topic(self, spec):
        self._redpanda.logger.debug("Creating topic: %s", spec.name)
        args = ["--create"]
        args += ["--topic", spec.name]
        args += ["--partitions", str(spec.partition_count)]
        args += ["--replication-factor", str(spec.replication_factor)]
        if spec.cleanup_policy:
            args += [
                "--config", "cleanup.policy={}".format(spec.cleanup_policy)
            ]
        return self._run("kafka-topics.sh", args)

    def delete_topic(self, topic):
        self._redpanda.logger.debug("Deleting topic: %s", topic)
        args = ["--delete"]
        args += ["--topic", topic]
        return self._run("kafka-topics.sh", args)

    def list_topics(self):
        self._redpanda.logger.debug("Listing topics")
        args = ["--list"]
        res = self._run("kafka-topics.sh", args)
        topics = res.split()
        self._redpanda.logger.debug("Found topics: %s", topics)
        return topics

    def describe_topics(self):
        self._redpanda.logger.debug("Describing topics")
        args = ["--describe"]
        res = self._run("kafka-topics.sh", args)
        self._redpanda.logger.debug("Describe topics result: %s", res)
        return res

    def describe_topic(self, topic):
        self._redpanda.logger.debug("Describing topics")
        args = ["--describe", "--topic", topic]
        res = self._run("kafka-topics.sh", args)
        self._redpanda.logger.debug("Describe topics result: %s", res)

        # parse/extract the topic configuration
        configs = None
        for part in [part.strip() for part in res.split("\t")]:
            if part.startswith("Configs:"):
                configs = part[8:]

        def maybe_int(key, value):
            if key in ["partition_count", "replication_factor"]:
                value = int(value)
            return value

        def fix_key(key):
            if key == "cleanup.policy":
                return "cleanup_policy"
            return key

        self._redpanda.logger.debug(f"Describe topics configs: {configs}")
        configs = [config.split("=") for config in configs.split(",")]
        configs = {fix_key(kv[0].strip()): kv[1].strip() for kv in configs}
        configs = {kv[0]: maybe_int(kv[0], kv[1]) for kv in configs.items()}
        return TopicSpec(name=topic, **configs)

    def produce(self,
                topic,
                num_records,
                record_size,
                acks=-1,
                throughput=-1,
                batch_size=81960):
        self._redpanda.logger.debug("Producing to topic: %s", topic)
        cmd = [self._script("kafka-producer-perf-test.sh")]
        cmd += ["--topic", topic]
        cmd += ["--num-records", str(num_records)]
        cmd += ["--record-size", str(record_size)]
        cmd += ["--throughput", str(throughput)]
        cmd += [
            "--producer-props",
            "acks=%d" % acks, "client.id=ducktape",
            "batch.size=%d" % batch_size,
            "bootstrap.servers=%s" % self._redpanda.brokers()
        ]
        self._execute(cmd)

    def _run(self, script, args):
        cmd = [self._script(script)]
        cmd += ["--bootstrap-server", self._redpanda.brokers()]
        if self._command_config:
            cmd += ["--command-config", self._command_config.name]
        cmd += args
        return self._execute(cmd)

    def _execute(self, cmd):
        self._redpanda.logger.debug("Executing command: %s", cmd)
        try:
            res = subprocess.check_output(cmd,
                                          stderr=subprocess.STDOUT,
                                          text=True)
            self._redpanda.logger.debug(res)
            return res
        except subprocess.CalledProcessError as e:
            self._redpanda.logger.debug("Error (%d) executing command: %s",
                                        e.returncode, e.output)

    def _script(self, script):
        version = self._version or KafkaCliTools.VERSIONS[0]
        return "/opt/kafka-{}/bin/{}".format(version, script)
