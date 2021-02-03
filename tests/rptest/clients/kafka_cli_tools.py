# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess


class KafkaCliTools:
    """
    Wrapper around the Kafka admin command line tools.
    """

    # See tests/docker/Dockerfile to add new versions
    VERSIONS = ("2.5.0", "2.4.1", "2.3.1")

    def __init__(self, redpanda, version=None):
        self._redpanda = redpanda
        self._version = version
        assert self._version is None or \
                self._version in KafkaCliTools.VERSIONS

    def create_topic(self,
                     topic,
                     partitions=1,
                     replication_factor=3,
                     cleanup_policy=None):
        self._redpanda.logger.debug("Creating topic: %s", topic)
        args = ["--create"]
        args += ["--topic", topic]
        args += ["--partitions", str(partitions)]
        args += ["--replication-factor", str(replication_factor)]
        if cleanup_policy:
            args += ["--config", "cleanup.policy={}".format(cleanup_policy)]
        return self._run("kafka-topics.sh", args)

    def create_topic_from_spec(self, spec):
        return self.create_topic(spec.name,
                                 partitions=spec.partitions,
                                 replication_factor=spec.replication_factor,
                                 cleanup_policy=spec.cleanup_policy)

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
