# Copyright 2020 Redpanda Data, Inc.
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
import json
from ducktape.utils.util import wait_until
from typing import Optional
import os


class AuthenticationError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class ClusterAuthorizationError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class KafkaCliTools:
    """
    Wrapper around the Kafka admin command line tools.
    """

    # See tests/docker/Dockerfile to add new versions
    VERSIONS = ("3.0.0", "2.7.0", "2.5.0", "2.4.1", "2.3.1")

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
        if spec.segment_bytes:
            args += ["--config", f"segment.bytes={spec.segment_bytes}"]
        if spec.retention_bytes:
            args += ["--config", f"retention.bytes={spec.retention_bytes}"]
        if spec.retention_ms:
            args += ["--config", f"retention.ms={spec.retention_ms}"]
        return self._run("kafka-topics.sh", args)

    def create_topic_partitions(self, topic, partitions):
        self._redpanda.logger.debug("Adding %d partitions to topic: %s",
                                    partitions, topic)
        args = ['--alter']
        args += ["--topic", topic]
        args += ["--partitions", f"{partitions}"]
        return self._run("kafka-topics.sh", args)

    def create_topic_with_config(self, name, partitions, replication_factor,
                                 configs):
        cfgs = [f"{k}={v}" for k, v in configs.items()]
        self._redpanda.logger.debug("Creating topic: %s", name)
        args = ["--create"]
        args += ["--topic", name]
        args += ["--partitions", str(partitions)]
        args += ["--replication-factor", str(replication_factor)]
        for it in cfgs:
            args += ["--config", it]
        return self._run("kafka-topics.sh", args)

    def create_topic_with_assignment(self, name, assignments):
        self._redpanda.logger.debug(
            f"Creating topic: {name}, custom assignment: {assignments}")

        partitions = []
        for assignment in assignments:
            partitions.append(":".join(str(r) for r in assignment))

        args = ["--create"]
        args += ["--topic", name]
        args += ["--replica-assignment", ",".join(partitions)]

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

    def describe_topic_config(self, topic):
        self._redpanda.logger.debug("Describing topic configs")
        args = ["--describe", "--topic", topic, "--all"]
        res = self._run("kafka-configs.sh", args).strip()
        self._redpanda.logger.debug("Describe topic configs result: %s", res)
        if res is None:
            raise RuntimeError(f"Error describing topic {topic}")

        # parse/extract the topic configuration
        configs = {}
        config_lines = res.split("\n")[1:]
        for line in config_lines:
            config = line.strip().split(" ")[0].split("=")
            configs[config[0]] = config[1]

        return configs

    def describe_topic(self, topic):
        self._redpanda.logger.debug("Describing topics")
        args = ["--describe", "--topic", topic]
        res = self._run("kafka-topics.sh", args)
        self._redpanda.logger.debug("Describe topics result: %s", res)
        if res is None:
            raise RuntimeError(f"Error describing topic {topic}")

        # parse/extract the topic configuration
        configs = None
        replication_factor = 0
        partitions = 0
        for part in [part.strip() for part in res.split("\t")]:
            if part.startswith("ReplicationFactor:"):
                replication_factor = int(part.split(":")[1].strip())
                continue
            if part.startswith("PartitionCount:"):
                partitions = int(part.split(":")[1].strip())
                continue
            if part.startswith("Configs:"):
                configs = part[8:]

        def maybe_int(key, value):
            if key in ["retention_ms", "retention_bytes", 'segment_bytes']:
                value = int(value)
            return value

        def fix_key(key):
            return key.replace(".", "_")

        self._redpanda.logger.debug(f"Describe topics configs: {configs}")
        configs = [
            config.split("=") for config in configs.split(",")
            if len(config) > 0
        ]

        configs = {fix_key(kv[0].strip()): kv[1].strip() for kv in configs}
        configs = {kv[0]: maybe_int(kv[0], kv[1]) for kv in configs.items()}
        configs["replication_factor"] = replication_factor
        configs["partition_count"] = partitions
        return TopicSpec(name=topic, **configs)

    def describe_broker_config(self):
        self._redpanda.logger.debug("Describing brokers")
        args = ["--describe", "--entity-type", "brokers", "--all"]
        res = self._run("kafka-configs.sh", args)
        self._redpanda.logger.debug("Describe brokers config result: %s", res)
        return res

    def alter_topic_config(self, topic, configuration_map):
        self._redpanda.logger.debug("Altering topic %s configuration with %s",
                                    topic, configuration_map)
        args = ["--topic", topic, "--alter"]
        args.append("--add-config")
        args.append(",".join(
            map(lambda item: f"{item[0]}={item[1]}",
                configuration_map.items())))

        return self._run("kafka-configs.sh", args)

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

    def list_acls(self):
        args = ["--list"]
        return self._run("kafka-acls.sh", args)

    def create_cluster_acls(self, username, op):
        """
        Add allow+describe+cluster ACL
        """
        args = ["--add"]
        args += ["--allow-principal", f"User:{username}"]
        args += ["--operation", op, "--cluster"]
        return self._run("kafka-acls.sh", args)

    def get_api_versions(self):
        return self._run("kafka-run-class.sh", [],
                         "kafka.admin.BrokerApiVersionsCommand")

    def reassign_partitions(self,
                            reassignments: dict,
                            operation: str,
                            msg_retry: Optional[str] = None,
                            timeout_s: int = 10):
        assert "version" in reassignments
        assert reassignments["version"] == 1
        assert "partitions" in reassignments

        args = []
        if operation == "execute":
            args.append("--execute")
        elif operation == "verify":
            args.append("--verify")
            args.append("--preserve-throttles")
        elif operation == "cancel":
            args.append("--cancel")
            args.append("--preserve-throttles")
        else:
            raise NotImplementedError(f"Unknown operation: {operation}")

        json_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        output = None
        try:
            json.dump(reassignments, json_file)
            json_file.close()

            args = args + ["--reassignment-json-file", json_file.name]

            def do_reassign_partitions():
                nonlocal output
                output = self._run("kafka-reassign-partitions.sh", args)
                return True if msg_retry is None else msg_retry not in output

            wait_until(do_reassign_partitions,
                       timeout_sec=timeout_s,
                       backoff_sec=1)
        finally:
            os.remove(json_file.name)

        return output

    def describe_producers(self, topic, partition):
        expected_columns = [
            "ProducerId", "ProducerEpoch", "LatestCoordinatorEpoch",
            "LastSequence", "LastTimestamp", "CurrentTransactionStartOffset"
        ]
        self._redpanda.logger.debug(
            "Describe producers for topic %s partition %s", topic, partition)
        cmd = [self._script("kafka-transactions.sh")]
        cmd += ["--bootstrap-server", self._redpanda.brokers()]
        cmd += ["describe-producers"]
        cmd += ["--topic", topic]
        cmd += ["--partition", str(partition)]
        res = self._execute(cmd)

        producers = []
        split_str = res.split("\n")
        info_str = split_str[0]
        info_key = info_str.strip().split("\t")
        assert info_key == expected_columns, f"{info_key}"

        assert split_str[-1] == ""

        lines = split_str[1:-1]
        for line in lines:
            producer_raw = line.strip().split("\t")
            assert len(producer_raw) == len(expected_columns)
            producer = {}
            for i in range(len(info_key)):
                producer_info = producer_raw[i].strip()
                producer[info_key[i]] = producer_info
            producers.append(producer)

        return producers

    def list_transactions(self):
        expected_columns = [
            "TransactionalId", "Coordinator", "ProducerId", "TransactionState"
        ]
        self._redpanda.logger.debug("List transactions")
        cmd = [self._script("kafka-transactions.sh")]
        cmd += ["--bootstrap-server", self._redpanda.brokers()]
        cmd += ["list"]
        res = self._execute(cmd)

        txs = []
        split_str = res.split("\n")
        info_str = split_str[0]
        info_key = info_str.strip().split("\t")
        assert info_key == expected_columns, f"{info_key}"

        assert split_str[-1] == ""

        lines = split_str[1:-1]
        for line in lines:
            tx_raw = line.strip().split("\t")
            assert len(tx_raw) == len(expected_columns)
            tx = {}
            for i in range(len(info_key)):
                tx_info = tx_raw[i].strip()
                tx[info_key[i]] = tx_info
            txs.append(tx)
        return txs

    def describe_transaction(self, tx_id):
        expected_columns = [
            "CoordinatorId", "TransactionalId", "ProducerId", "ProducerEpoch",
            "TransactionState", "TransactionTimeoutMs",
            "CurrentTransactionStartTimeMs", "TransactionDurationMs",
            "TopicPartitions"
        ]
        self._redpanda.logger.debug("Describe transaction tx_id: %s", tx_id)
        cmd = [self._script("kafka-transactions.sh")]
        cmd += ["--bootstrap-server", self._redpanda.brokers()]
        cmd += ["describe"]
        cmd += ["--transactional-id", str(tx_id)]
        res = self._execute(cmd)
        # 0 - keys, 1 - tx info, 2 - empty string
        split_str = res.split("\n")
        assert len(split_str) == 3

        info_str = info_str = split_str[0]
        info_key = info_str.strip().split("\t")
        assert info_key == expected_columns, f"{info_key}"

        assert split_str[2] == ""

        line = split_str[1]
        tx_raw = line.strip().split("\t")
        assert len(tx_raw) == len(expected_columns)

        tx = {}
        for i in range(len(info_key)):
            tx_info = tx_raw[i].strip()
            tx[info_key[i]] = tx_info
        return tx

    def _run(self, script, args, classname=None):
        cmd = [self._script(script)]
        if classname is not None:
            cmd += [classname]
        cmd += ["--bootstrap-server", self._redpanda.brokers()]
        if self._command_config:
            cmd += ["--command-config", self._command_config.name]
        cmd += args
        return self._execute(cmd)

    def _execute(self, cmd):
        """

        :param cmd: list of strings
        :return: stdout on success (raise on failure)
        """
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
            if "SASL authentication failed: security: Invalid credentials" in e.output:
                raise AuthenticationError(e.output)
            if "ClusterAuthorizationException: Cluster authorization failed" in e.output:
                raise ClusterAuthorizationError(e.output)
            raise

    def _script(self, script):
        version = self._version or KafkaCliTools.VERSIONS[0]
        return "/opt/kafka-{}/bin/{}".format(version, script)
