# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from abc import ABC, abstractmethod
from enum import Enum, auto, unique
import json
import random
import re
import string
import requests
from threading import Event, Condition
import threading
import time
from requests.exceptions import HTTPError
from ducktape.utils.util import wait_until
from rptest.clients.kcl import KCL
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from time import sleep
from confluent_kafka import Producer
from typing import Dict

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.redpanda_installer import VERSION_RE, int_tuple


# Operation context (used to save state between invocation of operations)
class OperationCtx:
    def __init__(self, redpanda):
        self.redpanda = redpanda

    def rpk(self):
        return RpkTool(self.redpanda)

    def admin(self):
        return Admin(self.redpanda, retry_codes=[503, 504])

    def kcl(self):
        return KCL(self.redpanda)


# Base class for operation
class Operation(ABC):
    @abstractmethod
    def execute(self, ctx) -> bool:
        ...

    def validate(self, ctx) -> bool:
        ...


def random_string(length):
    return ''.join(
        [random.choice(string.ascii_lowercase) for i in range(0, length)]
    )  # Using only lower case to avoid getting "ERROR" or other string that would be perceived as an error in the log


@unique
class RedpandaAdminOperation(Enum):
    CREATE_TOPIC = auto()
    DELETE_TOPIC = auto()
    UPDATE_TOPIC = auto()
    ADD_PARTITIONS = auto()
    CREATE_USER = auto()
    DELETE_USER = auto()
    CREATE_ACLS = auto()
    UPDATE_CONFIG = auto()
    PRODUCE_TO_TOPIC = auto()
    DELETE_RECORDS = auto()


def _random_choice(prefix, collection):
    filtered = list(filter(lambda t: t.startswith(prefix), collection))
    if len(filtered) == 0:
        return None
    return random.choice(filtered)


def _choice_random_topic(ctx, prefix):
    return _random_choice(prefix, ctx.rpk().list_topics())


def _choice_random_user(ctx, prefix):
    return _random_choice(prefix, ctx.admin().list_users())


TOPIC_PROPERTIES = {
    TopicSpec.PROPERTY_CLEANUP_POLICY:
    lambda: random.choice(["delete", "compact"]),
    TopicSpec.PROPERTY_TIMESTAMP_TYPE:
    lambda: random.choice(["CreateTime", "LogAppendTime"]),
    TopicSpec.PROPERTY_SEGMENT_SIZE:
    lambda: random.randint(10 * 2**20, 512 * 2**20),
    TopicSpec.PROPERTY_RETENTION_BYTES:
    lambda: random.randint(10 * 2**20, 512 * 2**20),
    TopicSpec.PROPERTY_RETENTION_TIME:
    lambda: random.randint(10000, 1000000),
}


# topic operations
class CreateTopicOperation(Operation):
    def __init__(self, prefix, max_partitions, min_replication,
                 max_replication):
        self.prefix = prefix
        self.topic = f'{prefix}-{random_string(6)}'
        self.partitions = random.randint(1, max_partitions)
        self.rf = random.choice(
            [x for x in range(min_replication, max_replication + 1, 2)])
        self.config = {
            k: v()
            for k, v in TOPIC_PROPERTIES.items()
            if random.choice([True, False])
        }

    def execute(self, ctx):
        ctx.redpanda.logger.info(
            f"Creating topic with name {self.topic}, replication: {self.rf} partitions: {self.partitions}"
        )
        ctx.rpk().create_topic(self.topic, self.partitions, self.rf,
                               self.config)
        return True

    def validate(self, ctx):
        if self.topic is None:
            return False
        ctx.redpanda.logger.info(f"Validating topic {self.topic} creation")
        topics = ctx.rpk().list_topics()
        if self.topic not in topics:
            return False
        else:
            desc = ctx.rpk().describe_topic_configs(self.topic)
            return all([desc[k][0] == str(v) for k, v in self.config.items()])

    def describe(self):
        return {
            "type": "create_topic",
            "properties": {
                "name": self.topic,
                "replication_factor": self.topic,
                "partitions": self.topic,
                "config": self.config,
            }
        }


class DeleteTopicOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.topic = None

    def execute(self, ctx):
        if self.topic is None:
            self.topic = _choice_random_topic(ctx, prefix=self.prefix)
        if self.topic is None:
            return False
        ctx.redpanda.logger.info(f"Deleting topic: {self.topic}")
        ctx.rpk().delete_topic(self.topic)
        return True

    def validate(self, ctx):
        if self.topic is None:
            return False
        ctx.redpanda.logger.info(f"Validating topic {self.topic} deletion")
        # validate RPK output first
        topics = ctx.rpk().list_topics()
        if self.topic in topics:
            ctx.redpanda.logger.info(
                f"found deleted topic {self.topic} in RPK response: {topics}")
            return False

        try:
            brokers = ctx.admin().get_brokers()
        except:
            return False
        # since metadata in Redpanda and Kafka are eventually consistent
        # we must check all the nodes before proceeding
        for b in brokers:
            n = ctx.redpanda.get_node_by_id(b["node_id"])
            if n not in ctx.redpanda.started_nodes():
                continue
            # we will check does topic_table contain any info about our topic and partition 0. If not we can assume that topic was deleted
            try:
                ctx.admin().get_partitions(node=n,
                                           topic=self.topic,
                                           partition=0)
                ctx.redpanda.logger.info(
                    f"found deleted topic {self.topic} on node {n.account.hostname}"
                )
                return False
            except requests.exceptions.HTTPError as e:
                assert e.response.status_code == 404

        return True

    def describe(self):
        return {
            "type": "delete_topic",
            "properties": {
                "name": self.topic,
            }
        }


class UpdateTopicOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.topic = None
        self.property = None
        self.value = None

    def execute(self, ctx):
        if self.topic is None:
            self.topic = _choice_random_topic(ctx, prefix=self.prefix)
            if self.topic is None:
                return False
            self.property = random.choice(list(TOPIC_PROPERTIES.keys()))
            self.value = TOPIC_PROPERTIES[self.property]()

        ctx.redpanda.logger.info(
            f"Updating topic: {self.topic} with: {self.property}={self.value}")
        ctx.rpk().alter_topic_config(self.topic, self.property,
                                     str(self.value))
        return True

    def validate(self, ctx):
        if self.topic is None:
            return False
        ctx.redpanda.logger.info(
            f"Validating topic {self.topic} update, expected: {self.property}={self.value}"
        )

        desc = ctx.rpk().describe_topic_configs(self.topic)
        return desc[self.property][0] == str(self.value)

    def describe(self):
        return {
            "type": "update_topic_properties",
            "properties": {
                "name": self.topic,
                "key": self.property,
                "value": self.value,
            }
        }


class AddPartitionsOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.topic = None
        self.total = None
        self.current = None

    def _current_partition_count(self, ctx):

        per_node_count = set()
        try:
            brokers = ctx.admin().get_brokers()
        except:
            return None
        for b in brokers:
            n = ctx.redpanda.get_node_by_id(b['node_id'])
            if n not in ctx.redpanda.started_nodes():
                continue
            try:
                partitions = ctx.admin().get_partitions(node=n,
                                                        topic=self.topic)
                per_node_count.add(len(partitions))
            except HTTPError as err:
                if err.response.status_code == 404:
                    return None
                else:
                    raise

        if len(per_node_count) != 1:
            ctx.redpanda.logger.info(
                f"inconsistent partition count for {self.topic}: {per_node_count}"
            )
            return None

        return next(iter(per_node_count))

    def execute(self, ctx):
        if self.topic is None:
            self.topic = _choice_random_topic(ctx, prefix=self.prefix)
        if self.topic is None:
            return False

        if self.current is None:
            self.current = self._current_partition_count(ctx)
        if self.current is None:
            return False
        if self.total is None:
            self.total = random.randint(self.current + 1, self.current + 5)
        ctx.redpanda.logger.info(
            f"Updating topic: {self.topic} partitions count. Current partition count: {self.current} new partition count: {self.total}"
        )
        cli = KafkaCliTools(ctx.redpanda)
        cli.create_topic_partitions(self.topic, self.total)
        return True

    def validate(self, ctx):
        if self.topic is None:
            return False
        ctx.redpanda.logger.info(
            f"Validating topic {self.topic} partitions update")
        current = len(list(ctx.rpk().describe_topic(self.topic)))
        return current == self.total

    def describe(self):
        return {
            "type": "add_topic_partitions",
            "properties": {
                "name": self.topic,
                "total": self.total
            }
        }


class CreateUserOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.user = f'{prefix}-user-{random_string(6)}'
        self.password = f'{prefix}-user-{random_string(6)}'
        self.algorithm = "SCRAM-SHA-256"

    def execute(self, ctx):
        ctx.redpanda.logger.info(f"Creating user: {self.user}")
        ctx.admin().create_user(self.user, self.password, self.algorithm)
        return True

    def validate(self, ctx):
        if self.user is None:
            return False
        ctx.redpanda.logger.info(f"Validating user {self.user} is present")
        users = ctx.admin().list_users()
        return self.user in users

    def describe(self):
        return {
            "type": "create_user",
            "properties": {
                "name": self.user,
                "password": self.password
            }
        }


class DeleteUserOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.user = None

    def execute(self, ctx):
        self.user = _choice_random_user(ctx, prefix=self.prefix)
        if self.user is None:
            return False
        ctx.redpanda.logger.info(f"Deleting user: {self.user}")
        ctx.admin().delete_user(self.user)
        return True

    def validate(self, ctx):
        if self.user is None:
            return False
        ctx.redpanda.logger.info(f"Validating user {self.user} is deleted")
        users = ctx.admin().list_users()
        return self.user not in users

    def describe(self):
        return {
            "type": "delete_user",
            "properties": {
                "name": self.user,
            }
        }


class CreateAclOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.user = None

    def execute(self, ctx):
        if self.user is None:
            self.user = _choice_random_user(ctx, prefix=self.prefix)
        if self.user is None:
            return False

        ctx.redpanda.logger.info(
            f"Creating allow cluster describe ACL for user: {self.user}")
        ctx.rpk().acl_create_allow_cluster(self.user, op="describe")

        return True

    def validate(self, ctx):
        if self.user is None:
            return False
        ctx.redpanda.logger.info(f"Validating user {self.user} ACL is present")
        acls = ctx.rpk().acl_list()
        lines = acls.splitlines()
        for l in lines:
            if self.user in l and "ALLOW" in l:
                return True
        return False

    def describe(self):
        return {
            "type": "create_acl",
            "properties": {
                "principal": self.user,
            }
        }


class UpdateConfigOperation(Operation):
    # property values generator
    properties = {
        "group_max_session_timeout_ms":
        lambda: random.randint(300000, 600000),
        "metadata_dissemination_retry_delay_ms":
        lambda: random.randint(2000, 10000),
        "log_message_timestamp_type":
        lambda: random.choice(['CreateTime', 'LogAppendTime']),
        "alter_topic_cfg_timeout_ms":
        lambda: random.randint(2000, 10000),
    }

    def __init__(self):
        p = random.choice(list(UpdateConfigOperation.properties.items()))
        self.property = p[0]
        self.value = p[1]()

    def execute(self, ctx):
        ctx.redpanda.logger.info(
            f"Updating {self.property} value with {self.value}")
        ctx.rpk().cluster_config_set(self.property, str(self.value))
        return True

    def validate(self, ctx):
        ctx.redpanda.logger.info(
            f"Validating cluster configuration is set {self.property}=={self.value}"
        )
        return ctx.rpk().cluster_config_get(self.property) == str(self.value)

    def describe(self):
        return {
            "type": "update_config",
            "properties": {
                "key": self.property,
                "value": self.value
            }
        }


class ProduceToTopic(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.topic = None
        self.msg_cnt = random.randint(50, 500)
        self.delivery_offsets = {}
        self.produce_err = None

    def execute(self, ctx):
        if self.topic is None:
            self.topic = _choice_random_topic(ctx, prefix=self.prefix)

        if self.topic is None:
            return False

        producer = Producer({'bootstrap.servers': ctx.redpanda.brokers()})
        for i in range(self.msg_cnt):
            producer.produce(self.topic,
                             f"admin-ops-fuzzer-{self.topic}-{i}",
                             callback=lambda err, metadata: self.
                             _records_delivered(err, metadata))
        producer.flush()

        return True

    def _records_delivered(self, err, metadata):
        self.produce_err = err
        self.delivery_offsets[metadata.partition()] = metadata.offset()

    def validate(self, ctx):
        if self.produce_err:
            ctx.redpanda.logger.info(
                f"Produce operation returned an error: {self.produce_err}")
            return True

        partitions = ctx.rpk().describe_topic(self.topic)
        for p in partitions:
            if p.id in self.delivery_offsets:
                batch_offset = self.delivery_offsets[p.id]
                ctx.redpanda.logger.debug(
                    f"Last produced record offset: {self.delivery_offsets[p.id]}, topic high watermark: {p.high_watermark}"
                )
                if batch_offset > p.high_watermark:
                    return False

        return True

    def describe(self):
        return {
            "type": "produce_to_topic",
            "properties": {
                "topic": self.topic,
                "msg_cnt": self.msg_cnt
            }
        }


class DeleteRecords(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.truncate_point = ()

    def _random_truncate_point(self, ctx):
        """
        Returns a mapping of topic to partition offset:
        ( "foo", { 0 , 150 ) ) , will truncate foo partition 0 at offset 150
        """
        def deletable_topic_partition(ctx):
            def is_partition_empty(p):
                return int(p.high_watermark) == 0 or (int(p.start_offset)
                                                      == int(p.high_watermark))

            def is_deletable(ctx, t):
                cfgs = ctx.rpk().describe_topic_configs(topic=t)
                (values, _) = cfgs['cleanup.policy']
                values = values.split(',')
                return 'delete' in values

            deletable_topics = [
                t for t in ctx.rpk().list_topics() if is_deletable(ctx, t)
            ]
            if len(deletable_topics) == 0:
                return None

            deletable_topic = _random_choice(self.prefix, deletable_topics)

            if deletable_topic is None:
                return None

            deletable_partitions = [
                p for p in ctx.rpk().describe_topic(deletable_topic)
                if not is_partition_empty(p)
            ]

            return None if len(deletable_partitions) == 0 else (
                deletable_topic, random.choice(deletable_partitions))

        topic_partition = deletable_topic_partition(ctx)
        if topic_partition is None:
            return ()

        def random_truncate_offset(p):
            return random.randint(
                int(p.start_offset) + 1, int(p.high_watermark))

        (topic, partition) = topic_partition
        return (topic, (int(partition.id), random_truncate_offset(partition)))

    def execute(self, ctx):
        self.truncate_point = self._random_truncate_point(ctx)
        if len(self.truncate_point) == 0:
            return False

        ctx.redpanda.logger.info(
            f"Issuing DeleteRecords command: {self.truncate_point}")
        (topic, partition_offset) = self.truncate_point
        (partition, offset) = partition_offset
        ctx.rpk().trim_prefix(topic, offset, partitions=[partition])
        return True

    def validate(self, ctx):
        if len(self.truncate_point) == 0:
            return False

        ctx.redpanda.logger.info(
            f"Validing topic low watermarks, expecting: {self.truncate_point}")

        (topic, partition_offset) = self.truncate_point
        (partition_id, lwm) = partition_offset

        partitions_info = ctx.rpk().describe_topic(topic)
        partition_info = [
            p for p in partitions_info if int(p.id) == partition_id
        ]
        assert len(partition_info
                   ) == 1, f"Expected {topic} with partition {partition_id}"
        return int(partition_info[0].start_offset) == lwm

    def describe(self):
        return {
            "type": "delete_records",
            "properties": {
                "truncate_point": self.truncate_point
            }
        }


class AdminOperationsFuzzer():
    def __init__(self,
                 redpanda,
                 initial_entities=10,
                 retries=5,
                 retries_interval=5,
                 operation_timeout=30,
                 operations_interval=1,
                 max_partitions=10,
                 min_replication=1,
                 max_replication=3,
                 allowed_operations=None):
        self.redpanda = redpanda
        self.operation_ctx = OperationCtx(self.redpanda)
        self.initial_entities = initial_entities
        self.retries = retries
        self.retries_interval = retries_interval
        self.operation_timeout = operation_timeout
        self.operations_interval = operations_interval
        self.max_partitions = max_partitions
        self.min_replication = min_replication
        self.max_replication = max_replication
        if allowed_operations is None:
            self.allowed_operations = [o for o in RedpandaAdminOperation]
        else:
            self.allowed_operations = allowed_operations

        self.prefix = f'fuzzy-operator-{random.randint(0,10000)}'
        self._stopping = Event()
        self.executed = 0
        self.attempted = 0
        self.history = []
        self.error = None

        self._pause_cond = Condition()
        self._pause_requested = False
        self._pause_reached = False

    def start(self):
        self.create_initial_entities()

        self.thread = threading.Thread(target=lambda: self.thread_loop(),
                                       args=())
        self.thread.daemon = True
        self.thread.start()

    def create_initial_entities(self):
        # pre-populate cluster with users and topics
        for i in range(0, self.initial_entities):
            tp = CreateTopicOperation(self.prefix, 1, self.min_replication,
                                      self.max_replication)
            self.append_to_history(tp)
            tp.execute(self.operation_ctx)

            user = CreateUserOperation(self.prefix)
            self.append_to_history(user)
            user.execute(self.operation_ctx)

    def thread_loop(self):
        while not self._stopping.is_set():
            with self._pause_cond:
                if self._pause_requested:
                    self._pause_reached = True
                    self._pause_cond.notify()

                while self._pause_requested:
                    self._pause_cond.wait()

                self._pause_reached = False

            try:
                self.execute_one()
            except Exception as e:
                self.error = e
                self._stopping.set()

        with self._pause_cond:
            self._pause_reached = True
            self._pause_cond.notify()

    def pause(self):
        with self._pause_cond:
            self.redpanda.logger.info("pausing admin ops fuzzer...")
            assert self._pause_requested == False
            self._pause_requested = True
            while not self._pause_reached:
                self._pause_cond.wait()
            self.redpanda.logger.info("paused admin ops fuzzer")

    def unpause(self):
        with self._pause_cond:
            self._pause_requested = False
            self._pause_cond.notify()
            self.redpanda.logger.info("unpaused admin ops fuzzer")

    def append_to_history(self, op):
        d = op.describe()
        d['timestamp'] = int(time.time())
        self.history.append(d)

    def execute_one(self):
        op_type, op = self.make_random_operation()

        def validate_result():
            try:
                return op.validate(self.operation_ctx)
            except Exception as e:
                self.redpanda.logger.debug(
                    f"Error validating operation {op_type}", exc_info=True)
                return False

        try:
            self.attempted += 1
            if self.execute_with_retries(op_type, op):
                wait_until(
                    validate_result,
                    timeout_sec=self.operation_timeout,
                    backoff_sec=1,
                    err_msg=
                    f"Timeout waiting for {op.describe()} operation validation"
                )
                self.executed += 1
                sleep(self.operations_interval)
                return True
            else:
                self.redpanda.logger.info(
                    f"Skipped operation: {op_type}, current cluster state does not allow executing the operation"
                )
                return False
        except Exception as e:
            self.redpanda.logger.error(f"Operation: {op.describe()} failed",
                                       exc_info=True)
            raise e
        finally:
            self.append_to_history(op)

    def execute_with_retries(self, op_type, op):
        self.redpanda.logger.info(
            f"Executing operation: {op_type} with {self.retries} retries")
        if self.retries == 0:
            return op.execute(self.operation_ctx)
        error = None
        for retry in range(0, self.retries):
            try:
                if retry > 0:
                    # it might happened that operation was already successful
                    if op.validate(self.operation_ctx):
                        return True
                return op.execute(self.operation_ctx)
            except Exception as e:
                error = e
                self.redpanda.logger.info(
                    f"Operation: {op_type}, retries left: {self.retries-retry}/{self.retries}",
                    exc_info=True)
                sleep(self.retries_interval)
        assert error  # will always be set but type checker can't figure it out
        raise error

    def make_random_operation(self):
        op = random.choice(self.allowed_operations)
        actions = {
            RedpandaAdminOperation.CREATE_TOPIC:
            lambda:
            CreateTopicOperation(self.prefix, self.max_partitions, self.
                                 min_replication, self.max_replication),
            RedpandaAdminOperation.DELETE_TOPIC:
            lambda: DeleteTopicOperation(self.prefix),
            RedpandaAdminOperation.UPDATE_TOPIC:
            lambda: UpdateTopicOperation(self.prefix),
            RedpandaAdminOperation.ADD_PARTITIONS:
            lambda: AddPartitionsOperation(self.prefix),
            RedpandaAdminOperation.CREATE_USER:
            lambda: CreateUserOperation(self.prefix),
            RedpandaAdminOperation.DELETE_USER:
            lambda: DeleteUserOperation(self.prefix),
            RedpandaAdminOperation.CREATE_ACLS:
            lambda: CreateAclOperation(self.prefix),
            RedpandaAdminOperation.UPDATE_CONFIG:
            lambda: UpdateConfigOperation(),
            RedpandaAdminOperation.PRODUCE_TO_TOPIC:
            lambda: ProduceToTopic(self.prefix),
            RedpandaAdminOperation.DELETE_RECORDS:
            lambda: DeleteRecords(self.prefix),
        }
        return (op, actions[op]())

    def stop(self):
        if self._stopping.is_set():
            return

        self.redpanda.logger.info(
            f"operations history: {json.dumps(self.history)}")
        self._stopping.set()
        self.thread.join()

        assert self.error is None, f"Encountered an error in admin operations fuzzer: {self.error}"

    def ensure_progress(self):
        executed = self.executed
        attempted = self.attempted

        def check():
            # Drop out immediately if the main loop errored out.
            if self.error:
                self.redpanda.logger.error(
                    f"wait: terminating for error {self.error}")
                raise self.error

            # the attempted condition gurantees that we measure progress
            # by use an operation which started after ensure_progress is
            # invoked
            if self.executed > executed and self.attempted > attempted:
                return True
            elif self._stopping.is_set():
                # We cannot ever reach the count, error out
                self.redpanda.logger.error(f"wait: terminating for stop")
                raise RuntimeError(f"Stopped without observing progress")
            return False

        # we use 2*self.operation_timeout to give time (self.operation_timeout) for
        # the operation started before ensure_progress is invoked to finish prior to
        # measuring the real indicator (next self.operation_timeout)
        wait_until(check,
                   timeout_sec=2 * self.operation_timeout,
                   backoff_sec=2)

    def wait(self, count, timeout):
        def check():
            # Drop out immediately if the main loop errored out.
            if self.error:
                self.redpanda.logger.error(
                    f"wait: terminating for error {self.error}")
                raise self.error

            if self.executed >= count:
                return True
            elif self._stopping.is_set():
                # We cannot ever reach the count, error out
                self.redpanda.logger.error(f"wait: terminating for stop")
                raise RuntimeError(
                    f"Stopped without reaching target ({self.executed}/{count})"
                )
            return False

        wait_until(check, timeout_sec=timeout, backoff_sec=2)
