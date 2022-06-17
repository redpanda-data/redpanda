# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from enum import Enum, auto, unique
import random
import re
import string
from threading import Event
import threading
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from time import sleep

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin


# Base class for operation
class Operation():
    def execute(self, redpanda) -> bool:
        return False

    def validate(self, redpanda) -> bool:
        pass


def random_string(length):
    return ''.join(
        [random.choice(string.ascii_letters) for i in range(0, length)])


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


def _random_choice(prefix, collection):
    filtered = list(filter(lambda t: t.startswith(prefix), collection))
    if len(filtered) == 0:
        return None
    return random.choice(filtered)


def _choice_random_topic(redpanda, prefix):
    return _random_choice(prefix, RpkTool(redpanda).list_topics())


def _choice_random_user(redpanda, prefix):
    return _random_choice(prefix, Admin(redpanda).list_users())


# topic operations
class CreateTopicOperation(Operation):
    def __init__(self, prefix, max_partitions, max_replication):
        self.prefix = prefix
        self.topic = f'{prefix}-{random_string(6)}'
        self.partitions = random.randint(1, max_partitions)
        self.rf = random.choice([x for x in range(1, max_replication + 1, 2)])

    def execute(self, redpanda):
        redpanda.logger.info(
            f"Creating topic with name {self.topic}, replication: {self.rf} partitions: {self.partitions}"
        )
        rpk = RpkTool(redpanda)
        rpk.create_topic(self.topic, self.partitions, self.rf)
        return True

    def validate(self, redpanda):
        if self.topic is None:
            return False
        redpanda.logger.info(f"Validating topic {self.topic} creation")
        rpk = RpkTool(redpanda)
        topics = rpk.list_topics()
        return self.topic in topics


class DeleteTopicOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.topic = None

    def execute(self, redpanda):
        rpk = RpkTool(redpanda)

        self.topic = _choice_random_topic(redpanda, prefix=self.prefix)
        if self.topic is None:
            return False
        redpanda.logger.info(f"Deleting topic: {self.topic}")
        rpk.delete_topic(self.topic)
        return True

    def validate(self, redpanda):
        if self.topic is None:
            return False
        redpanda.logger.info(f"Validating topic {self.topic} deletion")
        rpk = RpkTool(redpanda)
        topics = rpk.list_topics()
        return self.topic not in topics


class UpdateTopicOperation(Operation):

    properties = {
        TopicSpec.PROPERTY_CLEANUP_POLICY:
        lambda: random.choice(['delete', 'compact']),
        TopicSpec.PROPERTY_TIMESTAMP_TYPE:
        lambda: random.choice(['CreateTime', 'LogAppendTime']),
        TopicSpec.PROPERTY_SEGMENT_SIZE:
        lambda: random.randint(10 * 2 ^ 20, 512 * 2 ^ 20),
        TopicSpec.PROPERTY_RETENTION_BYTES:
        lambda: random.randint(10 * 2 ^ 20, 512 * 2 ^ 20),
        TopicSpec.PROPERTY_RETENTION_TIME:
        lambda: random.randint(10000, 1000000)
    }

    def __init__(self, prefix):
        self.prefix = prefix
        self.topic = None
        self.property = None
        self.value = None

    def execute(self, redpanda):
        rpk = RpkTool(redpanda)

        self.topic = _choice_random_topic(redpanda, prefix=self.prefix)

        if self.topic is None:
            return False

        self.property = random.choice(
            list(UpdateTopicOperation.properties.keys()))
        self.value = UpdateTopicOperation.properties[self.property]()

        redpanda.logger.info(
            f"Updating topic: {self.topic} with: {self.property}={self.value}")
        rpk.alter_topic_config(self.topic, self.property, str(self.value))
        return True

    def validate(self, redpanda):
        if self.topic is None:
            return False
        redpanda.logger.info(
            f"Validating topic {self.topic} update, expected: {self.property}={self.value}"
        )
        rpk = RpkTool(redpanda)

        desc = rpk.describe_topic_configs(self.topic)
        return desc[self.property][0] == str(self.value)


class AddPartitionsOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.topic = None
        self.total = None

    def execute(self, redpanda):
        rpk = RpkTool(redpanda)

        self.topic = _choice_random_topic(redpanda, prefix=self.prefix)
        if self.topic is None:
            return False
        current = len(list(rpk.describe_topic(self.topic)))
        to_add = random.randint(1, 5)
        self.total = current + to_add
        redpanda.logger.info(
            f"Updating topic: {self.topic} partitions count, current: {current} adding: {to_add} partitions"
        )
        rpk.add_topic_partitions(self.topic, to_add)
        return True

    def validate(self, redpanda):
        if self.topic is None:
            return False
        rpk = RpkTool(redpanda)
        redpanda.logger.info(
            f"Validating topic {self.topic} partitions update")
        current = len(list(rpk.describe_topic(self.topic)))
        return current == self.total


class CreateUserOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.user = f'{prefix}-user-{random_string(6)}'
        self.password = f'{prefix}-user-{random_string(6)}'
        self.algorithm = "SCRAM-SHA-256"

    def execute(self, redpanda):
        admin = Admin(redpanda)
        redpanda.logger.info(f"Creating user: {self.user}")
        admin.create_user(self.user, self.password, self.algorithm)
        return True

    def validate(self, redpanda):
        if self.user is None:
            return False
        admin = Admin(redpanda)
        redpanda.logger.info(f"Validating user {self.user} is present")
        users = admin.list_users()
        return self.user in users


class DeleteUserOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.user = None

    def execute(self, redpanda):
        admin = Admin(redpanda)
        self.user = _choice_random_user(redpanda, prefix=self.prefix)
        if self.user is None:
            return False
        redpanda.logger.info(f"Deleting user: {self.user}")
        admin.delete_user(self.user)
        return True

    def validate(self, redpanda):
        if self.user is None:
            return False
        admin = Admin(redpanda)
        redpanda.logger.info(f"Validating user {self.user} is deleted")
        users = admin.list_users()
        return self.user not in users


class CreateAclOperation(Operation):
    def __init__(self, prefix):
        self.prefix = prefix
        self.user = None

    def execute(self, redpanda):
        self.user = _choice_random_user(redpanda, prefix=self.prefix)
        if self.user is None:
            return False

        rpk = RpkTool(redpanda)

        redpanda.logger.info(
            f"Creating allow cluster describe ACL for user: {self.user}")
        rpk.acl_create_allow_cluster(self.user, op="describe")

        return True

    def validate(self, redpanda):
        if self.user is None:
            return False
        redpanda.logger.info(f"Validating user {self.user} ACL is present")
        rpk = RpkTool(redpanda)
        acls = rpk.acl_list()
        lines = acls.splitlines()
        for l in lines:
            if self.user in l and "ALLOW" in l:
                return True
        return False


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

    def execute(self, redpanda):
        rpk = RpkTool(redpanda)
        redpanda.logger.info(
            f"Updating {self.property} value with {self.value}")
        rpk.cluster_config_set(self.property, str(self.value))
        return True

    def validate(self, redpanda):
        rpk = RpkTool(redpanda)
        redpanda.logger.info(
            f"Validating cluster configuration is set {self.property}=={self.value}"
        )
        return rpk.cluster_config_get(self.property) == str(self.value)


class AdminOperationsFuzzer():
    def __init__(self,
                 redpanda,
                 initial_entities=10,
                 retries=5,
                 retries_interval=5,
                 operation_timeout=30,
                 operations_interval=1,
                 max_partitions=10,
                 max_replication=3):
        self.redpanda = redpanda
        self.initial_entities = initial_entities
        self.retries = retries
        self.retries_interval = retries_interval
        self.operation_timeout = operation_timeout
        self.operations_interval = operations_interval
        self.max_partitions = max_partitions
        self.max_replication = max_replication

        self.prefix = f'fuzzy-operator-{random.randint(0,10000)}'
        self._stopping = Event()
        self.executed = 0

        self.error = None

    def start(self):
        self.thread = threading.Thread(target=lambda: self.thread_loop(),
                                       args=())
        # pre-populate cluster with users and topics
        for i in range(0, self.initial_entities):
            CreateTopicOperation(self.prefix, 1,
                                 self.max_replication).execute(self.redpanda)
            CreateUserOperation(self.prefix).execute(self.redpanda)
        self.thread.start()

    def thread_loop(self):
        while not self._stopping.is_set():
            op_type, op = self.make_random_operation()

            def validate_result():
                try:
                    op.validate(self.redpanda)
                except Exception as e:
                    self.redpanda.logger.debug(
                        f"Error validating operation {op_type} - {e}")
                    return False

            try:
                if self.execute_with_retries(op_type, op):
                    wait_until(lambda: validate_result,
                               timeout_sec=self.operation_timeout,
                               backoff_sec=1)
                    self.executed += 1
                    sleep(self.operations_interval)
                else:
                    self.redpanda.logger.info(
                        f"Skipped operation: {op_type}, current cluster state does not allow executing the operation"
                    )
            except Exception as e:
                self.redpanda.logger.error(f"Operation: {op_type} error: {e}")
                self.error = e
                self._stopping.set()

    def execute_with_retries(self, op_type, op):
        self.redpanda.logger.info(
            f"Executing operation: {op_type} with {self.retries} retries")
        if self.retries == 0:
            return op.execute(self.redpanda)
        error = None
        for retry in range(0, self.retries):
            try:
                if retry > 0:
                    # it might happened that operation was already successful
                    if op.validate(self.redpanda):
                        return True
                return op.execute(self.redpanda)
            except Exception as e:
                error = e
                self.redpanda.logger.info(
                    f"Operation: {op_type} error: {error}, retires left: {self.retries-retry}/{self.retries}"
                )
                sleep(1)
        raise error

    def make_random_operation(self) -> Operation:
        op = random.choice([o for o in RedpandaAdminOperation])
        actions = {
            RedpandaAdminOperation.CREATE_TOPIC:
            lambda: CreateTopicOperation(self.prefix, self.max_partitions, self
                                         .max_replication),
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
        }
        return (op, actions[op]())

    def stop(self):
        self._stopping.set()
        self.thread.join()

        assert self.error is None, f"Encountered an error in admin operations fuzzer: {self.error}"

    def wait(self, count, timeout):
        wait_until(lambda: self.executed > count, timeout, 2)
