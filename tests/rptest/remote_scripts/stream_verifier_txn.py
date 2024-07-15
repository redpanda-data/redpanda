# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import argparse
import confluent_kafka as ck
import falcon
import json
import logging
import numpy as np
import signal
import string
import sys
import threading

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from time import sleep
from typing import Generator, IO, Tuple, Callable, Any, List
from wsgiref.simple_server import make_server

global title
app_name = "StreamVerifierTx"
title = f"{app_name}, python transaction verifier"
log_level = logging.INFO
LOGGER_STARTUP = 'startup'
LOGGER_MAIN = 'main'
LOGGER_CORE = 'core'
LOGGER_CLI_COMMAND = 'cli_command'
LOGGER_WEB_PRODUCE = "web_produce"
LOGGER_WEB_CONSUME = "web_consume"

CONSUMER_LOGGING_THRESHOLD = 100

CONSUME_STOP_EOF = "eof"
CONSUME_STOP_SLEEP = "sleep"
CONSUME_STOP_CONTINUOUS = "continuous"
consume_stop_options = [
    CONSUME_STOP_EOF, CONSUME_STOP_SLEEP, CONSUME_STOP_CONTINUOUS
]


# Class to serialize int64
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def setup_logger(child_logger_name: str = "",
                 level: int = logging.DEBUG) -> logging.Logger:
    """Create main logger or its child based on naming

    Args:
        name (str, optional): name of the logger. Defaults to "stream_verifier"
        level (int, optional): Log level. Defaults to logging.DEBUG.

    Returns:
        logging.Logger: Logger created
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setLevel(level)
    logger_name = "stream_verifier"
    if len(child_logger_name) > 0:
        # Just simple hierachy, no special handler
        return logging.getLogger(f"{logger_name}.{child_logger_name}")
    else:
        logger = logging.getLogger(logger_name)
        logger.addHandler(handler)
        logger.setLevel(level)
        return logger


def write_json(ioclass: IO, data: dict) -> None:
    ioclass.write(json.dumps(data))
    ioclass.write('\n')
    ioclass.flush()


def validate_keys(incoming: list[str], local: list[str],
                  forbidden: list[str]) -> list[str]:
    error_msgs = []
    # Validate incoming keys
    for k in incoming:
        if k in forbidden:
            error_msgs += [f"Key '{k}' can't be updated"]
        elif k not in local:
            error_msgs += [f"Unknown key '{k}'"]
    return error_msgs


def validate_option(option: str, value: str,
                    available_values: list[str]) -> Tuple[bool, str]:
    if value not in available_values:
        error_msg = f"{option} value of {value} is invalid. " \
            f"Expected on of: {', '.join(available_values)}"
        return False, error_msg
    else:
        return True, ""


class Updateable(object):
    def update(self, new: dict):
        for key, value in new.items():
            if hasattr(self, key):
                setattr(self, key, value)


@dataclass(kw_only=True)
class AppCfg(Updateable):
    """Holds configuration for the app along with
    functions to handle REST calls to update it
    """
    app_name: str = app_name
    brokers: str = "localhost:9092"
    # topic group id for Consumer configuration
    topic_group_id: str = "group-stream-verifier-tx"
    # Topics
    topic_produce: str = "stream-topic-dst"
    topic_consume: str = "stream-topic-src"
    # 0 - no rate limiting
    msg_rate_limit: int = 0
    msg_per_txn: int = 1
    msg_total: int = 256
    # Amount of errors in topic before removing it from processing
    topic_error_threshold: int = 1
    # When consume will stop processing
    # eof - on discovering partition EOF
    # sleep - when there is no new messages after sleep time
    # continuous - exit only on terminate signal
    consume_stop_criteria: str = "sleep"
    # how much time to wait in a loop for next message, sec
    consume_timeout_s: int = 60
    # Timeout for poll operation
    consume_poll_timeout: int = 5
    # if no messages received after 2 min
    # Just exit consume operation
    consume_sleep_time_s: int = 60
    # how often to log sent message event
    # On higher scale it could be >10000 to eliminate log IO overhead
    consumer_logging_threshold: int = 1000
    # web server port
    web_port: int = 8090

    @property
    def msgs_rate_ms(self):
        # It is reasonable to assume that single message will not be sent
        # faster than 1 ms in case of no rate limitations
        return 1000 / self.msg_rate_limit if self.msg_rate_limit > 0 else 0

    @property
    def forbidden_keys(self) -> list[str]:
        # non-updatable vars via REST handle
        return ['web_port', 'app_name']

    def on_get(self, req, resp):
        """Handles 'GET:<host:port>/' calls. Returns current app config

        Args:
            req (falcon.Request): request coming in
            resp (falcon.Response): response to be filled

        Returns JSON on GET, same one can be used to update via POST:
        {
            "brokers": "'localhost:19092'",
            "topic_group_id": "group-stream-verifier-tx",
            "topic_produce": "stream-topic-src",
            "topic_consume": "stream-topic-dst",
            "msg_rate_limit": 0,
            "msg_per_txn": 1,
            "msg_total": 256,
            "consume_timeout_s": 60,
            "consumer_logging_threshold": 1000,
        }
        """
        resp.status = falcon.HTTP_200
        resp.content_type = falcon.MEDIA_JSON
        # need to copy here otherwise, Class gets broken
        _conf = deepcopy(vars(self))
        # remove forbidden keys
        for k in self.forbidden_keys:
            _conf.pop(k)
        # dump it without prettify
        resp.media = json.dumps(_conf)

    def on_post(self, req, resp):
        """Handles 'POST:<host:port>/' calls. Updates app config

        Args:
            req (falcon.Request): request coming in
            resp (falcon.Response): response to be filled
        """
        # Get main logger from manager class
        # If it exists, it will not be recreated
        logger = setup_logger(LOGGER_MAIN)
        logger.info(f"POST: Updating app configuration: {req.media}")
        # App conf keys
        keys_list = list(vars(self).keys())
        # incoming keys
        keys_req = list(req.media.keys())
        # Validate options
        error_msgs = validate_keys(keys_req, keys_list, self.forbidden_keys)

        # Validate specific option
        if "consume_stop_criteria" in keys_req:
            valid, error = validate_option("consume_stop_criteria",
                                           req.media["consume_stop_criteria"],
                                           consume_stop_options)
            if not valid:
                error_msgs += [error]

        if error_msgs:
            # Just dump errors as list
            msg = f"Incoming request invalid: {', '.join(error_msgs)}"
            logger.error(msg)
            resp.status = falcon.HTTP_400
            resp.media_type = falcon.MEDIA_JSON
            resp.media = {"errors": msg}
        else:
            # since validation is passed, no more strict checking needed
            self.update(req.media)
            logger.debug(f"...updated app config: {vars(self)}")
            resp.status = falcon.HTTP_200
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {'result': "OK"}


# singleton for app_config
app_config = AppCfg()


@dataclass(kw_only=True)
class TopicStatus(Updateable):
    """Holds single live topic status

    'TopicStatus' holds data for live topic in action
    and is recreated for each initialized produce/consume
    action. It is chosen not to divide them into
    'TopicProduceStatus' and 'TopicConsumeStatus' as
    parameters are almost the same. Also, for slightly
    less memory consumption

    Returns:
        _type_: _description_
    """
    topic_group_id: str
    # topic to consume from
    source_topic_name: str
    # atomic produce target
    target_topic_name: str
    # how many messages to process in single transaction
    msgs_per_transaction: int
    # Total messages to be processed for this topic
    total_messages: int
    # Current topic index or how much messages already produced/consumed
    index: int
    # sent count
    processed_count: int
    # timestamp for last message
    last_message_ts: float
    # how much ms should pass between messages to be sent
    msgs_rate_ms: float
    # how much time to wait when waiting for the next message
    consume_timeout_s: int
    # Precreated Producer class.
    producer: ck.Producer
    # All data needed to create consumer class.
    # As opposed to Producer, Consumer must be created/used in the same thread
    consumer_config: dict
    # EOF flag
    reached_eof: bool
    # Termination signalling flag
    # It is simpler that threading event and faster
    terminate: bool
    # if errors happen during produce/send track them here
    errors: list[str]

    # Function should have followind format:
    # func(start_index, message_count) -> Generator
    message_generator: None | Callable[[int, int], Generator]

    # Function should have followind format:
    # func(src_value) -> bool
    message_validator: None | Callable[[Any], bool]

    # Function should have followind format:
    # func(src_key, src_value) -> Tuple(src_key, src_value)
    message_transform: None | Callable[[str, str], List[str]]

    @property
    def transaction_id(self):
        return f"{self.source_topic_name}-{self.last_message_ts}"

    @property
    def forbidden_keys(self) -> list[str]:
        # non-updatable vars via REST handle
        return [
            'producer', 'message_generator', 'message_validator',
            'message_transform', 'terminate', 'reached_eof'
        ]


# This can hold future checksum checks or similar
class MessageValidators:
    """Class to hold message validation strategies
    """

    previous_number = -1

    # Function is general case can have different messages value type
    # so we should not stick to single one
    def is_numbered_sequence(self, value) -> bool:
        """Functions validates if message has number grater by exactly 1

        Args:
            value (Unknown): message value

        Returns:
            bool: Whether given value bigger by exactly 1
        """
        if not isinstance(value, int):
            # Just fail check if the type is not correct
            return False

        outcome = (value - self.previous_number) == 1
        self.previous_number = value
        return outcome


class MessageTransforms:
    """Class to hold message transforming functions
    """
    @staticmethod
    def dezero_transform(src_key: str, src_value: str) -> List:
        """Removes zeroes from all numbers in value
        Example:
            src value "aaa00023bbb_453z0002"
            returned: "aaa23bbb_453z2"

        Args:
            src_key (str): message key
            src_value (str): message value

        Returns:
            str, str: transformed key and value
        """
        active_number = ""
        new_value = ""
        if isinstance(src_key, bytes):
            src_key = src_key.decode('utf-8')
        if isinstance(src_value, bytes):
            src_value = src_value.decode('utf-8')
        for char in src_value:
            if char in string.digits:
                active_number += char
            else:
                new_int = int(active_number) if len(active_number) > 0 else 0
                if new_int > 0:
                    new_value += str(new_int) + char
                    active_number = ""
                else:
                    new_value += char
        if len(active_number) > 0:
            new_int = int(active_number)
            new_value += str(new_int)
        return [src_key, new_value]


class MessageGenerators:
    """Various Generator functions to use in Producer
    """
    @staticmethod
    def gen_indexed_messages(start_index: int,
                             message_count: int) -> Generator:
        """Generates indexed messages:
        'key_0'/'0000', 'key_1'/'0001', ...

        Args:
            start_index (int): message index to start from
            message_count (int): number of messages to generate

        Yields:
            Generator: message_key: int, message_value: int
        """
        for idx in range(start_index, start_index + message_count):
            key = f"key_{idx}"
            value = f"{idx:04}"
            yield key, value


class StreamVerifier():
    """Main class to process messages
    """
    def __init__(self, brokers):
        # Create core logger
        self.logger = setup_logger(LOGGER_CORE)
        # Remove quotes from broker config value if an
        self.brokers = brokers.strip('\"').strip("'")
        # Create initial universal topics config
        _live_cfg = {
            # For produce only mode just set them to the same value
            "topic_group_id": app_config.topic_group_id,
            "source_topic_name": app_config.topic_consume,
            "target_topic_name": app_config.topic_produce,
            "msgs_per_transaction": app_config.msg_per_txn,
            "total_messages": app_config.msg_total,
            "index": 0,
            "processed_count": 0,
            "last_message_ts": datetime.now().timestamp(),
            "msgs_rate_ms": app_config.msgs_rate_ms,
            "consume_timeout_s": app_config.consume_timeout_s,
            "producer": None,
            "consumer_config": {},
            "reached_eof": False,
            "terminate": False,
            "errors": [],
            # topic has its own instance to message generator
            # so it is instance of a class in-place
            "message_generator": None,
            "message_validator": None,
            "message_transform": None,
        }
        self.live_topic = TopicStatus(**_live_cfg)
        # Announcement of dynamic vars
        self.produce_thread = None
        self.consume_thread = None
        self.atomic_thread = None

    @staticmethod
    def _worker_thread(func, topic: TopicStatus, consume_stop: str,
                       consume_sleep: int, logger: logging.Logger):
        msgs_processed = 0
        last_message_count = 0
        # Message processing loop
        while msgs_processed < topic.total_messages or \
                topic.total_messages < 0:
            topic_status = func(logger, topic)
            msgs_processed += topic_status.processed_count
            topic_status.processed_count = 0
            # Check for errors and stop processing if any
            if len(topic.errors) >= app_config.topic_error_threshold:
                logger.warning(f"Topic {topic.source_topic_name} has errors")
                break

            # Check EOF flag and break out if all set
            if topic.reached_eof:
                logger.info('EOF reached')
                # EOF checks will work only for Consume enabled actions
                # Produce actions will exit on reaching total_messages
                if consume_stop == CONSUME_STOP_EOF:
                    # Just exit
                    logger.info("Stopping consumption")
                    break
                elif consume_stop == CONSUME_STOP_SLEEP:
                    # Check if consumed totals had changed
                    if last_message_count < msgs_processed:
                        last_message_count = msgs_processed
                        logger.info(f"Sleeping for {consume_sleep}s")
                        sleep(consume_sleep)
                    else:
                        logger.info(f"No new messages after {consume_sleep}s, "
                                    "exiting")
                        break
                elif consume_stop == CONSUME_STOP_CONTINUOUS:
                    # Nothing to do, just go to another iteration
                    pass

            # Log pretty name of underlying func
            logger.info(f"{func.__qualname__}, "
                        f"processed so far {msgs_processed}")

            # Check termination flag in all topics before the next chunk
            if topic.terminate:
                logger.warning("Got terminate signal, "
                               "exiting from message processing")
                break
        logger.info("End of processing messages.")

    def create_thread(self, func, thread_name="stream_thread"):
        thread = threading.Thread(
            name=thread_name,
            target=self._worker_thread,
            args=(func, self.live_topic, app_config.consume_stop_criteria,
                  app_config.consume_sleep_time_s, self.logger))
        thread.start()
        return thread

    def init_producers(self):
        """Precreates topic status lists and initializes Producers.
        One producer per topic.
        """
        self.logger.info("Initializing producer")
        # Update values for the next run
        self.live_topic.index = 0
        self.live_topic.processed_count = 0
        self.live_topic.last_message_ts = datetime.now().timestamp()
        self.live_topic.terminate = False
        self.live_topic.errors = []
        self.live_topic.message_generator = \
            MessageGenerators().gen_indexed_messages
        self.live_topic.producer = ck.Producer(
            {'bootstrap.servers': self.brokers})

    #
    # Produce functions
    #
    def produce(self, wait=True):
        """Starts produce messages thread

        Args:
            wait (bool, optional): Wait for produce thread to finish or not.
            Defaults to True.
        """
        self.logger.info("Start of sending messages")
        self.produce_thread = self.create_thread(
            self._async_send_messages, thread_name="stream_produce_thread")
        if wait:
            self.produce_thread.join()

    @staticmethod
    def _async_send_messages(logger: logging.Logger,
                             topic: TopicStatus) -> TopicStatus:
        # Thread safe function to send messages as fast as possible
        def ensure_message_rate(rate_ms: float, last_message_ts: float,
                                logger: logging.Logger):
            def time_since_last_msg() -> int:
                diff_ms = datetime.now().timestamp() - last_message_ts
                return int(diff_ms * 1000)

            # Handle message rate
            if rate_ms > 0:
                _time_since = time_since_last_msg()
                if _time_since < rate_ms:
                    wait_time = (rate_ms - _time_since) / 1000
                    logger.debug(
                        f"...waiting {wait_time}s before sending message")
                    sleep(wait_time)

        if topic.message_generator is None:
            topic.errors.append("Message generator is not defined, "
                                "unable to produce")
            return topic

        for key, value in topic.message_generator(topic.index,
                                                  topic.msgs_per_transaction):
            # Handle message rate
            ensure_message_rate(topic.msgs_rate_ms, topic.last_message_ts,
                                logger)

            # Async message sending
            try:
                topic.producer.produce(topic.target_topic_name,
                                       key=key,
                                       value=value)
                topic.index += 1
                topic.processed_count += 1
            except ck.KafkaException as e:
                logger.warning(e)
                topic.errors.append(e)

            # save time for this message
            topic.last_message_ts = datetime.now().timestamp()

            # exit if terminate flag is set
            if topic.terminate:
                logger.warning("Got terminate signal. Exiting")
                break
        # Make sure all messages being delivered
        topic.producer.flush()
        # Return topic meta
        return topic

    #
    # Consume functions
    #
    def init_consumers(self):
        """Precreates topic status lists for Consuming thread.
        """
        self.logger.info("Initializing consumers")
        # Instanciate validator
        validators = MessageValidators()
        # reset validator
        validators.is_numbered_sequence(-1)
        self.live_topic.total_messages = -1
        # consumed messages so far
        self.live_topic.index = 0
        self.live_topic.processed_count = 0
        # time when last message consumed
        self.live_topic.last_message_ts = datetime.now().timestamp()
        # consumer config
        self.live_topic.consumer_config = {
            'bootstrap.servers': self.brokers,
            'group.id': app_config.topic_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'enable.partition.eof': True,
        }
        # EOF flag
        self.live_topic.reached_eof = False
        # termination flag
        self.live_topic.terminate = False
        self.live_topic.errors = []
        # This is consume topic action,
        # No generation or transform needed
        self.live_topic.message_validator = validators.is_numbered_sequence

    def consume(self, wait=True):
        """Starts consume messages thread

        Args:
            wait (bool, optional): Wait for consume thread to finish or not.
            Defaults to True.
        """
        self.logger.info("Start of consuming messages")
        self.consume_thread = self.create_thread(
            self._consume_from_topic, thread_name="stream_consume_thread")
        if wait:
            self.consume_thread.join()
        return

    @staticmethod
    def _consume_from_topic(logger: logging.Logger,
                            topic: TopicStatus) -> TopicStatus:
        """Consumes all messages from topic

        Args:
            logger (logging.Logger): logger class
            topic (TopicStatus): current live topic status class to work with

        Raises:
            ck.KafkaException: on any error occured while consuming

        Returns:
            int: number of consumed messages
        """
        def time_since_last_msg() -> int:
            diff_ms = datetime.now().timestamp() - topic.last_message_ts
            return int(diff_ms * 1000)

        if topic.reached_eof:
            # Just skip it, nothing consumed
            logger.debug(f"...topic {topic.source_topic_name} already at eof")
            return topic

        # Message consuming loop
        consumer = ck.Consumer(topic.consumer_config)
        try:
            # Recent changes offers use of subscribe
            # regardless of transactions and partitions
            consumer.subscribe([topic.source_topic_name])
            topic.last_message_ts = datetime.now().timestamp()
            while True:
                # calculate elapsed time
                _since_last_msg_ms = time_since_last_msg()

                # Exit on timeout
                if _since_last_msg_ms > topic.consume_timeout_s:
                    logger.error("Timeout consuming messages "
                                 f"from {topic.source_topic_name}")
                    break

                # Poll for the message
                msg = consumer.poll(timeout=app_config.consume_poll_timeout)
                if msg is None:
                    # no messages
                    continue
                # On error, check for the EOF
                if msg.error():
                    if msg.error().code() == ck.KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info(f"Consumer of '{msg.topic()}' "
                                    f"[{msg.partition()}] reached "
                                    f"end at offset {msg.offset()}")
                        topic.reached_eof = True
                        break
                    # If not EOF, save the error and exit
                    else:
                        logger.error("Failed to consume message from "
                                     f"'{topic.source_topic_name}': "
                                     f"{msg.error().str()}")
                        topic.errors.append(msg.error().str())
                        break
                else:
                    topic.last_message_ts = datetime.now().timestamp()
                    # Check if there is validation needed
                    if topic.message_validator is not None:
                        try:
                            value = msg.value().decode()
                            int_value = int(value)
                            iscorrect = topic.message_validator(int_value)
                            if not iscorrect:
                                error_message = \
                                    f"Message value of '{value}' failed " \
                                    "validation check of " \
                                    f"'{topic.message_validator.__qualname__}'"
                            else:
                                error_message = ""
                        except Exception:
                            error_message = \
                                f"Invalid message value of '{value}' " \
                                "for selected validator " \
                                f"{topic.message_validator.__qualname__}"
                        finally:
                            # Validation errors does not break message flow
                            # so no loop exit.
                            # But they will affect topic queue later
                            if len(error_message) > 0:
                                logger.error(error_message)
                                topic.errors.append(error_message)

                    # Increment index
                    topic.index += 1
                    # Increment processed count for this iteration
                    topic.processed_count += 1
                    # log only milestones to eliminate IO stress
                    if topic.index % CONSUMER_LOGGING_THRESHOLD == 0:
                        logger.debug(f"...consumed {topic.index} messages "
                                     f"from {topic.source_topic_name}")

                # exit if terminate flag is set
                if topic.terminate:
                    logger.warning("Got terminate signal. Exiting")
                    break
        finally:
            # Close down consumer
            consumer.close()

        logger.info(f"Consumed {topic.index} messages")
        return topic

    #
    # Atomic Produce functions
    #
    def init_atomic_produce(self):
        """Precreates topic list for atomic operation
        """
        self.logger.info("Initializing topic pairs for atomic processing")
        self.live_topic.total_messages = -1
        # consumed messages so far
        self.live_topic.index = 0
        self.live_topic.processed_count = 0
        # time when last message consumed
        self.live_topic.last_message_ts = datetime.now().timestamp()
        # consumer config
        self.live_topic.consumer_config = {
            'bootstrap.servers': self.brokers,
            'group.id': app_config.topic_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'enable.partition.eof': True,
            "isolation.level": "read_committed"
        }
        # EOF flag
        self.live_topic.reached_eof = False
        # termination flag
        self.live_topic.terminate = False
        self.live_topic.errors = []
        # This is consume topic action,
        # No generation or transform needed
        self.live_topic.producer = ck.Producer({
            'bootstrap.servers':
            self.brokers,
            'transactional.id':
            self.live_topic.transaction_id
        })
        self.live_topic.producer.init_transactions()
        return

    def atomic_produce(self, wait=True):
        """Starts atomic consume/produce messages thread

        Args:
            wait (bool, optional): Wait for consume thread to finish or not.
            Defaults to True.
        """
        self.logger.info("Start of atomic consume/produce of messages")
        self.atomic_thread = self.create_thread(
            self._consume_atomic_produce, thread_name="stream_atomic_thread")
        if wait:
            self.atomic_thread.join()
        return

    @staticmethod
    def _consume_atomic_produce(logger: logging.Logger,
                                topic: TopicStatus) -> TopicStatus:
        # Thread safe function to send messages as fast as possible
        def ensure_message_rate(rate_ms: float, last_message_ts: float,
                                logger: logging.Logger):
            def time_since_last_msg() -> int:
                diff_ms = datetime.now().timestamp() - last_message_ts
                return int(diff_ms * 1000)

            # Handle message rate
            if rate_ms > 0:
                _time_since = time_since_last_msg()
                if _time_since < rate_ms:
                    wait_time = (rate_ms - _time_since) / 1000
                    logger.debug(
                        f"...waiting {wait_time}s before sending message")
                    sleep(wait_time)

        def time_since_last_msg_ms() -> int:
            diff_ms = datetime.now().timestamp() - topic.last_message_ts
            return int(diff_ms * 1000)

        consumer = ck.Consumer(topic.consumer_config)
        active_tx = False
        try:
            logger.debug(f"...processing {topic.source_topic_name}")

            def on_assign(consumer, partitions):
                logger.debug(f"[{topic.transaction_id}] Assigned {partitions}")

            def on_revoke(consumer, partitions):
                nonlocal active_tx
                logger.debug(f"[{topic.transaction_id}] Revoked {partitions}")
                if active_tx:
                    logger.info(f"[{topic.transaction_id}] abort transaction")
                    topic.producer.abort_transaction()
                    active_tx = False

            consumer.subscribe([topic.source_topic_name],
                               on_assign=on_assign,
                               on_revoke=on_revoke)

            processed_count = 0
            # Reset message timing
            topic.last_message_ts = datetime.now().timestamp()
            while True:
                # Handle message rate
                ensure_message_rate(topic.msgs_rate_ms, topic.last_message_ts,
                                    logger)

                # calculate elapsed time
                _since_last_msg_ms = time_since_last_msg_ms()
                # Exit on timeout
                if _since_last_msg_ms > topic.consume_timeout_s * 1000:
                    logger.error("Timeout consuming messages "
                                 f"from {topic.source_topic_name}")
                    break

                msg = consumer.poll(timeout=app_config.consume_poll_timeout)
                if msg is not None:
                    err = msg.error()
                    if err is not None:
                        if err.code() == ck.KafkaError._PARTITION_EOF:
                            # End of partition event
                            logger.info(f"Consumer of '{msg.topic()}' "
                                        f"[{msg.partition()}] reached "
                                        f"end at offset {msg.offset()}")
                            topic.reached_eof = True
                            break
                        else:
                            logger.error("Failed to consume messages "
                                         f"from {topic.source_topic_name}")
                            topic.errors.append(err.str())
                            break
                    if not active_tx:
                        # begin transaction
                        topic.producer.begin_transaction()
                        active_tx = True
                    topic.index += 1
                    processed_count += 1
                    # If message transform is defined, use it
                    if topic.message_transform is not None:
                        t_key, t_value = topic.message_transform(
                            msg.key(), msg.value())
                    else:
                        t_key = msg.key().decode()
                        t_value = msg.value().decode()

                    topic.producer.produce(topic.target_topic_name,
                                           value=t_value,
                                           key=t_key,
                                           partition=msg.partition())
                    # Save message timestamp on successfull produce
                    topic.last_message_ts = datetime.now().timestamp()
                    # Commit if reached msgs per tx
                    if topic.index % topic.msgs_per_transaction == 0:
                        logger.debug(
                            f"[{topic.transaction_id}] Committing transaction "
                            f"after {topic.msgs_per_transaction} messages. "
                            "Current consumer positions: "
                            f"{consumer.position(consumer.assignment())}")

                        topic.producer.send_offsets_to_transaction(
                            consumer.position(consumer.assignment()),
                            consumer.consumer_group_metadata())
                        topic.producer.commit_transaction()
                        active_tx = False

                # exit if terminate flag is set
                if topic.terminate:
                    logger.warning("Got terminate signal. Exiting")
                    break

            if active_tx:
                topic.producer.send_offsets_to_transaction(
                    consumer.position(consumer.assignment()),
                    consumer.consumer_group_metadata(), 60)

                topic.producer.commit_transaction()
                active_tx = False
        except ck.error.ConsumeError as ce:
            if ce.code == ck.KafkaError._PARTITION_EOF:
                # End of partition event
                error_message = "Reached EOF"
                if ce.kafka_message is not None:
                    error_message = ce.kafka_message.error().str()
                logger.info(f"[{topic.transaction_id}] {error_message}")
                topic.reached_eof = True
        except ck.KafkaError as e:
            logger.error(f"Client error reported: {e.error} - {e.reason}, "
                         f"retryable: {e.retryable}")
        finally:
            consumer.close()
            topic.producer.flush()
            topic.processed_count += processed_count

        return topic

    def get_high_watermarks(self, topic_name: str) -> dict:
        cfg = {
            'bootstrap.servers': self.brokers,
            'group.id': self.live_topic.topic_group_id,
            'auto.offset.reset': 'latest',
        }
        c = ck.Consumer(cfg)
        all_topics = c.list_topics().topics
        if topic_name in all_topics:
            partitions = [
                ck.TopicPartition(topic_name, p)
                for p in all_topics[topic_name].partitions
            ]
            hwms = {}
            for p in partitions:
                # Get the partitions low and high watermark offsets.
                (lo, hi) = c.get_watermark_offsets(p, timeout=10, cached=False)
                hwms[p.partition] = hi
            return hwms
        else:
            return {}

    def status(self):
        """Provides current processing status

        Args:
            name (str, optional): topic name. Defaults to "".

        Returns:
            dict: Dict with status
        """
        # topics configuration to use in POST command
        t = self.live_topic
        # Create live_config
        topic_cfg = {}
        forbidden = t.forbidden_keys
        for k, v in vars(t).items():
            if k in forbidden:
                continue
            else:
                topic_cfg[k] = v
        topic_cfg["msg_rate_limit"] = app_config.msg_rate_limit
        # Avoid double calling of hwm func
        offsets = {}
        if t.target_topic_name:
            offsets[t.target_topic_name] = self.get_high_watermarks(
                t.target_topic_name)
        if t.source_topic_name and t.source_topic_name != t.target_topic_name:
            offsets[t.source_topic_name] = self.get_high_watermarks(
                t.source_topic_name)

        response = {
            "offsets": offsets,
            "processed_messages": 0 if t is None else t.index,
            "topic_config": topic_cfg,
            "errors": [] if t is None else t.errors
        }
        return response

    def terminate(self):
        """Sets terminate flag for all topics and flushes producers
        """
        operation = "current"
        if self.produce_thread is not None:
            operation = "produce"
        elif self.consume_thread is not None:
            operation = "consume"
        elif self.atomic_thread is not None:
            operation = "atomic consume/produce"

        self.logger.info(f"Terminating {operation} operation")

        if self.live_topic is not None:
            self.live_topic.terminate = True
            if self.live_topic.producer is not None:
                self.live_topic.producer.flush()

    def is_alive(self, thread: threading.Thread | None):
        if thread is not None:
            return thread.is_alive()
        else:
            return False


class StreamVerifierWeb(StreamVerifier):
    """Handles get requests and holds basic validation

    On GET returns current status JSON
    {
        "topics": {},
        "workload_config": {
            "topic_group_id": "group-stream-verifier-tx",
            "topic_produce": "stream-topic-src",
            "topic_consume": "stream-topic-dst",
            "consume_timeout_s": 60,
            "msg_rate_limit": 0,
            "msg_total": 256
        },
        "stats": {
            "total_messages": 0
        },
        "delivery_errors": {}
    }

    On DELETE terminates current produce command
    """
    def __init__(self, cfg):
        self.cfg = cfg
        self.wlogger = setup_logger(LOGGER_WEB_PRODUCE)
        super().__init__(cfg.brokers)

    def on_get(self, req: falcon.Request, resp: falcon.Response):
        """Handles GET requests"""
        def is_active(thread):
            return "ACTIVE" if self.is_alive(thread) else "READY"

        self.wlogger.debug("Processing get request")
        resp.status = falcon.HTTP_200  # This is the default status
        resp.content_type = falcon.MEDIA_JSON  # Default is JSON, so override
        status = self.status()
        status['status'] = {
            COMMAND_PRODUCE: is_active(self.produce_thread),
            COMMAND_CONSUME: is_active(self.consume_thread),
            COMMAND_ATOMIC: is_active(self.atomic_thread)
        }
        resp.media = status

    def on_delete(self, req: falcon.Request, resp: falcon.Response):
        self.terminate()

        resp.status = falcon.HTTP_200  # This is the default status
        resp.content_type = falcon.MEDIA_JSON  # Default is JSON, so override
        resp.media = self.status()

    def _validate_media_type(self, req: falcon.Request,
                             resp: falcon.Response) -> bool:
        if req.content_type != falcon.MEDIA_JSON:
            resp.status = falcon.HTTP_400
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {"errors": ["Invalid request media type"]}
            return False
        else:
            return True

    def _http_400(self, errors: list[str], resp: falcon.Response) -> None:
        resp.status = falcon.HTTP_400
        resp.content_type = falcon.MEDIA_JSON
        resp.media = {"errors": errors}

    def _validate_request(self, topics_cfg_keys: list[str],
                          forbidden: list[str], req: falcon.Request,
                          resp: falcon.Response) -> bool:
        error_msgs = validate_keys(req.media, topics_cfg_keys, forbidden)
        if error_msgs:
            msg = f"Incoming request invalid: {', '.join(error_msgs)}"
            self.wlogger.error(msg)
            self._http_400([msg], resp)
            return False
        else:
            return True

    def _check_active_thread(self, thread: threading.Thread | None,
                             req: falcon.Request,
                             resp: falcon.Response) -> bool:
        if thread is not None:
            if thread.is_alive():
                self._http_400(["Active job not finished"], resp)
                return True
        return False


class StreamVerifierProduce(StreamVerifierWeb):
    """Holds specific POST handlers for host:port/produce requests

    On POST runs produce command, can use cfg from status JSON.
    {
        "topic_group_id": "group-stream-verifier-tx",
        "topic_produce": "stream-topic-src",
        "topic_consume": "stream-topic-dst",
        "consume_timeout_s": 60,
        "msg_rate_limit": 0,
        "msg_total": 256
    }

    If Produce is active, returns error JSON
    Validation errors are returned in the same way
    {
        "errors": [
            "Active produce job not finished"
        ]
    }

    """
    def on_post(self, req: falcon.Request, resp: falcon.Response):
        self.wlogger.debug("Processing produce post request")

        if not self._validate_media_type(req, resp):
            self.wlogger.warning("Media type invalid in produce request")
            return
        elif self._check_active_thread(self.produce_thread, req, resp):
            self.wlogger.warning("Produce action is active")
            return

        # update topic config
        topics_cfg_keys = list(vars(self.live_topic).keys())
        topics_cfg_keys += ["msg_rate_limit", "total_messages"]
        if not self._validate_request(topics_cfg_keys, [], req, resp):
            # response is already populated inside validate_request
            return
        else:
            # Update topic configs
            if 'msg_rate_limit' in req.media:
                app_config.msg_rate_limit = req.media.pop('msg_rate_limit')
                self.live_topic.msgs_rate_ms = app_config.msgs_rate_ms
            if 'target_topic_name' in req.media:
                self.live_topic.target_topic_name = req.media[
                    'target_topic_name']
            for k, v in req.media.items():
                self.live_topic.__setattr__(k, v)
            # start producers
            self.init_producers()
            self.produce(wait=False)

            resp.status = falcon.HTTP_200
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {'result': "OK"}


class StreamVerifierConsume(StreamVerifierWeb):
    """Handled host:port/consume requests

    On POST runs consume command, can use cfg from status JSON.
    {
        "topic_group_id": "group-stream-verifier-tx",
        "topic_produce": "stream-topic-src",
        "topic_consume": "stream-topic-dst",
        "consume_timeout_s": 60,
        "msg_rate_limit": 0,
        "msg_total": 256
    }

    If Consume is active, returns error JSON
    Validation errors are returned in the same way
    {
        "errors": [
            "Active consume job not finished"
        ]
    }

    """
    def on_post(self, req: falcon.Request, resp: falcon.Response):
        self.wlogger.debug("Processing consume post request")
        # Validate request
        if not self._validate_media_type(req, resp):
            self.wlogger.warning("Media type invalid in produce request")
            return
        elif self._check_active_thread(self.consume_thread, req, resp):
            self.wlogger.warning("Consume action is active")
            return

        # update topic config
        topics_cfg_keys = ["source_topic_name"]
        forbidden = ["msg_rate_limit", "total_messages"]
        if not self._validate_request(topics_cfg_keys, forbidden, req, resp):
            # response is already populated inside validate_request
            return

        # Update topic configs
        for k, v in req.media.items():
            self.live_topic.__setattr__(k, v)
        # start consumers
        self.init_consumers()
        self.consume(wait=False)

        resp.status = falcon.HTTP_200
        resp.content_type = falcon.MEDIA_JSON
        resp.media = {'result': "OK"}


class StreamVerifierAtomic(StreamVerifierWeb):
    """Handled host:port/consume requests

    On POST runs atomic consume/produce command, can use cfg from status JSON.
    {
        "topic_group_id": "group-stream-verifier-tx",
        "topic_produce": "stream-topic-src",
        "topic_consume": "stream-topic-dst",
        "consume_timeout_s": 60,
        "msg_rate_limit": 0,
        "msg_total": 256
    }

    If Consume is active, returns error JSON
    Validation errors are returned in the same way
    {
        "errors": [
            "Active consume job not finished"
        ]
    }

    """
    def on_post(self, req: falcon.Request, resp: falcon.Response):
        self.wlogger.debug("Processing atomic consume/produce post request")
        # Validate request
        if not self._validate_media_type(req, resp):
            self.wlogger.warning("Media type invalid in atomic request")
            return
        elif self._check_active_thread(self.consume_thread, req, resp):
            self.wlogger.warning("Atomic action is active")
            return

        # update topic config
        topics_cfg_keys = list(vars(self.live_topic).keys())
        topics_cfg_keys += ["msg_rate_limit", "total_messages"]
        if not self._validate_request(topics_cfg_keys, [], req, resp):
            # response is already populated inside validate_request
            return
        elif req.media['source_topic_name'] == req.media['target_topic_name']:
            self._http_400(["Source and taget topics can't be equal"], resp)
            return
        else:
            # Update topic configs
            for k, v in req.media.items():
                self.live_topic.__setattr__(k, v)
            # start consumers
            self.init_atomic_produce()
            self.atomic_produce(wait=False)

            resp.status = falcon.HTTP_200
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {'result': "OK"}


def start_webserver():
    def terminate_handler(signum, frame):
        """SIGTERM handler
        """
        signame = signal.Signals(signum).name
        logger.info(f"{signame} ({signum}) received, terminating threads")
        producer.terminate()
        consumer.terminate()

    def add_route(route: str, handler: AppCfg | StreamVerifier):
        logger.debug(f"Registering handler for '/' as {type(handler)}")
        app.add_route(route, handler)

    global app_config
    logger = setup_logger(LOGGER_STARTUP)

    app = falcon.App()
    json_handler = falcon.media.JSONHandler(dumps=partial(
        json.dumps,
        cls=NpEncoder,
        sort_keys=True,
    ), )
    extra_handlers = {
        'application/json': json_handler,
    }

    app = falcon.App()
    app.req_options.media_handlers.update(extra_handlers)
    app.resp_options.media_handlers.update(extra_handlers)

    # Add subpages
    logger.debug("Initializing producer class")
    producer = StreamVerifierProduce(app_config)
    logger.debug("Initializing consumer class")
    consumer = StreamVerifierConsume(app_config)
    logger.debug("Initializing atomic class")
    atomic = StreamVerifierAtomic(app_config)
    add_route("/", app_config)
    add_route("/produce", producer)
    add_route("/verify", consumer)
    add_route("/atomic", atomic)

    # Create and run service
    with make_server('', app_config.web_port, app) as httpd:
        # Serve until process is killed, SGTERM received or Keyboard interrupt
        try:
            logger.debug("Registering SIGTERM")
            signal.signal(signal.SIGTERM, terminate_handler)
            logger.info(f'Serving on port {app_config.web_port}...')
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Got keyboard interupt, exiting")
            producer.terminate()
            consumer.terminate()


COMMAND_PRODUCE = 'produce'
COMMAND_ATOMIC = 'atomic'
COMMAND_CONSUME = 'consume'
COMMAND_SERVICE = 'webservice'
commands = [COMMAND_PRODUCE, COMMAND_ATOMIC, COMMAND_CONSUME]


def process_command(command, cfg, ioclass):
    try:
        logger = setup_logger(LOGGER_CLI_COMMAND)
        verifier = StreamVerifier(cfg.brokers)
        if command == COMMAND_PRODUCE:
            logger.info("Init Produce command")
            verifier.init_producers()
            _rate = "no rate limiting"
            if cfg.msg_rate_limit > 0:
                _rate = f"{cfg.msg_rate_limit}/sec"
            logger.info(f"Producing {cfg.msg_total} messages, {_rate}")
            verifier.produce()
            data = verifier.status()
        elif command == COMMAND_ATOMIC:
            logger.info("Init Atomic Consume and Produce command")
            verifier.init_atomic_produce()
            logger.info(f"Consuming from {cfg.topic_consume} and "
                        f"producing to {cfg.topic_produce}")
            verifier.atomic_produce()
            data = verifier.status()
        elif command == COMMAND_CONSUME:
            logger.info("Init Consume command")
            verifier.init_consumers()
            logger.info(f"Consuming all messages from {cfg.topic_consume}")
            verifier.consume()
            data = verifier.status()
    except (ck.KafkaException) as e:
        data = {'error': f"{e.__str__()} for '{cfg.brokers}'"}
    except KeyboardInterrupt:
        logger.info("Got keyboard interupt, exiting")
        verifier.terminate()
        data = verifier.status()
    except Exception as e:
        import traceback
        # If timeout happens, it will go here
        # and be reported and handled like a normal error
        exc_fmt = traceback.format_exception(type(e), e, e.__traceback__)
        trimmed = []
        # Trim long traceback lines here
        for line in exc_fmt:
            _size = len(line)
            # Traceback lines actually trippled:
            # "File ...\n Code hint\nMarker"
            # This is why 512 would work better
            if len(line) > 512:
                trimmed += [line[:256] + f"...({_size} chars)"]
            else:
                trimmed += [line]
        data = {'error': ''.join(trimmed)}
    finally:
        write_json(ioclass, data)


def main(args):
    global log_level
    global app_config

    if args.isdebug:
        log_level = logging.DEBUG

    logger = setup_logger(level=log_level)
    logger.info(f"{title}")
    logger.info(f"Using log level {logging.getLevelName(log_level)}")

    if args.command in [COMMAND_CONSUME, COMMAND_ATOMIC]:
        isvalid, error = validate_option("--consume-stop-criteria",
                                         args.consume_stop_criteria,
                                         consume_stop_options)
        if not isvalid:
            logger.error(error)
            sys.exit(1)
    elif args.command == COMMAND_ATOMIC:
        if args.topic_produce == args.topic_consume:
            logger.error("Source and taget topics can't be equal")
            sys.exit(1)

    app_config.update(vars(args))
    if args.command == COMMAND_SERVICE:
        # Run app as a service
        logger.info(f"Starting as a webservice on {app_config.web_port}")
        start_webserver()
    elif args.command in commands:
        process_command(args.command, app_config, sys.stdout)
    else:
        write_json(sys.stdout, {'error': f"unknown command '{args.command}'"})
        sys.exit(1)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=title)
    parser.add_argument('-b',
                        '--brokers',
                        dest="brokers",
                        default='localhost:9092',
                        help="Bootstrap broker(s) (host[:port])")

    parser.add_argument('-d',
                        '--debug',
                        dest="isdebug",
                        action="store_true",
                        default=False,
                        help="Debug log level")

    parser.add_argument("-g",
                        "--group-id",
                        dest="topic_group_id",
                        default="group-stream-verifier-tx",
                        help="Group it to use")

    subparsers = parser.add_subparsers(dest='command', required=True)
    parser_produce = subparsers.add_parser(COMMAND_PRODUCE)
    parser_produce.add_argument("--topic-produce",
                                dest="topic_produce",
                                default="stream-topic-src",
                                help="Topic to produce messages to")

    parser_produce.add_argument('--rps',
                                dest='msg_rate_limit',
                                default=0,
                                type=int,
                                help="Producer's message rate per sec")
    parser_produce.add_argument('-t',
                                '--total',
                                dest='msg_total',
                                default=256,
                                type=int,
                                help="Producer's message rate per sec")

    parser_consume = subparsers.add_parser(COMMAND_CONSUME)
    parser_consume.add_argument("--topic-consume",
                                dest="topic_consume",
                                default="stream-topic-dst",
                                help="Topic to use for consumption. ")

    parser_consume.add_argument('--consume-stop-criteria',
                                dest="consume_stop_criteria",
                                default="sleep",
                                type=str,
                                help="When to stop consumption: "
                                "'eof' - on first partition EOF, "
                                "'sleep' - on no new messages received "
                                "after sleep time after EOF, "
                                "'continuos' - stop only on terminate signal")

    parser_consume.add_argument('--consume-sleep-timeout',
                                dest="consume_sleep_time_s",
                                default=60,
                                type=int,
                                help="Time to sleep after EOF in sleep "
                                "stop criteria")

    parser_atomic = subparsers.add_parser(COMMAND_ATOMIC)
    parser_atomic.add_argument('-t',
                               '--total',
                               dest='msg_total',
                               default=256,
                               type=int,
                               help="Producer's message rate per sec")
    parser_atomic.add_argument('--rps',
                               dest='msg_rate_limit',
                               default=0,
                               type=int,
                               help="Producer's message rate per sec")
    parser_atomic.add_argument("--topic-produce",
                               dest="topic_produce",
                               default="stream-topic-src",
                               help="Topic to use for producing messages to")

    parser_atomic.add_argument("--topic-consume",
                               dest="topic_consume",
                               default="stream-topic-src",
                               help="Topic to use for consuming messages from")

    parser_atomic.add_argument('--consume-stop-criteria',
                               dest="consume_stop_criteria",
                               default="sleep",
                               type=str,
                               help="When to stop consumption: "
                               "'eof' - on first partition EOF, "
                               "'sleep' - on no new messages received "
                               "after sleep time after EOF, "
                               "'continuos' - stop only on terminate signal")

    parser_atomic.add_argument('--consume-sleep-timeout',
                               dest="consume_sleep_time_s",
                               default=60,
                               type=int,
                               help="Time to sleep after EOF in sleep "
                               "stop criteria")

    parser_service = subparsers.add_parser(COMMAND_SERVICE)
    parser_service.add_argument('--port',
                                dest='web_port',
                                default=8090,
                                type=int,
                                help="Webservice port to bind to")

    main(parser.parse_args())
