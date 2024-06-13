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
import signal
import sys
import threading

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from itertools import repeat
from time import sleep
from typing import Generator, IO, Tuple
from wsgiref.simple_server import make_server

global title
app_name = "StreamVerifierTx"
title = f"{app_name}, python transaction verifier worker"
log_level = logging.INFO
LOGGER_STARTUP = 'startup'
LOGGER_MAIN = 'main'
LOGGER_WEB_PRODUCE = "web_produce"
LOGGER_WEB_CONSUME = "web_consume"
CONSUMER_LOGGING_THRESHOLD = 1000


def setup_logger(name: str = "", level: int = logging.DEBUG) -> logging.Logger:
    """Create main loggeer or its child based on naming

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
    _name = "stream_verifier"
    if name is not None:
        # Just simpel hierachy, no special handler
        _name = f"{_name}.{name}"
        logger = logging.getLogger(_name)
        return logger
    else:
        logger = logging.getLogger(_name)
        logger.addHandler(handler)
        logger.setLevel(level)
        return logger


def write_json(ioclass: IO, data: dict) -> None:
    ioclass.write(json.dumps(data))
    ioclass.write('\n')
    ioclass.flush()


def validate_keys(imcoming: list[str], local: list[str],
                  forbidden: list[str]) -> list[str]:
    error_msgs = []
    # Validate incoming keys
    for k in imcoming:
        if k in forbidden:
            error_msgs += [f"Key '{k}' can't be updated"]
        elif k not in local:
            error_msgs += [f"Unknown key '{k}'"]
    return error_msgs


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
    topic_group_id: str = "group-stream-verifier-tx"
    topic_prefix: str = "stream-verifier-topic"
    topic_count: int = 16
    # 0 - no rate limiting
    msg_rate: int = 0
    msg_per_txn: int = 1
    # per-topic = msg_total / topic_count
    msg_total: int = 256
    # time between two consumed messages
    consume_timeout: int = 60
    # how often to log sent message event
    # On higher scale it could be >10000 to eliminate log IO overhead
    consumer_logging_threshold: int = 1000
    # How many workers will be in the message processing pool
    worker_threads: int = 4
    # web server port
    web_port: int = 8090

    @property
    def topics_config(self):
        """Generates topic config for TopicsConfig class

        Returns:
            dict: config that can be used for TopicsConfig(**topic_config)
        """
        return {
            "topic_group_id": self.topic_group_id,
            "topic_prefix": self.topic_prefix,
            "topic_count": self.topic_count,
            "consume_timeout": self.consume_timeout
        }

    @property
    def forbidden_keys(self) -> list[str]:
        # non-updatable vars via REST handle
        return ['web_port', 'app_name']

    def on_get(self, req, resp):
        """Handles 'GET:<host:port>/' calls. Returns current app config

        Args:
            req (falcon.Request): request coming in
            resp (falcon.Response): response to be filled
        """
        resp.status = falcon.HTTP_200
        resp.content_type = falcon.MEDIA_JSON
        # need to copy here otherwise, Class gets broken
        _conf = deepcopy(vars(self))
        # remove forbidden keys
        for k in _conf.keys():
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
        # Validation
        error_msgs = validate_keys(keys_req, keys_list, self.forbidden_keys)
        if len(error_msgs) > 0:
            # Just dump errors as list
            msg = f"Incoming request invalid: {', '.join(error_msgs)}"
            logger.error(msg)
            resp.status = falcon.HTTP_400
            resp.media_type = falcon.MEDIA_JSON
            resp.media = {"errors": msg}
        else:
            # since validation is passed, no more stric checking needed
            for k in req.media.keys():
                setattr(self, k, req.media[k])
            logger.debug(f"...updated app config: {vars(self)}")
            resp.status = falcon.HTTP_200
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {'result': "OK"}


# singleton for app_config
app_config = AppCfg()


@dataclass(kw_only=True)
class TopicsConfig:
    """Holds data for topic operations: Produce/Consume.
    Can generate list of topic names based on config
    """
    topic_group_id: str
    topic_prefix: str
    topic_count: int
    consume_timeout: int
    partitions: int = 0
    replicas: int = 0

    @property
    def names(self):
        # <topic_prefix>-<sequence_number>
        return [
            f"{self.topic_prefix}-{idx}" for idx in range(self.topic_count)
        ]


@dataclass(kw_only=True)
class TopicStatus(Updateable):
    """Holds single live topic status

    Returns:
        _type_: _description_
    """
    # Reserved for future use in case of name will not be usable
    id: int
    # topic name
    name: str
    # how many messages to process in single transaction
    msgs_per_transaction: int
    # Total messages to be processed for this topic
    total_messages: int
    # Current topic index or how much messages already produced/consumed
    index: int
    # timestamp for last message
    last_message_ts: float
    # how much ms should pass between messages to be sent
    msgs_rate_ms: float
    # how much time to wait when consuming next message
    consume_timeout: int
    # Precreated Producer class.
    producer: ck.Producer
    # All data needed to create consumer class.
    # As opposed to Producer, Consumer must be created/used in the same thread
    consumer_config: dict
    # Termination signalling flag
    # It is simpler that threading event and faster
    terminate: bool

    @property
    def transaction_id(self):
        return f"{self.id}-{self.name}-{self.last_message_ts}"


class MessageGenerator:
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
    def __init__(self,
                 brokers,
                 topics_config: dict,
                 worker_threads: int,
                 rate=0,
                 total_messages=100):
        # Remove quotes if any
        self.logger = setup_logger('core')
        # Remove quotes from broker config value if an
        self.brokers = brokers.strip('\"').strip("'")
        # Create main topics config
        self.topics_config = TopicsConfig(**topics_config)
        self.message_rate = rate
        # It is reasonable to assume that single message will not be sent
        # faster than 1 ms in case of no rate limitations
        self.msgs_rate_ms = 1000 / self.message_rate if rate > 0 else 0
        # total messages to process in all topics
        self.total_messages = total_messages
        self.workers = worker_threads
        # Annoucement of dynamic vars
        self.topics_status = {}
        self.topics = {}
        self.id_index = 0
        self.produce_thread = None
        self.consume_thread = None
        self.delivery_reports = {}

    def init_producers(self):
        """Precreates topic status lists and initializes Producers.
        One producer per topic.
        """
        self.logger.info("Initializing producers")
        # Calculate messages per topic and announce changes
        msgs_per_topic = int(self.total_messages /
                             self.topics_config.topic_count)
        new_total_messages = msgs_per_topic * self.topics_config.topic_count
        if new_total_messages != self.total_messages:
            self.logger.warning("Messages per topic rounded to "
                                f"{msgs_per_topic} with the new "
                                f"total of {new_total_messages}")
            self.total_messages = new_total_messages
        else:
            self.logger.info(f"Messages per topic is {msgs_per_topic} "
                             f"with the total of {self.total_messages}")
        for name in self.topics_config.names:
            topic_config = {
                "id": self.id_index,
                "name": name,
                "msgs_per_transaction": app_config.msg_per_txn,
                "total_messages": msgs_per_topic,
                "index": 0,
                "last_message_ts": datetime.now().timestamp(),
                "msgs_rate_ms": self.msgs_rate_ms,
                "consume_timeout": app_config.consume_timeout,
                "producer": None,
                "consumer_config": {},
                "terminate": False
            }
            t = TopicStatus(**topic_config)
            t.producer = ck.Producer({
                'bootstrap.servers': self.brokers,
                'transactional.id': t.transaction_id
            })
            t.producer.init_transactions()
            self.topics[t.name] = t

    def _send_transaction(self, logger: logging.Logger,
                          topic: TopicStatus) -> int:
        """
            Transactional message sent. Uses simple indexed generator
        """
        sent_count = 0

        def acked(err: ck.KafkaError, msg: ck.Message):
            """
                Unsafe callback that fills up delivery reports
                Can be modified to send reports externally

                err: error class from Kafka
                msg: message that was sent
            """
            t = msg.topic()
            pt = msg.partition()
            pt = pt if pt else 'N'
            k = msg.key().decode()
            v = msg.value().decode()

            if err is not None:
                # log delivery error locally
                logger.error(f"[{t}({pt}):{k}/{v}] {err.str()}")
                # update delivery report
                self.delivery_reports[f"{t}-{k}"] = {
                    "latency": msg.latency(),
                    "outcome": err.str()
                }

            else:
                # Saving all delivery reports turned off for now
                # self.delivery_reports[f"{t}-{k}"] = {
                #     "partition": pt,
                #     "latency": msg.latency(),
                #     "offset": msg.offset(),
                #     "outcome": "OK"
                # }
                pass
            return

        def time_since_last_msg() -> int:
            diff_ms = datetime.now().timestamp() - topic.last_message_ts
            return int(diff_ms * 1000)

        msg_gen = MessageGenerator()
        for key, value in msg_gen.gen_indexed_messages(
                topic.index, topic.msgs_per_transaction):
            # Handle message rate
            if topic.msgs_rate_ms > 0:
                _time_since = time_since_last_msg()
                if _time_since < topic.msgs_rate_ms:
                    wait_time = (topic.msgs_rate_ms - _time_since) / 1000
                    logger.debug(f"...waiting {wait_time}s "
                                 "before sending message")
                    sleep(wait_time)

            # Async message sending
            topic.producer.begin_transaction()
            try:
                topic.producer.produce(topic.name,
                                       key=key,
                                       value=value,
                                       callback=acked)
                # Commit transaction or abort it
                topic.producer.commit_transaction()
                topic.index += 1
                sent_count += 1
                if topic.index % CONSUMER_LOGGING_THRESHOLD == 0:
                    logger.debug(f"..sent {topic.index} to {topic.name}")
            except ck.KafkaException:
                # In case of any exception, abort it

                # TODO: handle retry message logic

                topic.producer.abort_transaction()
                logger.warning(f"Transaction {topic.transaction_id} aborted")

            # save time for this message
            topic.last_message_ts = datetime.now().timestamp()

            # exit if terminate flag is set
            if topic.terminate:
                logger.warning("Got terminate signal. Exiting")
                break
        # Return stats
        return sent_count

    @staticmethod
    def _worker_thread(func, workers: int, topics: dict, total_messages: int,
                       logger: logging.Logger):
        pool = ThreadPoolExecutor(workers, "stream_worker")
        msgs_processed = 0
        # Sending loop
        while msgs_processed < total_messages:
            # Get processed message count from worker threads
            for msg_count in pool.map(func, repeat(logger),
                                      list(topics.values())):
                msgs_processed += msg_count
            # Log pretty name of underlying func
            logger.info(f"{func.__qualname__}, "
                        f"processed so far {msgs_processed}")
            # Check termination flag in all topics before the next chunk
            if any([t.terminate for t in topics.values()]):
                logger.warning("Got terminate signal, "
                               "exiting from message processing")
        logger.info("End of processing messages.")

    def produce(self, wait=True):
        """Starts produce messages thread

        Args:
            wait (bool, optional): Wait for produce thread to finish or not.
            Defaults to True.
        """
        self.logger.info("Start of sending messages")
        thread = threading.Thread(name="stream_produce_thread",
                                  target=self._worker_thread,
                                  args=(self._send_transaction, self.workers,
                                        self.topics, self.total_messages,
                                        self.logger))
        thread.start()
        self.produce_thread = thread
        if wait:
            thread.join()
        return

    def init_consumers(self):
        """Precreates topic status lists for Cosuming thread.
        One Consumer per worker thread.
        """
        self.logger.info("Initializing consumers")
        for name in self.topics_config.names:
            topic_config = {
                "id": self.id_index,
                "name": name,
                # 0 means consume all messages
                "total_messages": 0,
                # consumed messages so far
                "index": 0,
                # time when last message consumed
                "last_message_ts": datetime.now().timestamp(),
                # max time between consuming messages
                "consume_timeout": app_config.consume_timeout,
                # consumer config
                "consumer_config": {
                    'bootstrap.servers': self.brokers,
                    'group.id': self.topics_config.topic_group_id,
                    'auto.offset.reset': 'earliest',
                    'enable.auto.commit': False,
                    'enable.partition.eof': True,
                },
                # termination flag
                "terminate": False,
                # not used, but needs to be filled
                "msgs_per_transaction": 1,
                "producer": None,
                "msgs_rate_ms": self.msgs_rate_ms
            }
            t = TopicStatus(**topic_config)
            self.topics[t.name] = t
        return

    def consume(self, wait=True):
        """Starts consume messages thread

        Args:
            wait (bool, optional): Wait for consume thread to finish or not.
            Defaults to True.
        """
        self.logger.info("Start of consuming messages")
        thread = threading.Thread(name="stream_consume_thread",
                                  target=self._worker_thread,
                                  args=(self._consume_from_topic, self.workers,
                                        self.topics, self.total_messages,
                                        self.logger))
        thread.start()
        self.consume_thread = thread
        if wait:
            thread.join()
        return

    @staticmethod
    def _consume_from_topic(logger: logging.Logger, topic: TopicStatus):
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

        topic.last_message_ts = datetime.now().timestamp()

        # Message consuming loop
        consumer = ck.Consumer(topic.consumer_config)
        try:
            # Recent changes offers use of subscribe
            # regardless of transactions and partitions
            consumer.subscribe([topic.name])
            while True:
                # calculate elapsed time
                _since_last_msg_ms = time_since_last_msg()

                # Exit on timeout
                if _since_last_msg_ms > topic.consume_timeout:
                    logger.error("Timeout consuming messages "
                                 f"from {topic.name}")
                    break

                # Poll for the message
                msg = consumer.poll(timeout=1.0)
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
                        break
                    # If not EOF, raise the error
                    elif msg.error():
                        raise ck.KafkaException(msg.error())
                else:
                    topic.last_message_ts = datetime.now().timestamp()
                    # Save index
                    topic.index += 1
                    # log only milestones to eliminate IO stress
                    if topic.index % CONSUMER_LOGGING_THRESHOLD == 0:
                        logger.debug(f"...consumed {topic.index} messages "
                                     f"from {topic.name}")

                # exit if terminate flag is set
                if topic.terminate:
                    logger.warning("Got terminate signal. Exiting")
                    break
        finally:
            # Close down consumer
            consumer.close()

        logger.info(f"Consumed {topic.index} messages")
        return topic.index

    def _calculate_totals(self) -> Tuple:
        msg_total = 0
        indices = []
        for t in self.topics.values():
            if t.producer is not None:
                msg_total += t.total_messages
            elif t.consumer_config:
                msg_total += t.index
            indices += [t.index]
        return msg_total, indices

    def status(self, name: str = ""):
        """Provides current processing status

        Args:
            name (str, optional): topic name. Defaults to "".

        Returns:
            dict: Dict with status
        """

        response = {"topics": {}}
        if len(name) > 0:
            # include topic as asked
            response['topics'][name] = vars(self.topics_status[name])

        msg_total, indices = self._calculate_totals()
        response['stats'] = {"total_messages": msg_total, "indices": indices}
        response['delivery_errors'] = self.delivery_reports
        return response

    def terminate(self):
        """Sets terminate flag for all topics and flushes producers
        """
        for t in self.topics.values():
            t.terminate = True
            if t.producer is not None:
                t.producer.flush()


class StreamVerifierProduce(StreamVerifier):
    def __init__(self, cfg):
        self.cfg = cfg
        self.wlogger = setup_logger(LOGGER_WEB_PRODUCE)
        super().__init__(cfg.brokers,
                         cfg.topics_config,
                         cfg.worker_threads,
                         rate=cfg.msg_rate,
                         total_messages=cfg.msg_total)

    def on_get(self, req: falcon.Request, resp: falcon.Response):
        """Handles GET requests"""
        self.wlogger.debug("Processing produce get request")
        resp.status = falcon.HTTP_200  # This is the default status
        resp.content_type = falcon.MEDIA_JSON  # Default is JSON, so override
        resp.media = self.status()

    def on_post(self, req: falcon.Request, resp: falcon.Response):
        self.wlogger.debug("Processing produce post request")

        if req.content_type != falcon.MEDIA_JSON:
            resp.status = falcon.HTTP_400
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {"errors": ["Invalid request media type"]}
            return

        # Check active producing event
        if self.produce_thread is not None:
            if self.produce_thread.is_alive():
                resp.status = falcon.HTTP_400
                resp.content_type = falcon.MEDIA_JSON
                resp.media = {"errors": ["Active produce job not finished"]}
                return

        # update topic config
        topics_cfg_keys = list(vars(self.topics_config).keys())
        topics_cfg_keys += ["msg_rate", "msg_total"]
        error_msgs = validate_keys(req.media, topics_cfg_keys, [])
        if len(error_msgs) > 0:
            msg = f"Incoming request invalid: {', '.join(error_msgs)}"
            self.wlogger.error(msg)
            resp.status = falcon.HTTP_400
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {"errors": msg}
        else:
            # Update topic configs
            self.message_rate = req.media['msg_rate'] \
                if 'msg_rate' in req.media else app_config.msg_rate
            self.total_messages = req.media['msg_total'] \
                if 'msg_total' in req.media else app_config.msg_total
            if 'topic_prefix' in req.media:
                self.topics_config.topic_prefix = req.media['topic_prefix']
            if 'topic_count' in req.media:
                self.topics_config.topic_count = req.media['topic_count']
            # start producers
            self.init_producers()
            self.produce(wait=False)

            resp.status = falcon.HTTP_200
            resp.content_type = falcon.MEDIA_TEXT
            resp.text = "OK"

    def on_delete(self, req: falcon.Request, resp: falcon.Response):
        self.terminate()

        resp.status = falcon.HTTP_200  # This is the default status
        resp.content_type = falcon.MEDIA_JSON  # Default is JSON, so override
        resp.media = self.status()

    def terminate(self):
        self.wlogger.debug("Terminating web produce class")
        for t in self.topics.values():
            t.terminate = True
            t.producer.flush()
        return


class StreamVerifierConsume(StreamVerifier):
    def __init__(self, cfg):
        self.cfg = cfg
        self.wlogger = setup_logger(LOGGER_WEB_CONSUME)
        super().__init__(cfg.brokers,
                         cfg.topics_config,
                         cfg.worker_threads,
                         rate=cfg.msg_rate,
                         total_messages=cfg.msg_total)

    def on_get(self, req: falcon.Request, resp: falcon.Response):
        """Handles GET requests"""
        self.wlogger.debug("Processing consume get request")
        resp.status = falcon.HTTP_200  # This is the default status
        resp.content_type = falcon.MEDIA_JSON  # Default is JSON, so override
        resp.media = self.status()

    def on_post(self, req: falcon.Request, resp: falcon.Response):
        self.wlogger.debug("Processing consume post request")
        # Validate request
        if req.content_type != falcon.MEDIA_JSON:
            resp.status = falcon.HTTP_400
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {"errors": ["Invalid request media type"]}
            return

        # Check active consuming event
        if self.consume_thread is not None and \
                self.consume_thread.is_alive():
            resp.status = falcon.HTTP_400
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {"errors": ["Active produce job not finished"]}
            return

        # update topic config
        topics_cfg_keys = ["topic_prefix", "topic_count"]
        forbidden = ["msg_rate", "msg_total"]
        error_msgs = validate_keys(req.media, topics_cfg_keys, forbidden)
        if len(error_msgs) > 0:
            msg = f"Incoming request invalid: {', '.join(error_msgs)}\n" \
                f"Valid keys are: {', '.join(topics_cfg_keys)}"
            self.wlogger.error(msg)
            resp.status = falcon.HTTP_400
            resp.content_type = falcon.MEDIA_JSON
            resp.media = {"errors": msg}
        else:
            # Update topic configs
            if 'topic_prefix' in req.media:
                self.topics_config.topic_prefix = req.media['topic_prefix']
            if 'topic_count' in req.media:
                self.topics_config.topic_count = req.media['topic_count']
            # start consumers
            self.init_consumers()
            self.consume(wait=False)

            resp.status = falcon.HTTP_200
            resp.content_type = falcon.MEDIA_TEXT
            resp.text = "OK"

    def terminate(self):
        self.wlogger.debug("Terminating web consume class")
        for t in self.topics.values():
            t.terminate = True
        return


def start_webserver():
    def terminate_handler(signum, frame):
        """SIGTERM handler

        Args:
            signum (_type_): _description_
            frame (_type_): _description_
        """
        signame = signal.Signals(signum).name
        logger.info(f"{signame} ({signum}) received, terminating threads")
        producer.terminate()
        consumer.terminate()

    def add_route(route: str, handler: AppCfg | StreamVerifier):
        logger.debug(f"Registering handler for '/' as {type(handler)}")
        app.add_route(route, handler)

    global app_config
    app = falcon.App()
    logger = setup_logger(LOGGER_STARTUP)

    # Add subpages
    logger.debug("Initializing producer class")
    producer = StreamVerifierProduce(app_config)
    logger.debug("Initializing consumer class")
    consumer = StreamVerifierConsume(app_config)
    add_route("/", app_config)
    add_route("/produce", producer)
    add_route("/consume", consumer)

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
COMMAND_CONSUME = 'consume'
COMMAND_SERVICE = 'webservice'
commands = [COMMAND_PRODUCE, COMMAND_CONSUME]


def process_command(command, cfg, ioclass):
    try:
        logger = setup_logger('cli_command')
        verifier = StreamVerifier(cfg.brokers,
                                  cfg.topics_config,
                                  cfg.worker_threads,
                                  rate=cfg.msg_rate,
                                  total_messages=cfg.msg_total)
        if command == COMMAND_PRODUCE:
            logger.info("Init Produce command")
            verifier.init_producers()
            _rate = "no rate limiting"
            if cfg.msg_rate > 0:
                _rate = f"{cfg.msg_rate}/sec"
            logger.info(f"Starting to produce {cfg.msg_total} messages "
                        f"to {cfg.topic_count} topics, {_rate}")
            verifier.produce()
            data = verifier.status()
        elif command == COMMAND_CONSUME:
            logger.info("Init Consume command")
            verifier.init_consumers()
            logger.info("Starting to consume all messages from "
                        f"{cfg.topic_prefix}*")
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

    app_config.update(vars(args))
    if args.command == COMMAND_SERVICE:
        # Run app as a service
        logger.info(f"Starting as a webservice on {app_config.web_port}")
        start_webserver()
    elif args.command in commands:
        process_command(args.command, app_config, sys.stdout)
    else:
        data = {
            'error':
            f"topic swarm command "
            f"'{args.command}' not yet implemented"
        }
        write_json(sys.stdout, data)
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

    parser.add_argument('-w',
                        '--workers',
                        dest="worker_threads",
                        default=4,
                        help="Number of threads to process messages")

    parser.add_argument(
        "-f",
        "--topic-prefix",
        dest="topic_prefix",
        default="topic-stream-verifier-tx",
        help="Topic prefix to use when creating. "
        "Formats: '<prefix>-p<partitions>-r<replicas>-<sequence_number>")

    parser.add_argument("-g",
                        "--group-id",
                        dest="topic_group_id",
                        default="group-stream-verifier-tx",
                        help="Group it to use")

    parser.add_argument('-c',
                        '--topic-count',
                        dest="topic_count",
                        default=16,
                        type=int,
                        help="Number of topics to create")

    subparsers = parser.add_subparsers(dest='command', required=True)
    parser_produce = subparsers.add_parser(COMMAND_PRODUCE)

    parser_produce.add_argument('--rps',
                                dest='msg_rate',
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

    parser_service = subparsers.add_parser(COMMAND_SERVICE)
    parser_service.add_argument('--port',
                                dest='web_port',
                                default=8090,
                                type=int,
                                help="Webservice port to bind to")

    main(parser.parse_args())
