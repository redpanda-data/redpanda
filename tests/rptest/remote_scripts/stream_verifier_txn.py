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
