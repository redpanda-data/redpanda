#!/usr/bin/env python3
import os
import sys
from os.path import join

from controller import ControllerLog
from consumer_groups import GroupsLog

from storage import Store
from kvstore import KvStore
from kafka import KafkaLog
import logging
import json

logger = logging.getLogger('viewer')


def print_kv_store(store):
    for ntp in store.ntps:
        if ntp.nspace == "redpanda" and ntp.topic == "kvstore":
            logger.info(f"inspecting {ntp}")
            kv = KvStore(ntp)
            kv.decode()
            items = kv.items()
            logger.info(json.dumps(items, indent=2))


def print_controller(store):
    for ntp in store.ntps:
        if ntp.nspace == "redpanda" and ntp.topic == "controller":
            ctrl = ControllerLog(ntp)
            ctrl.decode()
            logger.info(json.dumps(ctrl.records, indent=2))


def print_kafka(store, topic, headers_only):
    for ntp in store.ntps:
        if ntp.nspace in ["kafka", "kafka_internal"]:
            if topic and ntp.topic != topic:
                continue

            logger.info(f'topic: {ntp.topic}, partition: {ntp.partition}')
            log = KafkaLog(ntp, headers_only=headers_only)
            for result in log.decode():
                logger.info(json.dumps(result, indent=2))


def print_groups(store):
    for ntp in store.ntps:
        if ntp.nspace == "kafka_internal" and ntp.topic == "group":
            l = GroupsLog(ntp)
            l.decode()
            logger.info(json.dumps(l.records, indent=2))
    logger.info("")


def validate_path(path):
    if not os.path.exists(path):
        logger.error(f"Path doesn't exist {path}")
        sys.exit(1)
    controller = join(path, "redpanda", "controller")
    if not os.path.exists(controller):
        logger.error(
            f"Each redpanda data dir should have controller piece but {controller} isn't found"
        )
        sys.exit(1)


def validate_topic(path, topic):
    if topic:
        topic = join(path, "kafka", topic)
        if not os.path.exists(topic):
            logger.error(f"Topic {topic} doesn't exist")
            sys.exit(1)


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description='Redpanda log analyzer')
        parser.add_argument(
            '--path',
            type=str,
            help='Path to data dir of the node desired to be analyzed')
        parser.add_argument('--type',
                            type=str,
                            choices=[
                                'controller', 'kvstore', 'kafka', 'group',
                                'kafka_records'
                            ],
                            required=True,
                            help='operation to execute')
        parser.add_argument(
            '--topic',
            type=str,
            required=False,
            help='for kafka type, if set, parse only this topic')
        parser.add_argument('-v', "--verbose", action="store_true")
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    if options.verbose:
        logging.basicConfig(level="DEBUG")
    else:
        logging.basicConfig(level="INFO")
    logger.info(f"starting metadata viewer with options: {options}")

    validate_path(options.path)

    store = Store(options.path)
    if options.type == "kvstore":
        print_kv_store(store)
    elif options.type == "controller":
        print_controller(store)
    elif options.type == "kafka":
        validate_topic(options.path, options.topic)
        print_kafka(store, options.topic, headers_only=True)
    elif options.type == "kafka_records":
        validate_topic(options.path, options.topic)
        print_kafka(store, options.topic, headers_only=False)
    elif options.type == "group":
        print_groups(store)
    else:
        logger.error(f"Unknown type: {options.type}")
        sys.exit(1)


if __name__ == '__main__':
    main()
