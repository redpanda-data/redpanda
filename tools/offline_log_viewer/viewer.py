#!/usr/bin/env python3
import os
import sys

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


def print_kafka(store, topic):
    for ntp in store.ntps:
        if ntp.nspace.startswith("kafka"):
            if topic and ntp.topic != topic:
                continue

            log = KafkaLog(ntp)
            logger.info(f'topic: {ntp.topic}, partition: {ntp.partition}')
            for header in log.batch_headers():
                logger.info(json.dumps(header, indent=2))


def print_kafka_records(store, topic):
    for ntp in store.ntps:
        if ntp.nspace.startswith("kafka"):
            if topic and ntp.topic != topic:
                continue

            log = KafkaLog(ntp)
            logger.info(f'topic: {ntp.topic}, partition: {ntp.partition}')
            for batch in log.batches():
                logger.info(json.dumps(batch.header_dict(), indent=2))
                for record in batch:
                    logger.info(
                        json.dumps(
                            {
                                "key":
                                None
                                if record.key == None else record.key.hex(),
                                "value":
                                None if record.value == None else
                                record.value.hex(),
                            },
                            indent=2))


def print_groups(store):
    for ntp in store.ntps:
        if ntp.nspace == "kafka_internal" and ntp.topic == "group":
            l = GroupsLog(ntp)
            l.decode()
            logger.info(json.dumps(l.records, indent=2))
    logger.info("")


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description='Redpanda log analyzer')
        parser.add_argument('--path',
                            type=str,
                            help='Path to the log desired to be analyzed')
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

    if not os.path.exists(options.path):
        logger.error(f"Path doesn't exist {options.path}")
        sys.exit(1)
    store = Store(options.path)
    if options.type == "kvstore":
        print_kv_store(store)
    elif options.type == "controller":
        print_controller(store)
    elif options.type == "kafka":
        print_kafka(store, options.topic)
    elif options.type == "kafka_records":
        print_kafka_records(store, options.topic)
    elif options.type == "group":
        print_groups(store)


if __name__ == '__main__':
    main()
