#!/usr/bin/env python3
import os
import sys
from os.path import join

from controller import ControllerLog
from consumer_groups import GroupsLog
from consumer_offsets import OffsetsLog
from tx_coordinator import TxLog

from storage import Store
from kvstore import KvStore
from kafka import KafkaLog
import logging
import json

logger = logging.getLogger('viewer')


def print_kv_store(store):
    # Map of partition ID to list of kvstore items
    result = {}

    for ntp in store.ntps:
        if ntp.nspace == "redpanda" and ntp.topic == "kvstore":
            logger.info(f"inspecting {ntp}")
            kv = KvStore(ntp)
            kv.decode()
            items = kv.items()

            result[ntp.partition] = items

    # Send JSON output to stdout in case caller wants to parse it, other
    # CLI output goes to stderr via logger
    print(json.dumps(result, indent=2))


def print_controller(store, bin_dump: bool):
    for ntp in store.ntps:
        if ntp.nspace == "redpanda" and ntp.topic == "controller":
            ctrl = ControllerLog(ntp)
            ctrl.decode(bin_dump)

            # Send JSON output to stdout in case caller wants to parse it, other
            # CLI output goes to stderr via logger
            print(json.dumps(ctrl.records, indent=2))


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


def print_consumer_offsets(store):
    records = []
    for ntp in store.ntps:
        if ntp.nspace == "kafka" and ntp.topic == "__consumer_offsets":
            l = OffsetsLog(ntp)
            l.decode()
            records.append({
                "partition_id": ntp.partition,
                "records": l.records
            })

    # Send JSON output to stdout in case caller wants to parse it, other
    # CLI output goes to stderr via logger
    print(json.dumps(records, indent=2))


def print_tx_coordinator(store):
    for ntp in store.ntps:
        if ntp.nspace == "kafka_internal" and ntp.topic == "tx":
            l = TxLog(ntp)
            for result in l.decode():
                logger.info(json.dumps(result, indent=2))
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


def validate_tx_coordinator(path):
    tx = join(path, "kafka_internal", "tx")
    if not os.path.exists(tx):
        logger.error(f"tx coordinator dir {tx} doesn't exist")
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
                                'controller', 'kvstore', 'kafka',
                                'consumer_offsets', 'legacy-group',
                                'kafka_records', 'tx_coordinator'
                            ],
                            required=True,
                            help='operation to execute')
        parser.add_argument(
            '--topic',
            type=str,
            required=False,
            help='for kafka type, if set, parse only this topic')
        parser.add_argument('-v', "--verbose", action="store_true")
        parser.add_argument(
            '--dump',
            action='store_true',
            help='output binary dumps of keys and values being parsed')
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
        print_controller(store, options.dump)
    elif options.type == "kafka":
        validate_topic(options.path, options.topic)
        print_kafka(store, options.topic, headers_only=True)
    elif options.type == "kafka_records":
        validate_topic(options.path, options.topic)
        print_kafka(store, options.topic, headers_only=False)
    elif options.type == "legacy-group":
        print_groups(store)
    elif options.type == "consumer_offsets":
        print_consumer_offsets(store)
    elif options.type == "tx_coordinator":
        validate_tx_coordinator(options.path)
        print_tx_coordinator(store)
    else:
        logger.error(f"Unknown type: {options.type}")
        sys.exit(1)


if __name__ == '__main__':
    main()
