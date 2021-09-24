#!/usr/bin/env python3
import os
import sys

from controller import ControllerLog

from storage import Store
from kvstore import KvStore
import logging
import json

logger = logging.getLogger('viewer')


def print_kv_store(store):
    for ntp in store.ntps:
        if ntp.nspace == "redpanda" and ntp.topic == "kvstore":
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


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description='Redpanda log analyzer')
        parser.add_argument('--path',
                            type=str,
                            help='Path to the log desired to be analyzed')
        parser.add_argument('--type',
                            type=str,
                            choices=['controller', 'kvstore'],
                            required=True,
                            help='opertion to execute')
        parser.add_argument('-v', "--verbose", action="store_true")
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    if options.verbose:
        logging.basicConfig(level="DEBUG")
    else:
        logging.basicConfig(level="INFO")
    logger.info(f"starting metadata viewer with {options}")

    if not os.path.exists(options.path):
        logger.error(f"Path doesn't exist {options.path}")
        sys.exit(1)
    store = Store(options.path)
    if options.type == "kvstore":
        print_kv_store(store)
    elif options.type == "controller":
        print_controller(store)


if __name__ == '__main__':
    main()
