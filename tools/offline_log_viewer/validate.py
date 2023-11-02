#!/usr/bin/env python3
import os
import sys
from os.path import join

import itertools
import storage
import json


def validate_path(path):
    if not os.path.exists(path):
        print(f"Path doesn't exist {path}")
        sys.exit(1)
    controller = join(path, "redpanda", "controller")
    if not os.path.exists(controller):
        print(
            f"Each redpanda data dir should have controller piece but {controller} isn't found"
        )
        sys.exit(1)


def validate(store):
    for ntp in store.ntps:
        for path in ntp.segments:
            segment = storage.Segment(path, recover=True)
            try:
                for batch in segment:
                    pass
            except storage.CorruptBatchRecoveryError as e:
                print(f"Corruption detected in {path}")
                for start, end, b in e.corruption:
                    print(
                        f"Corruption at [{start}, {end}) with header {b.header}"
                    )


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description='Redpanda log analyzer')
        parser.add_argument(
            '--path',
            type=str,
            help='Path to data dir of the node desired to be analyzed')
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()

    validate_path(options.path)
    store = storage.Store(options.path)
    validate(store)


if __name__ == '__main__':
    main()
