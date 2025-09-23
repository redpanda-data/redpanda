#!/usr/bin/env python3
import itertools
import json
import logging
import os
import sys
from collections import namedtuple
from os.path import join
from typing import Any

from consumer_groups import GroupsLog
from consumer_offsets import ConsumerGroupsSummaryGenerator, OffsetsLog
from controller import ControllerLog, ControllerSnapshot
from crash_report import decode_crash_report
from kafka import KafkaLog
from kvstore import KvStore
from storage import Store
from topic_manifest import (
    decode_topic_manifest,
    decode_topic_manifest_to_legacy_v1_json,
)
from tx_coordinator import TxLog

logger = logging.getLogger("viewer")

Configuration = namedtuple(
    "Configuration",
    [
        "path",
        "type",
        "topic",
        "binary_dump",
        "partition",
    ],
)


class SerializableGenerator(list):
    """Generator that is serializable by JSON"""

    def __init__(self, iterable):
        tmp_body = iter(iterable)
        try:
            self._head = iter([next(tmp_body)])
            self.append(tmp_body)
        except StopIteration:
            self._head = []

    def __iter__(self):
        return itertools.chain(self._head, *self[:1])


class OfflineLogViewer:
    def __init__(self, config: Configuration):
        self._config = config
        self.validate_path()

    def output_json(self, data):
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.flush()

    def build_store(self):
        return Store(self._config.path)

    def stream_json(self, data: Any, wrap_with_gen: bool = True) -> None:
        iter_json = json.JSONEncoder(indent=2).iterencode(
            SerializableGenerator(data) if wrap_with_gen else data
        )
        for j in iter_json:
            sys.stdout.write(j)
        sys.stdout.flush()

    def print_kv_store(self):
        # Map of partition ID to list of kvstore items
        result = {}
        store = self.build_store()
        for ntp in store.ntps:
            if ntp.nspace == "redpanda" and ntp.topic == "kvstore":
                logger.info(f"inspecting {ntp}")
                kv = KvStore(ntp)
                kv.decode()
                items = kv.items()

                result[ntp.partition] = items
        # Send JSON output to stdout in case caller wants to parse it, other
        # CLI output goes to stderr via logger
        self.output_json(result)

    def print_controller(self):
        store = self.build_store()
        for ntp in store.ntps:
            if ntp.nspace == "redpanda" and ntp.topic == "controller":
                ctrl = ControllerLog(ntp, self._config.binary_dump)
                self.stream_json(ctrl)

    def print_controller_snapshot(self):
        store = self.build_store()
        for ntp in store.ntps:
            if ntp.nspace == "redpanda" and ntp.topic == "controller":
                snap = ControllerSnapshot(ntp, bin_dump=self._config.binary_dump)
                self.stream_json(snap.to_dict().items())

    def print_topic_manifest(self, legacy_json=False):
        res = (
            decode_topic_manifest_to_legacy_v1_json(self._config.path)
            if legacy_json
            else decode_topic_manifest(self._config.path)
        )
        self.output_json(res)

    def print_kafka(self, headers_only: bool = False):
        store = self.build_store()
        for ntp in store.ntps:
            if ntp.nspace in ["kafka", "kafka_internal"]:
                if self._config.topic and ntp.topic != self._config.topic:
                    continue
                if self._should_skip_partition(ntp.partition):
                    continue
                logger.info(f"topic: {ntp.topic}, partition: {ntp.partition}")
                log = KafkaLog(ntp, headers_only=headers_only)
                self.stream_json(log)

    def print_groups(self):
        store = self.build_store()
        for ntp in store.ntps:
            if ntp.nspace == "kafka_internal" and ntp.topic == "group":
                l = GroupsLog(ntp)
                l.decode()
                self.output_json(l.records)

    def print_consumer_offsets(self, decode_all_batches: bool):
        store = self.build_store()
        logs = {}
        for ntp in store.ntps:
            if ntp.nspace == "kafka" and ntp.topic == "__consumer_offsets":
                if self._should_skip_partition(ntp.partition):
                    continue
                logs[str(ntp)] = SerializableGenerator(
                    OffsetsLog(ntp, decode_all_batches)
                )
        self.stream_json(logs, wrap_with_gen=False)

    def print_consumer_offsets_summary(self):
        store = self.build_store()
        summaries = {}
        for ntp in store.ntps:
            if ntp.nspace == "kafka" and ntp.topic == "__consumer_offsets":
                if self._should_skip_partition(ntp.partition):
                    continue
                summaries[str(ntp)] = ConsumerGroupsSummaryGenerator(
                    ntp
                ).build_summary()
        self.output_json(summaries)

    def print_tx_coordinator(self):
        store = self.build_store()
        for ntp in store.ntps:
            if ntp.nspace == "kafka_internal" and ntp.topic == "tx":
                l = TxLog(ntp)
                for result in l.decode():
                    self.output_json(result)

    def print_crash_report(self) -> None:
        """
        Parses either a specific crash report or the all crashes in the crash_reports directory
        """
        if not os.path.exists(self._config.path):
            logger.error(f"Crash file {self._config.path} does not exist")
            sys.exit(1)

        if os.path.isdir(self._config.path):
            crash_reports_dir = os.path.join(self._config.path, "crash_reports")
            if not os.path.isdir(crash_reports_dir):
                logger.error(
                    f"Could not find crash_reports directory in {self._config.path}"
                )
                sys.exit(1)
            crash_files = [
                f for f in os.listdir(crash_reports_dir) if f.endswith(".crash")
            ]
            if not crash_files:
                logger.error(f"No crash reports found in {crash_reports_dir}")
                sys.exit(1)
            res = {}
            for f in crash_files:
                try:
                    res[f] = decode_crash_report(join(crash_reports_dir, f))
                except:
                    # Ignore errors when parsing files - there may be empty ones that fail to be decoded
                    pass
        else:
            res = decode_crash_report(self._config.path)
        self.output_json(res)

    def validate_path(self):
        path = self._config.path
        if not os.path.exists(path):
            logger.error(f"Path doesn't exist {path}")
            sys.exit(1)

    def validate_topic(self):
        if self._config.topic:
            topic = join(self._config.path, "kafka", self._config.topic)
            if not os.path.exists(topic):
                logger.error(f"Topic {topic} doesn't exist")
                sys.exit(1)

    def validate_tx_coordinator(self):
        tx = join(self._config.path, "kafka_internal", "tx")
        if not os.path.exists(tx):
            logger.error(f"tx coordinator dir {tx} doesn't exist")
            sys.exit(1)

    def run_viewer(self):
        if self._config.type == "topic_manifest":
            self.print_topic_manifest(legacy_json=False)
        elif self._config.type == "topic_manifest_legacy":
            self.print_topic_manifest(legacy_json=True)
        elif self._config.type == "crash_report":
            self.print_crash_report()
        elif self._config.type == "kvstore":
            self.print_kv_store()
        elif self._config.type == "controller":
            self.print_controller()
        elif self._config.type == "controller_snapshot":
            self.print_controller_snapshot()
        elif self._config.type == "kafka":
            self.validate_topic()
            self.print_kafka(headers_only=True)
        elif self._config.type == "kafka_records":
            self.validate_topic()
            self.print_kafka(headers_only=False)
        elif self._config.type == "legacy-group":
            self.print_groups()
        elif self._config.type == "consumer_offsets":
            self.print_consumer_offsets(decode_all_batches=False)
        elif self._config.type == "consumer_offsets_all":
            self.print_consumer_offsets(decode_all_batches=True)
        elif self._config.type == "consumer_offsets_summary":
            self.print_consumer_offsets_summary()
        elif self._config.type == "tx_coordinator":
            self.validate_tx_coordinator()
            self.print_tx_coordinator()
        else:
            logger.error(f"Unknown type: {self._config.type}")
            sys.exit(1)

    def _should_skip_partition(self, partition_id):
        if self._config.partition is None:
            return False
        return self._config.partition != partition_id


def build_config(options):
    return Configuration(
        path=options.path,
        type=options.type,
        topic=options.topic,
        binary_dump=options.dump,
        partition=options.partition,
    )


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description="Redpanda log analyzer")
        parser.add_argument(
            "--path",
            type=str,
            help="Path to data dir of the node desired to be analyzed",
        )
        parser.add_argument(
            "--type",
            type=str,
            choices=[
                "controller",
                "kvstore",
                "kafka",
                "consumer_offsets",
                "legacy-group",
                "kafka_records",
                "tx_coordinator",
                "topic_manifest",
                "topic_manifest_legacy",
                "controller_snapshot",
                "crash_report",
                "consumer_offsets_all",
                "consumer_offsets_summary",
            ],
            required=True,
            help="operation to execute",
        )
        parser.add_argument(
            "--topic",
            type=str,
            required=False,
            help="for kafka type, if set, parse only this topic",
        )
        parser.add_argument("-v", "--verbose", action="store_true")
        parser.add_argument(
            "--dump",
            action="store_true",
            help="output binary dumps of keys and values being parsed",
        )
        parser.add_argument("--force", action="store_true", help="Deprecated")
        parser.add_argument(
            "--partition",
            type=int,
            required=False,
            default=None,
            help="if set, will parse only this partition",
        )
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    if options.verbose:
        logging.basicConfig(level="DEBUG")
    else:
        logging.basicConfig(level="INFO")

    logger.info(f"starting metadata viewer with options: {options}")
    viewer = OfflineLogViewer(build_config(options))
    viewer.run_viewer()


if __name__ == "__main__":
    main()
