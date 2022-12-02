import os
from pathlib import Path

import subprocess
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.admin.new_topic import NewTopic
from kafka import TopicPartition
from kafka import OffsetAndMetadata
from jproperties import Properties
import logging

logger = logging.getLogger('cg-recovery-tool')


def read_offsets(admin: KafkaAdminClient):

    groups_dict = {}
    groups = admin.list_consumer_groups()
    for g, _ in groups:
        logger.info(f"reading group '{g}' offsets")
        offsets = admin.list_consumer_group_offsets(group_id=g)
        groups_dict[g] = offsets

    return groups_dict


def read_config(path):
    logger.debug(f"reading configuration from {path}")
    cfg = Properties()
    with open(path, 'rb') as f:
        cfg.load(f)
    cfg_dict = {}
    for k, v in cfg.items():
        cfg_dict[k] = v.data

    return cfg_dict


def seek_to_file(path, cfg, dry_run):

    logger.debug(f"reading offsets file: {path}")
    offsets = {}
    with open(path, 'r') as f:
        group = ""
        consumer = None
        for i, l in enumerate(f):
            if i == 0:
                group = l.strip()
                if not dry_run:
                    consumer = KafkaConsumer(group_id=group, **cfg)
                logger.info(f"seeking group {group} to file {path}")
                continue
            if l == "":
                continue

            topic, partition, offset = tuple([p.strip() for p in l.split(',')])
            logger.debug(
                f"group: {group} partition: {topic}/{partition} offset: {offset}"
            )
            tp = TopicPartition(topic=topic, partition=int(partition))

            offsets[tp] = OffsetAndMetadata(offset=int(offset), metadata='')
    if not dry_run:
        consumer.commit(offsets=offsets)


def offset_file(group):
    return f"{group}.offsets"


def seek_all(config, path, dry_run):
    logger.info(f"reading offsets from: {path}")
    for _, _, files in os.walk(path):
        for f in files:
            seek_to_file(Path(path).joinpath(f), config, dry_run)


def query_offsets(offsets_path, cfg):
    admin = KafkaAdminClient(**cfg)

    current_offsets = read_offsets(admin)
    path = Path(offsets_path)
    logger.info(f"storing offsets in {offsets_path}")
    path.mkdir(exist_ok=True)
    for group_id, offsets in current_offsets.items():
        with open(path.joinpath(offset_file(group_id)), 'w') as f:
            f.write(f"{group_id}\n")
            for tp, md in offsets.items():
                topic, partition = tp
                offset, _ = md
                f.write(f"{topic},{partition},{offset}\n")


def recreate_topic(partitions, replication_factor, cfg):
    admin = KafkaAdminClient(**cfg)
    logger.info('removing __consumer_offsets topic')

    admin.delete_topics(["__consumer_offsets"])

    new_topic = NewTopic(name="__consumer_offsets",
                         num_partitions=partitions,
                         topic_configs={
                             "cleanup.policy": "compact",
                             "retention.bytes": -1,
                             "retention.ms": -1,
                         },
                         replication_factor=replication_factor)
    logger.info('recreating __consumer_offsets topic')
    admin.create_topics(new_topics=[new_topic])


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(
            description='Redpanda group recovery tool')
        parser.add_argument('--cfg',
                            type=str,
                            help='config file path',
                            required=True)
        parser.add_argument('-o',
                            "--offsets-path",
                            type=str,
                            default="./offsets")
        parser.add_argument('-v', "--verbose", action="store_true")
        parser.add_argument('-s', "--skip-query", action="store_true")
        parser.add_argument('-p', "--target-partitions", type=int, default=16)
        parser.add_argument('-e', "--execute", action="store_true")
        parser.add_argument('-r', "--replication-factor", type=int, default=3)

        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    logging.basicConfig(
        format="%(asctime)s %(name)-20s %(levelname)-8s %(message)s")

    if options.verbose:
        logging.basicConfig(level="DEBUG")
        logger.setLevel(level="DEBUG")
    else:
        logging.basicConfig(level="ERROR")
        logger.setLevel(level="INFO")

    logger.info(f"starting group recovery tool with options: {options}")
    cfg = read_config(options.cfg)

    logger.info(f"configuration: {cfg}")

    if not options.skip_query:
        query_offsets(options.offsets_path, cfg)

    if options.execute:
        recreate_topic(options.target_partitions, options.replication_factor,
                       cfg)

    seek_all(cfg, options.offsets_path, dry_run=not options.execute)


if __name__ == '__main__':
    main()
