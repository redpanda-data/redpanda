#!/usr/bin/env python3
import sys
import os
import logging
import argparse
import tempfile
import random
import string
import shutil
import uuid
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')
fmt_string = '%(levelname)s:%(asctime)s %(filename)s:%(lineno)d] %(message)s'
logging.basicConfig(format=fmt_string)
formatter = logging.Formatter(fmt_string)
for h in logging.getLogger().handlers:
    h.setFormatter(formatter)

PARTITIONS = 96
REPLICATION = 3
TOPIC = "sfo"


def _exec_command(cmd):
    logger.info(cmd)
    os.execle("/bin/bash", "/bin/bash", "-exc", cmd, os.environ)


def _client_id():
    cid = uuid.uuid4().hex
    return "vectorized-%s" % cid


def produce(kdir, servers):
    cmd = Template("""$root/bin/kafka-run-class.sh \
    org.apache.kafka.tools.ProducerPerformance \
    --record-size $record_size \
    --topic $topic \
    --num-records $record_count \
    --throughput $throughput \
    --producer-props "acks=$acks" \
    "client.id=$client_id" \
    bootstrap.servers=$servers \
    batch.size=$batch_size \
    buffer.memory=$memory""").substitute(
        root=kdir,
        record_size=1024,
        topic=TOPIC,
        record_count=1 << 31,
        throughput=-1,
        acks=1,
        client_id=_client_id(),
        servers=servers,
        batch_size=81960,
        memory=1024 * 1024,
    )
    _exec_command(cmd)


def consume(kdir, servers):
    cmd = Template("""$root/bin/kafka-consumer-perf-test.sh \
    --broker-list=$servers \
    --fetch-size=$memory \
    --timeout=$timeout \
    --messages=$record_count \
    --group="$client_id" \
    --topic=$topic \
    --threads $thread_count""").substitute(
        root=kdir,
        topic=TOPIC,
        timeout=1000,
        record_count=1 << 31,
        thread_count=1,
        client_id=_client_id(),
        servers=servers,
        memory=1024 * 1024,
    )
    _exec_command(cmd)


def create(kdir, servers):
    cmd = Template("""$root/bin/kafka-topics.sh --bootstrap-server $servers \
    --create --topic $topic --partitions $partitions \
    --replication-factor $replication_factor""").substitute(
        root=kdir,
        servers=servers,
        partitions=PARTITIONS,
        topic=TOPIC,
        replication_factor=REPLICATION)
    _exec_command(cmd)


def main():
    def generate_options():
        parser = argparse.ArgumentParser(description='test helper for cmake')
        parser.add_argument('--kafka-dir',
                            type=str,
                            help='kafka binary dist directory')
        parser.add_argument('--kafka-type',
                            type=str,
                            help='one of: producer, consumer, create')
        parser.add_argument(
            '--servers',
            type=str,
            help='comma-separated list of the server IPs (with ports)')
        parser.add_argument(
            '--log',
            type=str,
            default='DEBUG',
            help='info,debug, type log levels. i.e: --log=debug')
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()
    logger.setLevel(getattr(logging, options.log.upper()))
    logger.info("%s *args=%s" % (options, program_options))

    if not options.kafka_dir or not options.servers:
        parser.print_help()
        exit(1)

    if options.kafka_type == "produce":
        produce(options.kafka_dir, options.servers)
    elif options.kafka_type == "consume":
        consume(options.kafka_dir)
    elif options.kafka_type == "create":
        create(options.kafka_dir, options.servers)

    parser.print_help()
    exit(1)


if __name__ == '__main__':
    main()
