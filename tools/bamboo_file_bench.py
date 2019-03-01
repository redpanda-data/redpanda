#!/usr/bin/env python3
import sys
import os
import logging
import argparse
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import shell
import log


def generate_options():
    parser = argparse.ArgumentParser(
        description='bamboo bench helper. one bench per line in file')
    parser.add_argument(
        '--log',
        type=str,
        default='DEBUG',
        help='info,debug, type log levels. i.e: --log=debug')
    parser.add_argument(
        '--file',
        type=str,
        help='file that is new line delimited. i.e: --file=foo.json')
    parser.add_argument(
        '--batchsize',
        type=int,
        help='batch size per request. i.e: --batchsize=130')
    parser.add_argument(
        '--totalcpus',
        type=int,
        help='one test per cpu, spread evenly. i.e: --totalcpus=32')
    parser.add_argument('--topic', type=str, help='topic. i.e: --topic=clicks')
    parser.add_argument(
        '--ip', type=str, help='ip of receiving host. i.e: --ip=127.0.0.1')
    parser.add_argument(
        '--bamboo',
        type=str,
        help='full path of program. i.e: --bamboo=/usr/local/bin/bamboo')
    parser.add_argument(
        '--partitions',
        type=int,
        help='partition count for the topic on creation. i.e: --partitions=32')

    return parser


def read_file(filename):
    with open(filename, 'r') as f:
        return f.readlines()


def launch_background_test(bamboo_program, testnumber, line, ip, batch_size,
                           topic, partitions, cpuset, memory):
    tpl = Template("""
    mkdir -p $testnumber && \
    cd $testnumber && \
    $bamboo --key-size 10 \
    --ip $ip \
    --value-size $line_length  \
    --write-batch-size $batch_size \
    --qps 1 \
    --concurrency 1 \
    --seconds-duration 1 \
    --cpuset $cpuset \
    --topic $topic \
    --partitions $partitions \
    --rw-balance 1.0 -m$memory 2&>1  > $testnumber.log
    """.strip())
    cmd = tpl.substitute(
        bamboo=bamboo_program,
        ip=ip,
        testnumber=testnumber,
        line_length=len(line),
        batch_size=batch_size,
        cpuset=cpuset,
        topic=topic,
        partitions=partitions,
        memory=memory,
    )
    logger.info("Launching command in background: %s" % cmd)
    shell.run_subprocess("%s &" % cmd)


def main():
    parser = generate_options()
    options, program_options = parser.parse_known_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)

    lines = read_file(options.file)
    cpu_balancer_rr = 0
    for line in lines:
        logger.info("Launching test: %d", cpu_balancer_rr)
        memory = 4096 + (len(line) * options.batchsize)
        launch_background_test(options.bamboo, cpu_balancer_rr, line,
                               options.ip, options.batchsize, options.topic,
                               options.partitions,
                               cpu_balancer_rr % options.totalcpus, memory)
        cpu_balancer_rr = cpu_balancer_rr + 1


if __name__ == '__main__':
    main()
