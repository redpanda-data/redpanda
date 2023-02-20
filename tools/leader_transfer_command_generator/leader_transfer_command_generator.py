#!/usr/bin/env python3
#
# Prerequisite:
#  - Get partitions.txt first via 'curl -s http://localhost:9644/v1/partitions > partitions.txt'
#  - Ensure the target nodes have a replica of the target partition
#  - Run the generated curl commands on the SOURCE host, or pod
#
# Command Usage:
# ./leader_transfer_command_generator.py \
#    -f FILE \
#    --source SOURCE \
#    --targets TARGET \
#    -t <topics> \
#    -p <partitions> \
#    -c <cores>
#
# Example:
#  ./leader_transfer_command_generator.py -f /tmp/testdata.txt --source 1 --targets 2,3 \
#     -t test,test2,test3 \
#     -p 0,1,2,3,4,5 \
#     -c 2
#  transfer leadership for topics ['test', 'test2', 'test3'] / partitions [0, 1, 2, 3, 4, 5] on core [2] from 1 to [2, 3]
#  curl -XPOST http://localhost:9644/v1/partitions/kafka/test/1/transfer_leadership?target=2
#  curl -XPOST http://localhost:9644/v1/partitions/kafka/test3/4/transfer_leadership?target=3
#


def main():
    import argparse
    import itertools
    import json
    import sys

    def generate_options():
        parser = argparse.ArgumentParser(
            description='Redpanda Granular Leader Transfer Command Generator')
        parser.add_argument('-f',
                            '--file',
                            required=True,
                            type=str,
                            help='path to partitions file')
        parser.add_argument('--source',
                            required=True,
                            type=str,
                            help='a single source node id')
        parser.add_argument('--targets',
                            required=True,
                            type=str,
                            help='comma separated target node ids')
        parser.add_argument('-t',
                            type=str,
                            help='comma separated user topics (Default: all)')
        parser.add_argument('-p',
                            type=str,
                            help='comma separated partitions (Default: all)')
        parser.add_argument(
            '-c',
            type=str,
            help=
            'comma separated core ids to where current leader is stuck (Default: all)'
        )
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    topics, partitions, cores = None, None, None
    source, targets = None, None

    with open(options.file) as f:
        all_partitions = json.load(f)

    all_partitions.sort(key=lambda x: x['topic'])

    # Preparation
    if options.t is not None:
        topics = options.t.split(',')

    if options.p is not None:
        if options.t is None:
            print("partition id has to be specified along with topic name")
            exit(1)
        else:
            partitions = [int(i) for i in options.p.split(',')]

    if options.c is not None:
        cores = [int(i) for i in options.c.split(',')]

    source = int(options.source)
    targets = [int(i) for i in options.targets.split(',')]

    # Parsing all the partitions

    # No option is specified
    if topics is None and partitions is None and cores is None:
        print(
            f"transfer leadership for all topics from node {source} to node(s) {targets}",
            file=sys.stderr)
        for p, t in zip(all_partitions, itertools.cycle(targets)):
            if p['leader'] == source:
                cmd = f"curl -XPOST http://localhost:9644/v1/partitions/{p['ns']}/{p['topic']}/{p['partition_id']}/transfer_leadership?target={t}"
                print(cmd)
        exit(0)

    # 'topics' is specified
    if topics is not None and partitions is None and cores is None:
        print(
            f"transfer leadership for topic(s) {topics} from node {source} to node(s) {targets}",
            file=sys.stderr)
        for p, t in zip(all_partitions, itertools.cycle(targets)):
            if p['topic'] in topics and p['leader'] == source:
                cmd = f"curl -XPOST http://localhost:9644/v1/partitions/{p['ns']}/{p['topic']}/{p['partition_id']}/transfer_leadership?target={t}"
                print(cmd)
        exit(0)

    # 'topics' and 'partitions' are specified but 'cores' is not
    if topics is not None and partitions is not None and cores is None:
        print(
            f"transfer leadership for topic(s) {topics} / partition(s) {partitions} from node {source} to node(s) {targets}",
            file=sys.stderr)
        for p, t in zip(all_partitions, itertools.cycle(targets)):
            if p['topic'] in topics and p['partition_id'] in partitions and p[
                    'leader'] == source:
                cmd = f"curl -XPOST http://localhost:9644/v1/partitions/{p['ns']}/{p['topic']}/{p['partition_id']}/transfer_leadership?target={t}"
                print(cmd)
        exit(0)

    # 'topics', 'partitions', and 'cores' are specified
    if topics is not None and partitions is not None and cores is not None:
        print(
            f"transfer leadership for topic(s) {topics} / partition(s) {partitions} on core(s) {cores} from node {source} to node(s) {targets}",
            file=sys.stderr)
        for p, t in zip(all_partitions, itertools.cycle(targets)):
            if p['topic'] in topics and p['partition_id'] in partitions and p[
                    'core'] in cores and p['leader'] == source:
                cmd = f"curl -XPOST http://localhost:9644/v1/partitions/{p['ns']}/{p['topic']}/{p['partition_id']}/transfer_leadership?target={t}"
                print(cmd)
        exit(0)

    # 'topics' and 'cores' are specified, but 'partition' is not
    if topics is not None and partitions is None and cores is not None:
        print(
            f"transfer leadership for topic(s) {topics} on core(s) {cores} from node {source} to node(s) {targets}",
            file=sys.stderr)
        for p, t in zip(all_partitions, itertools.cycle(targets)):
            if p['topic'] in topics and p['core'] in cores and p[
                    'leader'] == source:
                cmd = f"curl -XPOST http://localhost:9644/v1/partitions/{p['ns']}/{p['topic']}/{p['partition_id']}/transfer_leadership?target={t}"
                print(cmd)
        exit(0)

    # only 'cores' is specified
    if topics is None and partitions is None and cores is not None:
        print(
            f"transfer leadership for all partitions on core(s) {cores} from node {source} to node(s) {targets}",
            file=sys.stderr)
        for p, t in zip(all_partitions, itertools.cycle(targets)):
            if p['core'] in cores and p['leader'] == source:
                cmd = f"curl -XPOST http://localhost:9644/v1/partitions/{p['ns']}/{p['topic']}/{p['partition_id']}/transfer_leadership?target={t}"
                print(cmd)
        exit(0)


if __name__ == '__main__':
    main()
