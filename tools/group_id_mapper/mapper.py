#!/usr/bin/env python3
#
import jump
import xxhash


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(
            description='Redpanda Consumer Group Mapper')
        parser.add_argument('group_id',
                            type=str,
                            help='Id of the group to map')
        parser.add_argument(
            '--partition_count',
            type=int,
            help='Number of __consumer_offsets topic partitions',
            required=True)

        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    xx = xxhash.xxh64(seed=0)

    xx.update(options.group_id)

    partition_id = jump.hash(xx.intdigest(), options.partition_count)
    print(f"kafka/__consumer_offsets/{partition_id}")


if __name__ == '__main__':
    main()
