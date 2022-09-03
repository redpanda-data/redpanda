#!/usr/bin/env python3
#
# Examples:
# Say you have a segment file like: /var/lib/redpanda/data/kafka/test-si3/0_105/0-1-v1.log
# run the command like below:
#
# $ ./cloud_storage_name_mapper.py kafka/test-si3/0_105/0-1-v1.log
#
# and the expected output looks like:
#
# manifest path:       00000000/meta/kafka/test-si3/0_105/manifest.json
# topic manifest path: 60000000/meta/kafka/test-si3/topic_manifest.json
# segment path:        924dec9f/kafka/test-si3/0_105/0-1-v1.log.*
#

import xxhash
import sys


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(
            description='Redpanda Tiered Storage Remote Write Naming Mapper')
        parser.add_argument('path',
                            type=str,
                            help='Path to the file, starting with \'kafka/\'')

        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    if not options.path.startswith('kafka'):
        print(f"The path should start with 'kafka'")
        sys.exit(1)

    p = options.path.split('/')
    x = xxhash.xxh32()

    path = p[0] + '/' + p[1] + '/' + p[2]
    x.update(path.encode('ascii'))
    manifest_hash = x.hexdigest()[0] + '0000000'
    print(
        f"manifest path:       {manifest_hash}/meta/{p[0]}/{p[1]}/{p[2]}/manifest.json"
    )

    path = p[0] + '/' + p[1]
    x.reset()
    x.update(path.encode('ascii'))
    topic_manifest_hash = x.hexdigest()[0] + '0000000'
    print(
        f"topic manifest path: {topic_manifest_hash}/meta/{p[0]}/{p[1]}/topic_manifest.json"
    )

    x.reset()
    x.update(options.path.encode('ascii'))
    hash = x.hexdigest()
    print(f"segment path:        {hash}/{options.path}.*")


if __name__ == '__main__':
    main()
