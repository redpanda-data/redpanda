#!/usr/bin/env python3

# Usage:
#
# Say you have a dump file of the _schemas topic, taken via
# 'rpk topic consume _schemas' and you want to restore it
# directly to the _schemas topic again.
#
# Run the following command that parses the dump file and
# generate runnable commands. This will fail without generating
# the commands when a seq is higher than the corresponding offset.
#
# $ ./schemas_command_generator.py schemas-dump.txt > run.sh
#
# Here's how to restore the records:
#
# 1. Delete the existing _schemas topic
# 2. Restart Redpanda
# 3. Create the topic, configure the '-r' option based on the cluster size:
#    $ rpk topic create _schemas -r 3 -c cleanup.policy=compact -c compression.type=none
# 4. Run the generated script above
#    $ chmod 755 run.sh
#    $ sh run.sh
#
# Example outputs:
# $ sh run.sh
# Produced to partition 0 at offset 0 with timestamp 1662516019353.
# Produced to partition 0 at offset 1 with timestamp 1662516020736.
# Produced to partition 0 at offset 2 with timestamp 1662516021788.
# Produced to partition 0 at offset 3 with timestamp 1662516022826.
# Done
#

import json
from pprint import pprint
import re


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(
            description='Redpanda Schema Registry _schemas Command Generator')
        parser.add_argument('path', type=str, help='Path to the file')
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()

    # Formatting as a single json
    tmp = '['
    with open(options.path) as f:
        for l in f.read().splitlines():
            if l.startswith('}'):
                tmp += ('},')
            else:
                tmp += (l)
    tmp = re.sub('},$', '}]', tmp)
    j = json.loads(tmp)

    # Generating rpk topic create commands
    cmd_all = ''
    cmd_all += f"#!/bin/bash\n\n"
    for c, i in enumerate(j):
        d = json.loads(i['key'])
        if d['seq'] > i['offset']:
            cmd_all = f"The seq {d['seq']} is unexpectedly higher than the \
offset {i['offset']} at key {i['key']}. \nThat is it's likely broken, hence exiting...."

            break
        cmd_all += f"echo '{i['value']}' | rpk topic produce _schemas --compression none -k '{i['key']}'\n"
        if c == len(j) - 1:
            cmd_all += "echo Done"
        else:
            cmd_all += "sleep 1s\n"

    print(cmd_all)


if __name__ == '__main__':
    main()
