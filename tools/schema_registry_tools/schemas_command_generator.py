#!/usr/bin/env python3

# Usage:
#
# Say you have a dump file of the _schemas topic, taken via
# 'rpk topic consume _schemas' and you want to restore it
# directly to the _schemas topic again.
#
# Run the following command that parses the dump file and
# generate runnable commands.
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
#    $ ./run.sh
#
# Example outputs:
# $ ./run.sh
# Produced to partition 0 at offset 0 with timestamp 1662516019353.
# Produced to partition 0 at offset 1 with timestamp 1662516020736.
# Produced to partition 0 at offset 2 with timestamp 1662516021788.
# Produced to partition 0 at offset 3 with timestamp 1662516022826.
# Done
#

import json
from pprint import pprint
import re
import types


def generate_records(j):
    # Extracting Key-Value pairs
    expected_offset = 0
    records = []
    for item in j:
        while item['offset'] > expected_offset:
            records.append({
                "key": '''{\"keytype\":\"NOOP\",\"magic\":0}''',
                "value": "",
                "offset": expected_offset
            })
            expected_offset += 1
        records.append({
            "key": item['key'],
            "value": item['value'],
            "offset": expected_offset
        })
        expected_offset += 1

    return records


def sort_and_validate_offset(j):
    # Sort by the offset
    j.sort(key=lambda x: x['offset'])
    # Validate if there's a duplicated offset in the offset chain
    previous_offset = -1
    for item in j:
        if item['offset'] == previous_offset:
            return f"The offset {item['offset']} is duplicated unexpectedly at key {item['key']}, hence exiting..."
        previous_offset = item['offset']

    return j


def run_tests():
    print('##### Test: offset not in order #####')
    not_in_order = '''[
{"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 0}, 
{"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 2},
{"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 1}
]'''
    expected = '''[{"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 0}, {"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 1}, {"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 2}]'''

    print(f"Original:\n{not_in_order}\n")
    j = json.loads(not_in_order)
    validated = sort_and_validate_offset(j)
    assert json.dumps(
        validated) == expected, 'Offset order validation has failed'
    print(f"Result, offset in order:\n{json.dumps(validated, indent=2,)}")
    print('\n')

    print('##### Test: offset duplicated #####')
    duplicated = '''[
  {"topic": "_schemas", "key": "{\\"keytype\\":\\"SCHEMA\\",\\"subject\\":\\"apple_value\\",\\"magic\\":0}", "value": "", "offset": 0},
  {"topic": "_schemas", "key": "{\\"keytype\\":\\"SCHEMA\\",\\"subject\\":\\"orange_value\\",\\\"magic\\":0}", "value": "", "offset": 1},
  {"topic": "_schemas", "key": "{\\"keytype\\":\\"SCHEMA\\",\\"subject\\":\\"grape_value\\",\\\"magic\\":0}", "value": "", "offset": 1}
]'''
    print(f"Original:\n{duplicated}")
    j = json.loads(duplicated)
    validated = sort_and_validate_offset(j)
    assert isinstance(validated,
                      str), 'Offset order duplication validation has failed'
    print(f"Result:\n{validated}")
    print('\n')

    print('##### Test: offset gap #####')
    gap = '''[
  {"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 0},
  {"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 1},
  {"topic": "_schemas", "key": "{\\"keytype\\":\\"NOOP\\",\\"magic\\":0}", "value": "", "offset": 3}
]'''
    expected = list(range(4))
    print(f"Original:\n{gap}")
    j = json.loads(gap)
    validated = generate_records(j)
    validated_list = list(i['offset'] for i in validated)
    assert validated_list == expected, 'Offset gap validation has failed'
    print(f"Result, 2 is filled:\n{validated}")
    print('\n')

    print('\nAll tests have passed!')


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(
            description='Redpanda Schema Registry _schemas Command Generator')
        parser.add_argument('path', type=str, help='Path to the file')
        parser.add_argument('-t',
                            '--test',
                            action='store_true',
                            help='Run tests')
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    if options.test:
        run_tests()
        exit(0)

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

    # Pre-check the dump
    j = sort_and_validate_offset(j)
    assert isinstance(j, list), j

    records = generate_records(j)

    # Formatting as a bash script
    cmd_all = "#!/usr/bin/env bash\nset -euo pipefail\n\necho '"
    for r in records:
        cmd_all += f"{r['key']}, {r['value']}\n"
    cmd_all += "\' | rpk topic produce _schemas --compression none -f \'%k, %v\\n\'\n"
    cmd_all += "echo Done"

    print(cmd_all)


if __name__ == '__main__':
    main()
