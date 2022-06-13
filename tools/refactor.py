#!/usr/bin/env python3

import argparse
from collections import defaultdict
import sys
import re
from pathlib import Path
from typing import DefaultDict, NamedTuple

p = argparse.ArgumentParser()

p.add_argument('--include', help='File patterns to include', nargs='+')
p.add_argument('--root', help='Root for the search', default='.', type=Path)

args = p.parse_args()

root = args.root

print('root is ', root)

if args.include is None:
    paths = ['**']
else:
    paths = ['**/' + i for i in args.include]

print('Input globs: ', paths)


class FileHander:
    def __init__(self, file: Path, match_regex: str, replace_template: str,
                 stats: dict[str, int]):
        self.file = file
        self.matcher = re.compile(match_regex)
        self.replace_template = replace_template
        self.matches = 0
        self.stats = stats

    def replace(self):
        lines: list[str] = []
        modified = False
        with self.file.open() as f:
            for line in f:
                m = self.matcher.search(line)
                if m is not None:
                    self.stats['matches'] += 1
                    replaced = m.expand(self.replace_template)
                    print(f'Original: {line}', end='')
                    print(f'New     : {replaced}')
                    lines.append(replaced)
                    modified = True
                else:
                    lines.append(line)
        if modified:
            with self.file.open('w') as f:
                f.writelines(lines)


count = 0

regex = r'using (?P<typename>[^ ]+) = named_type<(?P<raw>[^,]+)'
replace = 'struct \g<typename> : named_base<\g<raw>, \g<typename>> { using base::base; };\n'
stats: DefaultDict[str, int] = defaultdict(int)

exclude_files = ['named_type', 'convert.h']

for globstr in paths:
    for file in root.glob(globstr):
        count += 1
        if any(ex in str(file) for ex in exclude_files):
            continue
        handler = FileHander(file, regex, replace, stats)
        handler.replace()

print(f'Processed {count} files with {stats["matches"]} matches')
