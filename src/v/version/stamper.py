#!/usr/bin/env python3

# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from string import Template
from pathlib import Path


def substitute(args):
    variables = {}
    for var_file in args.variables:
        vars_text = Path(var_file).read_text()
        for line in vars_text.splitlines():
            if not line.strip():
                continue
            (key, val) = line.split(" ", maxsplit=1)
            variables[key] = val
    tmpl = Template(Path(args.template).read_text())
    expanded = tmpl.substitute(**variables)
    Path(args.output).write_text(expanded)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="version stamper")
    parser.add_argument("--template", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--variables", nargs=2)
    substitute(parser.parse_args())


if __name__ == '__main__':
    main()
