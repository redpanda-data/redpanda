#!/usr/bin/env python3

import argparse
import subprocess
import sys

parser = argparse.ArgumentParser(
    description="retry a command a number of times until it succeeds")
parser.add_argument('--retries',
                    type=int,
                    default=3,
                    help='number of times to retry the command')
parser.add_argument('cmd', nargs='+', help='the command to execute')
args = parser.parse_args()

exitcode = 0

for _ in range(0, args.retries):
    completed_proc = subprocess.run(args.cmd)
    exitcode = completed_proc.returncode
    if exitcode == 0:
        break

sys.exit(exitcode)
