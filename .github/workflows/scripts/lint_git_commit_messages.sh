#!/bin/bash
set -e

base=${1}

echo Hello

git log ${base}...

exit 1
