#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

files=($(find ~/.redpanda -regex ".*[0-9]+.wal$" -type f))
file_count=${#files[@]}
for i in "${files[@]}"; do
    expected=$(od -t x1 -N 4 -j 20 $i | grep fe | awk '{print $5$4}')
    if [[ "$expected" != "cafe" ]]; then
        echo "Incorrect file header"
        echo "$expected"
        exit 1
    fi
done
echo "${file_count} files verified"
