#!/bin/bash

set -eu

files=($(find /var/log/pods/ -type f | grep "$1" | grep -v "configurator"))

for item in ${files[*]}; do
  echo "# $item"
  tail --lines=+$(cat $item | grep -n -m 1 "$2" | cut -d":" -f1) $item
  echo "# End of log"
done
