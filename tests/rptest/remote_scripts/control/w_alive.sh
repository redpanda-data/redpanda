#!/bin/bash

set -e

if [ ! -f /opt/remote/var/w.pid ]; then
  echo "NO"
  exit 0
fi

pid=$(cat /opt/remote/var/w.pid)

if [ $pid == "" ]; then
  echo "NO"
  exit 0
fi

if process=$(ps -p $pid -o comm=); then
  if [ $process == "python3" ]; then
    echo "YES"
    exit 0
  fi
fi

echo "NO"
exit 0
