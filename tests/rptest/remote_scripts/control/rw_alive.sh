#!/bin/bash

set -e

if [ ! -f /opt/remote/var/rw.pid ]; then
  echo "NO"
  exit 0
fi

pid=$(cat /opt/remote/var/rw.pid)

if [ $pid == "" ]; then
  echo "NO"
  exit 0
fi

if process=$(ps -p $pid -o comm=); then
  if [ $process == "java" ]; then
    echo "YES"
    exit 0
  fi
fi

echo "NO"
exit 0
