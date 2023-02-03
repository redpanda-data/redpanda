#!/bin/bash

set -e

if [ ! -f /opt/remote/var/$1.pid ]; then
  echo "NO"
  exit 0
fi

pid=$(cat /opt/remote/var/$1.pid)

if [ $pid == "" ]; then
  echo "NO"
  exit 0
fi

if process=$(ps -p $pid -o comm=); then
  echo "YES"
  exit 0
fi

echo "NO"
exit 0
