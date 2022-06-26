#!/bin/bash

set -e

if [ ! -f /opt/remote/var/w.pid ]; then
  exit 0
fi

pid=$(cat /opt/remote/var/w.pid)

if [ $pid == "" ]; then
  exit 0
fi

if ps -p $pid; then
  kill -9 $pid
fi

rm /opt/remote/var/w.pid
