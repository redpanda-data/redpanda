#!/bin/bash

set -e

mkdir -p /opt/remote/var
cd /opt/remote/var
nohup python3 /opt/remote/w_workload.py >/opt/remote/var/w.log 2>&1 &
echo $! >/opt/remote/var/w.pid
