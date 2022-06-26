#!/bin/bash

set -e

mkdir -p /opt/remote/var
cd /opt/remote/var
nohup java -cp /opt/verifiers/verifiers.jar io.vectorized.reads_writes.App >/opt/remote/var/rw.log 2>&1 &
echo $! >/opt/remote/var/rw.pid
