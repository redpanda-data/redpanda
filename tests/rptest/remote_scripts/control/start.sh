#!/bin/bash

set -e

mkdir -p /opt/remote/var
cd /opt/remote/var
nohup bash -c "$2" >/opt/remote/var/$1.log 2>&1 &
echo $! >/opt/remote/var/$1.pid
