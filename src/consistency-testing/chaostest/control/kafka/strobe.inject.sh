#!/usr/bin/env bash

set -e

echo "inject" >>/home/admin/strobe.inject.log

curl 127.0.0.1:9094/inject?delta_ms=$1\&period_ms=$2
