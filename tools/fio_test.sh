#!/bin/env bash
set -x

function write_only() {
  fio --name=seqwrite \
    --rw=write \
    --direct=1 \
    --ioengine=libaio \
    --bs=4k \
    --numjobs=2 \
    --size=6G \
    --runtime=600 \
    --group_reporting
}

function read_only() {
  fio --name=seqread \
    --rw=read \
    --direct=1 \
    --ioengine=libaio  --bs=4k \
    --numjobs=2 \
    --size=6G \
    --runtime=600 \
     --group_reporting
}

function read_write() {
  fio --name=randrw \
    --rw=randrw \
    --direct=1 \
    --ioengine=libaio \
    --bs=4k \
    --numjobs=2 --rwmixread=90 \
    --size=6G \
    --runtime=600 \
    --group_reporting
}

if [[ $1 == "ro" ]]; then
  read_only
elif [[ $1 == "wo" ]]; then
  write_only
elif [[ $1 == "rw" ]]; then
  read_write
else
  echo "invalid command - options are ro,wo,rw (read-only, write-only, read-write)"
  exit 1
fi
