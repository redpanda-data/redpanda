#!/usr/bin/env bash

set -e

ps aux | egrep [io]faults.py | awk '{print $2}' | xargs -r kill -9
umount /mnt/vectorized/front || true
