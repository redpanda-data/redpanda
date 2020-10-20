#!/usr/bin/env bash

set -e

ps aux | egrep [s]trobe.py | awk '{print $2}' | xargs -r kill -9
