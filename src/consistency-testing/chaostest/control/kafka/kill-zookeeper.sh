#!/usr/bin/env bash

set -e

ps aux | egrep [Q]uorumPeerMain | awk '{print $2}' | xargs -r kill -9
