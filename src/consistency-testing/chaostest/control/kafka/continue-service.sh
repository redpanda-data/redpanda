#!/usr/bin/env bash

set -e

ps aux | egrep [Q]uorumPeerMain | awk '{print $2}' | xargs -r kill -CONT
ps aux | egrep [k]afka.Kafka | awk '{print $2}' | xargs -r kill -CONT
