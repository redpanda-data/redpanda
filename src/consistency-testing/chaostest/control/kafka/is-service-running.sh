#!/usr/bin/env bash

set -e

ps aux | egrep [k]afka.Kafka && echo "YES" || echo "NO"
