#!/usr/bin/env bash

set -e

ps -C redpanda >/dev/null && echo "YES" || echo "NO"
