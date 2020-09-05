#!/usr/bin/env bash

set -e

ps -C kvelldb >/dev/null && echo "YES" || echo "NO"
