#!/usr/bin/env bash

set -e

ps aux | egrep bin/[k]velldb | awk '{print $2}' | xargs kill -9
