#!/usr/bin/env bash

set -e

ps aux | egrep [k]afkakv | awk '{print $2}' | xargs kill -9
