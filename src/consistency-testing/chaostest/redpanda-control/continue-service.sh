#!/usr/bin/env bash

set -e

ps aux | egrep [r]edpanda/bin | awk '{print $2}' | xargs kill -CONT
