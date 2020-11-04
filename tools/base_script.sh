#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

function log() {
  local __ts=$(date --utc +'%Y-%m-%d %H:%M:%S.%N %Z')
  local __fmt="[${__ts}] [${FUNCNAME[1]}:${BASH_LINENO[0]}]"
  echo -e "${__fmt} $@"
}

function fatal() {
  log "\e[1;31m[FATAL] $@ \e[0m"
  exit 1
}

function debug() {
  log "\e[1;34m[DEBUG] $@ \e[0m"
}

function error() {
  log "\e[1;31m[ERROR] $@ \e[0m"
}
