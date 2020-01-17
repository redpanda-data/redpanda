#!/bin/env bash

function vtools() {
  local curr=$(pwd)
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  vtools_bin="${tld}/build/bin/vtools"
  if [[ -e ${vtools_bin} ]]; then
    "${vtools_bin}" $@
  else
    echo "[error: please run bootstrap]"
  fi
}

function bootstrap() {
  local curr=$(pwd)
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  bootstrap_bin="${tld}/tools/bootstrap.sh"
  if [[ -e ${bootstrap_bin} ]]; then
    "${bootstrap_bin}"
  else
    echo "[error: cannot find tools/bootstrap.sh]"
  fi
}
