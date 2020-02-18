#!/bin/env bash

function vtools_reinstall() {
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  mkdir -p build/venv/
  python3 -mvenv "${tld}/build/venv/v/"
  source "${tld}/build/venv/v/bin/activate"
  pip install -e tools/
}

function vtools() {
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  vtools_bin="${tld}/build/bin/vtools"
  if [[ -e ${vtools_bin} ]]; then
    "${vtools_bin}" $@
  else
    vtools_reinstall
  fi
}

function bootstrap() {
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  bootstrap_bin="${tld}/tools/bootstrap.sh"
  if [[ -e ${bootstrap_bin} ]]; then
    "${bootstrap_bin}"
  else
    echo "[error: cannot find tools/bootstrap.sh]"
  fi
}
