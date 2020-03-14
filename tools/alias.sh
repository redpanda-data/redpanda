#!/bin/env bash

function vtools_reinstall() {
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  mkdir -p "${tld}/build/venv/"
  python3 -mvenv "${tld}/build/venv/v/"
  source "${tld}/build/venv/v/bin/activate"
  pip install -e "${tld}/tools"
  deactivate
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

# gcb_trigger <gcc|clang> <release|debug>
function gcb_trigger() {

  if [[ $# != 2 ]]; then
    echo "expecting two arguments"
    return 1
  fi

  if [[ $1 != "gcc" ]] && [[ $1 != "clang" ]]; then
    echo "expecting first argument to be 'clang' or 'gcc'"
    return 1
  fi

  if [[ $2 != "release" ]] && [[ $2 != "debug" ]]; then
    echo "expecting second argument to be 'release' or 'debug'"
    return 1
  fi

  tld=$(git rev-parse --show-toplevel 2>/dev/null)

  (cd $tld && \
  gcloud builds submit \
    --async \
    --project=redpandaci \
    --config tools/ci/gcbuild.yml \
    --substitutions="SHORT_SHA=$(git rev-parse --short HEAD),TAG_NAME=na,_COMPILER=$1,_BUILD_TYPE=$2" \
  )
}
