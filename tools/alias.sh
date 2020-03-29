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
  SHA1="$(git rev-parse --short HEAD)"
  TAG="NA"

  (
    cd $tld &&
      gcloud builds submit \
        --async \
        --project=redpandaci \
        --config tools/ci/gcbuild.yml \
        --substitutions="SHORT_SHA=$(git rev-parse --short HEAD),TAG_NAME=na,_COMPILER=$1,_BUILD_TYPE=$2"
  )
}

# same as above but execute local
function gcb_local() {
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

  # ensure no local image exists
  docker rmi gcr.io/redpandaci/builder-$1-$2:$SHA1 || true

  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  SHA1="$(git rev-parse --short HEAD)"
  TAG="NA"

  if [[ $tld != "/workspace" ]]; then
    echo " folder is /workspace"
    return 1
  fi

  (
    cd $tld &&
      cloud-build-local \
        --config tools/ci/gcbuild.yml \
        --bind-mount-source \
        --dryrun=false \
        --substitutions=SHORT_SHA=$SHA1,TAG_NAME="NA",_COMPILER=$1,_BUILD_TYPE=$2 \
        ./
  )
}

function vtools_dev_cluster() {
  set +ex
  local deploy_args=""
  local ansible_args=""
  local tld=$(git rev-parse --show-toplevel 2>/dev/null)
  if [[ $1 == "raid" ]]; then
    deploy_args="instance_type=m5ad.4xlarge"
    ansible_args="--var with_raid=true"
  fi
  vtools build go
  vtools build cpp --clang --build-type release
  rm $tld/build/release/clang/dist/rpm/RPMS/x86_64/redpanda-0.0-dev.x86_64.rpm || true
  vtools build pkg --format rpm --clang --build-type release
  vtools deploy cluster "${deploy_args}" nodes=3
  vtools deploy ansible --playbook=infra/ansible/playbooks/prepare-data-dir.yml
  vtools deploy ansible \
    --playbook=$tld/infra/ansible/playbooks/provision-test-node.yml \
    --var "rp_pkg=$tld/build/release/clang/dist/rpm/RPMS/x86_64/redpanda-0.0-dev.x86_64.rpm" "${ansible_args}"

  vtools deploy ansible \
    --playbook=$tld/infra/ansible/playbooks/redpanda-start.yml
  set -ex
}
