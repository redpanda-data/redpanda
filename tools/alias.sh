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
  (
    set -x
    set -o errexit
    set -o pipefail
    local deploy_args=()
    local ansible_vars=()
    local raid=false
    local provider="aws"
    local tld=$(git rev-parse --show-toplevel 2>/dev/null)
    while [ -n "$1" ]; do
      case "$1" in
        --raid)
          raid=true
          ;;
        --provider)
          provider="$2"
          shift
          ;;
      esac
      shift
    done

    deploy_args=(--provider "$provider")
    case "$provider" in
      aws)
        if [[ $raid == true ]]; then
          deploy_args+=("instance_type=m5ad.4xlarge")
          ansible_vars=(--var 'with_raid=true')
        fi
        ;;
      gcp)
        if [[ $raid == true ]]; then
          # TODO support RAID on GCP
        fi
        ;;
    esac
    vtools git verify
    vtools build go --targets rpk
    vtools build cpp --clang --build-type release --targets=redpanda
    rm $tld/build/release/clang/dist/rpm/RPMS/x86_64/redpanda-0.0-dev.x86_64.rpm || true
    vtools build pkg --format rpm --clang --build-type release
    vtools deploy cluster ${deploy_args[@]} nodes=3
    vtools deploy ansible \
      --provider "$provider" \
      --playbook=$tld/infra/ansible/playbooks/provision-test-node.yml \
      ${ansible_vars[@]} \
      --var "rp_pkg=$tld/build/release/clang/dist/rpm/RPMS/x86_64/redpanda-0.0-dev.x86_64.rpm"

    vtools deploy ansible \
      --provider "$provider" \
      --playbook=$tld/infra/ansible/playbooks/redpanda-start.yml
  )
}
