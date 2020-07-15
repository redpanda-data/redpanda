#!/bin/env bash

function vtools_reinstall() {
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  mkdir -p "${tld}/build/venv/"
  python3 -mvenv "${tld}/build/venv/v/"
  source "${tld}/build/venv/v/bin/activate"
  pip install -e "${tld}/tools"
  deactivate
}

function bamboo_reinstall() {
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  mkdir -p "${tld}/build/bamboo/venv/"
  python3 -mvenv "${tld}/build/bamboo/venv/v/"
  source "${tld}/build/bamboo/venv/v/bin/activate"
  pip install -e "${tld}/src/bamboo"
  deactivate
}

function bamboo() {
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  bamboo_bin="${tld}/build/bamboo/venv/v/bin/bamboo"
  if [[ ! -e ${bamboo_bin} ]]; then
    bamboo_reinstall
  fi
  "${bamboo_bin}" $@
}

function vtools() {
  tld=$(git rev-parse --show-toplevel 2>/dev/null)
  vtools_bin="${tld}/build/bin/vtools"
  if [[ ! -e ${vtools_bin} ]]; then
    vtools_reinstall
  fi
  "${vtools_bin}" $@
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
          deploy_args+=("disks=2")
          ansible_vars=(--var 'with_raid=true')
          echo ''
        fi
        ;;
    esac
    vtools git verify
    vtools build go --targets rpk
    vtools build cpp --clang --build-type release --targets=redpanda
    rm -rf $tld/build/release/clang/dist/debian || true
    vtools build pkg --format deb --clang --build-type release
    local pkg_file=$(find $tld/build/release/clang/dist/debian/ -iname "redpanda_*dev*.deb" | head -n1)
    vtools deploy cluster ${deploy_args[@]} nodes=4
    vtools deploy ansible \
      --provider "$provider" \
      --playbook=$tld/infra/ansible/playbooks/provision-test-node.yml \
      ${ansible_vars[@]} \
      --var "rp_pkg=$pkg_file"

    vtools deploy ansible \
      --provider "$provider" \
      --playbook=$tld/infra/ansible/playbooks/redpanda-start.yml

    vtools deploy ansible \
      --provider "$provider" \
      --playbook=$tld/infra/ansible/playbooks/deploy-prometheus-grafana.yml
  )
}

function vtools_prometheus() {
  (
    set -x
    set -o errexit
    set -o pipefail
    tld=$(git rev-parse --show-toplevel 2>/dev/null)
    # ensure data directory
    vtools grafana create --url http://127.0.0.1:9644 2>/dev/null || true
    prometheus --config.file ${tld}/conf/local_multi_node/prometheus.yaml \
      --storage.tsdb.path ${tld}/build/pometheus_data/
  )
}
