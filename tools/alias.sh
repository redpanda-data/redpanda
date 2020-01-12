#!/bin/env bash

#function alias
function vtools {
    local curr=$(pwd)
    tld=$(git rev-parse --show-toplevel 2> /dev/null)
    vtools_bin="${tld}/build/bin/vtools"
    if [[ -e "${vtools_bin}" ]]; then
        "${vtools_bin}" $@
    else
        echo "[error: please tools/bootstrap.sh]"
    fi
}
