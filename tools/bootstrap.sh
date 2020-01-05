#!/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

## we assume bootstraph.sh lives in v/tools/
vroot="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Root of project: $vroot"

if [[ ! -e "${vroot}/build/bin/vtools" ]]; then
    # install OS package deps
    sudo "${vroot}/tools/install-deps.sh"

    # install tox (via virtualenv)
    pip install --user tox
    PATH="${HOME}/.local/bin:${PATH}:${vroot}/build/bin"

    # build vtools
    tox -c tools/
fi
# install go dependencies
vtools install go-compiler
vtools install go-deps

# install clang
vtools install clang

# install external dependencies for all build types
vtools install cpp-deps
vtools install cpp-deps --clang
vtools install cpp-deps --build-type debug
vtools install cpp-deps --build-type debug --clang

echo ""
echo "###############################################"
echo ""
echo "successfully installed dev dependencies"
echo ""
echo "vtools command available as 'vtools'"
echo ""
echo "###############################################"
echo ""

vtools --help

echo ""
echo "###############################################"
echo ""
echo "happy vhacking!"
echo ""
