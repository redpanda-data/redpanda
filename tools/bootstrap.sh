#!/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

## we assume bootstraph.sh lives in v/tools/
vroot="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Root of project: $vroot"

# install OS package deps
sudo "${vroot}/tools/install-deps.sh"

# install tox (via virtualenv)
pip install --user tox
PATH=$HOME/.local/bin:$PATH

# build vtools
tox -c tools/

# install go dependencies
"${vroot}/build/bin/vtools install go-compiler"
"${vroot}/build/bin/vtools install go-deps"

# install clang
"${vroot}/build/bin/vtools install clang"

# install external dependencies for all build types
"${vroot}/build/bin/vtools install cpp-deps"
"${vroot}/build/bin/vtools install cpp-deps --clang"
"${vroot}/build/bin/vtools install cpp-deps --build-type debug"
"${vroot}/build/bin/vtools install cpp-deps --build-type debug --clang"

echo ""
echo "###############################################"
echo ""
echo "successfully installed dev dependencies"
echo ""
echo "vtools command available as '${vroot}/build/bin/vtools'"
echo ""
echo "###############################################"
echo ""

"${vroot}/build/bin/vtools" --help

echo ""
echo "###############################################"
echo ""
echo "happy vhacking!"
echo ""
