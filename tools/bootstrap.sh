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

echo ""
echo "###############################################"
echo ""
echo "successfully installed vtools in '${vroot}/build/bin/'"
echo ""
echo "###############################################"
echo ""

"${vroot}/build/bin/vtools" --help

echo ""
echo "###############################################"
echo ""
echo "happy vhacking!"
echo ""
