#!/usr/bin/env bash
set -o errexit
set -o pipefail

# we assume bootstraph.sh lives in v/tools/
vroot="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "root of project: $vroot"

cd "${vroot}"

if [[ ! -e "${HOME}/.local/bin/vtools" ]]; then
  # install OS package deps
  sudo tools/install-deps.sh

  # install vtools
  pip install --user -r tools/requirements.txt
  pip install --user -e tools/
fi

# add build/bin/vtools to PATH
if [[ ${PATH} != *"${HOME}/.local/bin/"* ]]; then
  export PATH="${PATH}:${HOME}/.local/bin/"
fi

if [ ! -f "build/go/bin/go" ]; then
  vtools install go-compiler
fi

vtools install go-deps

vtools install clang

vtools build cpp
vtools build cpp --clang
vtools build cpp --build-type debug
vtools build cpp --build-type debug --clang

echo ""
echo "###############################################"
echo ""
echo "Successfully installed vtools and dev dependencies"
echo ""
echo "Execute vtools with:"
echo ""
echo "  ${HOME}/.local/bin/vtools"
echo ""
echo "Alternatively, add ${HOME}/.local/bin to PATH:"
echo ""
echo "  export PATH=\$PATH:${HOME}/.local/bin"
echo ""
echo "Or execute tools/bootstrap.sh on new terminals."
echo ""
echo "###############################################"
echo ""
echo "happy v-hacking!"
echo ""
