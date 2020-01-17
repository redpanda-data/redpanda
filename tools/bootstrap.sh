#!/usr/bin/env bash
#
# this whole bootstrap.sh script is idempotent so it can safely be re-executed 
# from the top and only missing packages (or intermediary objects) will be 
# build/installed
set -o errexit
set -o nounset
set -o pipefail

# we assume bootstraph.sh lives in v/tools/
vroot="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "root of project: $vroot"

cd "${vroot}"

# install OS package deps
sudo tools/install-deps.sh

# install vtools
mkdir -p build/venv/
python3 -mvenv build/venv/v/
source build/venv/v/bin/activate
pip install -r tools/requirements.txt
pip install -e tools/
mkdir -p build/bin/
ln -sf "${vroot}/build/venv/v/bin/vtools" build/bin/

if [[ ! -e "${vroot}/compile_commands.json" ]]; then
  ln -sf "${vroot}/build/debug/clang/compile_commands.json" "${vroot}/compile_commands.json"
fi

# check gcc version
gcc_installed="$(gcc -dumpversion)"
gcc_required="9"
if (( $gcc_installed < $gcc_required )); then
  echo "Expecting GCC 9, found $gcc_installed"
  exit 1
fi

# add build/bin/ to PATH
if [[ ${PATH} != *"${vroot}/build/bin/"* ]]; then
  PATH="${PATH}:${vroot}/build/bin/"
fi

vtools install go-compiler
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
echo "  ${vroot}/build/bin/vtools"
echo ""
echo "Alternatively, add ${vroot}/build/bin to PATH:"
echo ""
echo "  export PATH=\${PATH}:${vroot}/build/bin"
echo ""
echo "###############################################"
echo ""
echo "happy v-hacking!"
echo ""
