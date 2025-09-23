#!/usr/bin/env bash

# Make a local ducktape venv. This installs ducktape and rptest packages to ease
# local development in an IDE (which will now be able to find the packages it
# expects) but does _not_ let you run tests locally outside of docker: for that
# you should still use the rp:run-ducktape-tests targets to run them inside docker.

# NOTE: if you get errors about conflicting dependencies, delete the venv and try again.

set -euo pipefail

# script expects cwd to be the parent dir of the script
# CC BY-SA 4.0 https://creativecommons.org/licenses/by-sa/4.0/ https://stackoverflow.com/a/246128
cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null

VENV="${VENV:-.venv}"

# Function to calculate SHA of input files
calculate_input_sha() {
  # Hash setup.py and type-checking/requirements.txt which contain dependency information
  sha256sum local_venv.sh setup.py type-checking/requirements.txt | sha256sum | cut -d' ' -f1
}

# Calculate current input SHA
CURRENT_SHA=$(calculate_input_sha)
SHA_FILE="${VENV}/.install_sha"

# Check if venv is up to date
if [[ -f $SHA_FILE && -f "$VENV/bin/activate" ]]; then
  EXISTING_SHA=$(cat "$SHA_FILE" 2>/dev/null || echo "")
  if [[ $CURRENT_SHA == "$EXISTING_SHA" ]]; then
    echo "Virtual environment is up to date (SHA: $CURRENT_SHA)"
    echo "To activate the existing venv, run:"
    echo "source $VENV/bin/activate"
    exit 0
  else
    echo "Input files have changed, rebuilding venv in ${VENV:=.venv}"
    echo "Previous SHA: $EXISTING_SHA"
    echo "Current SHA:  $CURRENT_SHA"
  fi
else
  echo "Creating new virtual environment in ${VENV}"
fi

python3 -m venv $VENV
. $VENV/bin/activate

# this should be closely aligned with the install step in ./docker/Dockerfile
python3 -m pip install -e .

# for type-checking/type-check.py
python3 -m pip install -r type-checking/requirements.txt

# Save the SHA of input files to mark this install as complete
echo "$CURRENT_SHA" >"$SHA_FILE"

echo "Install completed successfully (SHA: $CURRENT_SHA)"

echo "Everything installed, now activate the venv in your shell by running:"
echo "source $VENV/bin/activate"
