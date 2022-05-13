#!/usr/bin/env bash

set -euo pipefail

dl_materialized() {
  INSTALL_DIR=$1
  git -C "$INSTALL_DIR" clone git@github.com:MaterializeInc/materialize.git
  cd "$INSTALL_DIR/materialize"
}

edit_rp_service() {
  SERVICE_FILE_PY="$1/materialize/misc/python/materialize/mzcompose/services.py"
  TARGET_VERSION=$2

  # Set RP version in the service file
  sed -i "/version: str = /s@\"[^\"]*\"@\"$TARGET_VERSION\"@" "$SERVICE_FILE_PY"

  if [[ $TARGET_VERSION == *"-"* && $TARGET_VERSION != "v21"* ]]; then
    echo "Version $TARGET_VERSION requires unstable repo"
    sed -i'' "s@vectorized/redpanda@vectorized/redpanda-unstable@" "$SERVICE_FILE_PY"
  fi
}

run_test_suite() {
  cd "$1/materialize/test/testdrive"
  ./mzcompose run default --redpanda

  ./mzcompose down -v
}

if [ $# -ne 2 ]; then
  echo "Usage: $0 <install dir> <rp version>"
  echo "Example: $0 /home/user/Documents v22.1.3-rc1"
else
  INSTALL_DIR=$1
  RP_VERSION=$2

  dl_materialized "$INSTALL_DIR"

  edit_rp_service "$INSTALL_DIR" "$RP_VERSION"

  run_test_suite "$INSTALL_DIR"

  # Clean up by removing the entire
  # materialize repo from disk
  cd "$INSTALL_DIR"
  rm -rf materialize
fi
