#!/usr/bin/env bash
set -e

# This script regenerates the protobuf Python files used by ducktape tests.

# Enable globstar for ** pattern matching and nullglob to avoid errors on no matches
shopt -s globstar nullglob

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Set directories
OUT_DIR="$REPO_ROOT/tests/rptest/clients/admin"
GOOGLEAPIS_DIR="$(cd "$REPO_ROOT" && bazel info output_base)/external/googleapis+"

echo "Checking for uv..."
if ! command -v uv &>/dev/null; then
  echo "Error: uv not found in PATH"
  echo "Please install uv: https://docs.astral.sh/uv/getting-started/installation/"
  exit 1
fi

echo "Checking for googleapis directory..."
if [ ! -d "$GOOGLEAPIS_DIR" ]; then
  echo "Error: googleapis directory not found at $GOOGLEAPIS_DIR"
  echo "Please run: bazel fetch //proto/..."
  exit 1
fi

echo "Cleaning old generated files..."
rm -rf "$OUT_DIR/proto"/**/*.py{,i}

echo "Generating protobuf Python files..."
uv run --python 3.11 --no-project \
  --with 'grpcio-tools==1.71' \
  --with 'mypy-protobuf==3.7.0' \
  --with 'connect-python[compiler]==0.4.2' \
  -m grpc_tools.protoc \
  --python_out="$OUT_DIR" \
  --mypy_out="$OUT_DIR" \
  --connect_python_out="$OUT_DIR" \
  -I"$REPO_ROOT" \
  -I"$GOOGLEAPIS_DIR" \
  "$REPO_ROOT"/proto/redpanda/core/common/**/*.proto \
  "$REPO_ROOT"/proto/redpanda/core/pbgen/*.proto \
  "$REPO_ROOT"/proto/redpanda/core/admin/**/*.proto \
  "$REPO_ROOT"/proto/redpanda/core/rest/**/*.proto \
  "$GOOGLEAPIS_DIR"/google/api/field_behavior.proto \
  "$GOOGLEAPIS_DIR"/google/api/field_info.proto \
  "$GOOGLEAPIS_DIR"/google/api/resource.proto

echo "Fixing imports with protoletariat..."
uv run --python 3.11 --no-project \
  --with 'grpcio-tools==1.71' \
  --with protoletariat -- \
  protol \
  --create-package \
  --in-place \
  --module-suffixes "_pb2.py" \
  --module-suffixes "_pb2.pyi" \
  --module-suffixes "_pb2_connect.py" \
  --python-out="$OUT_DIR" \
  protoc \
  --proto-path "$REPO_ROOT" \
  --proto-path "$GOOGLEAPIS_DIR" \
  --protoc-path "python3 -m grpc_tools.protoc" \
  "$REPO_ROOT"/proto/redpanda/core/common/**/*.proto \
  "$REPO_ROOT"/proto/redpanda/core/pbgen/*.proto \
  "$REPO_ROOT"/proto/redpanda/core/admin/**/*.proto \
  "$REPO_ROOT"/proto/redpanda/core/rest/**/*.proto \
  "$GOOGLEAPIS_DIR"/google/api/field_behavior.proto \
  "$GOOGLEAPIS_DIR"/google/api/field_info.proto \
  "$GOOGLEAPIS_DIR"/google/api/resource.proto

echo "Done! Protobuf files generated in $OUT_DIR"
