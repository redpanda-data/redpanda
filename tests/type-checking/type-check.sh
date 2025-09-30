#!/usr/bin/env bash

set -euo pipefail

image_name=python-type-checking
tag=redpanda-data/$image_name

# Get the directory containing this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the parent directory (tests)
TESTS_DIR="$(dirname "$SCRIPT_DIR")"

cd $TESTS_DIR

# Build the Docker image
echo "Building Docker image $tag..."
docker build "$TESTS_DIR" -t $tag -f type-checking/Dockerfile ${TARGET:+--target=$TARGET}

# docker image ls $tag

# Run the container with the tests directory mounted
# echo "Running type checker in Docker container..."
docker run --rm -t \
  -v "$TESTS_DIR:$TESTS_DIR" $tag --no-venv --tests-root "$TESTS_DIR" "$@"
