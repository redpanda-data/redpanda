#!/bin/bash
#
# Docker-based build wrapper for macOS developers
#
# This script provides an interim solution for building Redpanda on macOS
# before the native macOS-to-Linux cross-compilation toolchains are available.
#
# Usage:
#   ./tools/macos/docker-build.sh [bazel-args]
#
# Examples:
#   ./tools/macos/docker-build.sh build //:redpanda
#   ./tools/macos/docker-build.sh test //src/v/storage:storage_test
#   ./tools/macos/docker-build.sh build --config=release //:redpanda
#

set -euo pipefail

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running. Please start Docker Desktop and try again."
  exit 1
fi

# Docker image with Redpanda build dependencies
# TODO: Update this to the official Redpanda build image
DOCKER_IMAGE="${REDPANDA_BUILD_IMAGE:-ubuntu:22.04}"

# Get the absolute path to the repository root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "Building Redpanda in Docker container..."
echo "Image: ${DOCKER_IMAGE}"
echo "Repository: ${REPO_ROOT}"
echo ""

# Run bazel in Docker
# - Mount the repository as /work
# - Run as current user to avoid permission issues
# - Pass through all arguments to bazel
docker run --rm -ti \
  -v "${REPO_ROOT}:/work:Z" \
  -w /work \
  -u "$(id -u):$(id -g)" \
  "${DOCKER_IMAGE}" \
  bazel "$@"

echo ""
echo "Build complete. Output is in bazel-bin/"
