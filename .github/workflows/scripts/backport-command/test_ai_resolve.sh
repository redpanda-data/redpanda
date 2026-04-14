#!/usr/bin/env bash
# Local test for ai_resolve.py against a synthetic cherry-pick conflict.
#
# Usage:
#   ANTHROPIC_API_KEY=... bash test_ai_resolve.sh
#
# The test constructs a minimal git repo, stages a cherry-pick conflict that
# mirrors the class of mechanical conflicts the script is designed to handle
# (context drift + nearby line change), then validates that the resolved file
# is clean and contains no model reasoning or markdown artifacts.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

cd "$TMP"
git init -q
git config user.email "test@test.com"
git config user.name "Test"

# Base: a simple C++ function
cat >foo.cc <<'EOF'
// Copyright Redpanda Data, Inc.
void process(int a, int b) {
    log("processing");
    return a + b;
}
EOF
git add foo.cc
git commit -qm "base: add process()"

# Release branch diverges: adds a third parameter
git checkout -qb release
cat >foo.cc <<'EOF'
// Copyright Redpanda Data, Inc.
void process(int a, int b, int c) {
    log("processing");
    return a + b + c;
}
EOF
git add foo.cc
git commit -qm "release: add parameter c to process()"

# Dev branch: adds a log message in the same area (mechanical context drift)
git checkout -q -
cat >foo.cc <<'EOF'
// Copyright Redpanda Data, Inc.
void process(int a, int b) {
    log("processing", a, b);
    return a + b;
}
EOF
git add foo.cc
COMMIT=$(git commit -qm "dev: improve log message" && git rev-parse HEAD)

# Cherry-pick onto release — will conflict around the function signature
git checkout -q release
git cherry-pick -x "$COMMIT" >/dev/null 2>&1 || true

echo "Conflict staged. Running ai_resolve.py..."

RESOLVED_OUT=$(mktemp)
DIFFICULTY_OUT=$(mktemp)
export RESOLVED_FILES_OUT="$RESOLVED_OUT"
export DIFFICULTY_OUT
export BACKPORT_COMMITS="$COMMIT"
export BACKPORT_BRANCH="release"

if ! uv run "$SCRIPT_DIR/ai_resolve.py"; then
  echo "FAIL: ai_resolve.py exited non-zero (model could not resolve)"
  exit 1
fi

echo ""
echo "=== Resolved file ==="
cat foo.cc
echo ""

# Validation
fail=0

if grep -qE "^<<<<<<|^=======$|^>>>>>>>" foo.cc; then
  echo "FAIL: conflict markers remain in resolved file"
  fail=1
fi

if grep -qE '^```' foo.cc; then
  echo "FAIL: markdown code fences found in resolved file"
  fail=1
fi

if head -1 foo.cc | grep -qiE "^(looking|here is|the resolved|i'll|let me|this file)"; then
  echo "FAIL: model reasoning leaked into first line of resolved file"
  fail=1
fi

if ! grep -q "Copyright" foo.cc; then
  echo "FAIL: copyright header missing — file may be truncated or replaced"
  fail=1
fi

if [[ $fail -eq 0 ]]; then
  echo "PASS: resolved cleanly (difficulty: $(cat "$DIFFICULTY_OUT"))"
fi

exit $fail
