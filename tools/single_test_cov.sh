#!/bin/bash

# single_test_cov.sh
# ==================
#
# Prerequisite: redpanda debug binaries built with RP_ENABLE_COV=true
#
# This script runs a single unit test with coverage profiling enabled
# and processes the output into an html report.
#
# It is useful for developers working on an individual test who would
# like to directly measure the coverage of the class they are testing.
#
# Usage (in your redpanda directory):
#  single_test_cov.sh vbuild/debug/clang/bin/<your test> [... extra args to test ...]

set -e

BINARY=$1
BINARY_BASENAME=$(basename $1)
PROFRAW=/tmp/${BINARY_BASENAME}.profraw
PROFDATA=/tmp/${BINARY_BASENAME}.profdata
HTML_DIR=/tmp/${BINARY_BASENAME}_coverage

export LLVM_PROFILE_FILE=$PROFRAW
echo "${@:2}"
$BINARY ${@:2}

vbuild/llvm/install/bin/llvm-profdata merge -sparse $PROFRAW -o $PROFDATA

vbuild/llvm/install/bin/llvm-cov show $BINARY -instr-profile=$PROFDATA -format=html -output-dir=$HTML_DIR

echo Wrote to ${HTML_DIR}/index.html
