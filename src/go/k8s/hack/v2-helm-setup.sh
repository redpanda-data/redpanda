#!/bin/bash

set -xe

TEST_DIR="${1-tests/e2e-v2-helm}"

# to make this idempotent, need to clean prior runs
rm -rf $TEST_DIR || true

echo "Will be creating tests in ${TEST_DIR}"

# Pull CI here, some of these cannot be tested yet
echo "Pulling git repository"

git clone -n --depth=1 --filter=tree:0 https://github.com/redpanda-data/helm-charts.git
pushd helm-charts
git sparse-checkout set --no-cone charts/redpanda/ci
git checkout

echo "Filter files that cannot be tested"

popd

# Removing files that require environment variables available only in helm chart Github Action execution.
rm helm-charts/charts/redpanda/ci/9*.yaml || true
rm helm-charts/charts/redpanda/ci/9*.yaml.tpl || true

# Removing files that are intended for cloud providers
rm helm-charts/charts/redpanda/ci/2*.yaml.tpl || true

# create files directory as it may not exist
rm -rf test-generator-chart/files/ || true
mkdir -p test-generator-chart/files/

# Remove the preamble from remaining CI file
for FILE in helm-charts/charts/redpanda/ci/*.yaml; do
  echo $FILE
  tail -n +16 $FILE >test-generator-chart/files/$(basename $FILE)
done

# Remove the charts dir now since this is not needed anymore

rm -rf helm-charts

# Remove the following CI files as they cannot pass right now
# CRD for servicemonitor missing
rm test-generator-chart/files/15-*.yaml || true
# CRD for servicemonitor missing
rm test-generator-chart/files/14-*.yaml || true
# Needs external-tls secret, tests lb
rm test-generator-chart/files/13-*.yaml || true
# Needs sasl secret updates to test
rm test-generator-chart/files/11-*.yaml || true
# Needs external-tls secret, tests nodeport
rm test-generator-chart/files/12-*.yaml || true
# remove origin type tests
rm test-generator-chart/files/18-*.yaml || true
# remove origin type tests
rm test-generator-chart/files/19-*.yaml || true

echo "Creating template files for testing"

# clean and recreate if it exists already
rm -rf temp_tests || true
mkdir -p temp_tests

# create the directory structure
helm template redpanda test-generator-chart -s templates/rp.yaml | yq '"temp_tests/" + .metadata.name' | xargs mkdir -p

# create the test files
helm template redpanda test-generator-chart -s templates/rp.yaml | yq -s '"temp_tests/" + .metadata.name + "/00-create.yaml"'

# create the assertion files
helm template redpanda test-generator-chart -s templates/assertions.yaml | yq -s '"temp_tests/" + .metadata.name + "/00-assert.yaml"'

# create the next step files
helm template redpanda test-generator-chart -s templates/run-helm-tests.yaml | yq -s '"temp_tests/" + .metadata.name + "/01-helm-test.yaml"'

# remove the #--- from comments, this is on purpose
OS=$(uname -s)
if [ "$OS" == "Darwin" ]; then
  grep -rl "#---" temp_tests | xargs sed -i "" -e 's/#---/---/g'
else
  grep -rl "#---" temp_tests | xargs sed -i -e 's/#---/---/g'
fi

mv temp_tests $TEST_DIR

rm -rf temp_tests
