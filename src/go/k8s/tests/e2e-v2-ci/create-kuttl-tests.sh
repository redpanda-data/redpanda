#!/bin/bash

mkdir -p tests

# create the directory structure
helm template redpanda operator-tests -s templates/rp.yaml | yq '"tests/" + .metadata.name' | xargs mkdir -p

# create the test files
helm template redpanda operator-tests -s templates/rp.yaml | yq -s '"tests/" + .metadata.name + "/00-create.yaml"'

# create the assertion files
helm template redpanda operator-tests -s templates/assertions.yaml | yq -s '"tests/" + .metadata.name + "/00-assert.yaml"'

# create the next step files
helm template redpanda operator-tests -s templates/run-helm-tests.yaml | yq -s '"tests/" + .metadata.name + "/01-helm-test.yaml"'

grep -rl "#---" ./tests | xargs sed -i "" -e 's/#---/---/g'

