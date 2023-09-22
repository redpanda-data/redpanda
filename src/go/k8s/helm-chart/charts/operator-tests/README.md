# Operator-tests

This is a helm chart to produce valid kuttl tests from the ci folder of the redpanda helm chart.
The chart itself is not a test nor does it contain tests but only templates for creating these.


# Example usage
## create the directory structure
```
helm template redpanda operator-tests -s templates/rp.yaml | yq '"${TEST_DIR}/" + .metadata.name' | xargs mkdir -p
```

## create the test files
```
helm template redpanda operator-tests -s templates/rp.yaml | yq -s '"${TEST_DIR}/" + .metadata.name + "/00-create.yaml"'
```

## create the assertion files
```
helm template redpanda operator-tests -s templates/assertions.yaml | yq -s '"${TEST_DIR}/" + .metadata.name + "/00-assert.yaml"'
```

## create the next step files
```
helm template redpanda operator-tests -s templates/run-helm-tests.yaml | yq -s '"${TEST_DIR}/" + .metadata.name + "/01-helm-test.yaml"'
```