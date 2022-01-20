# TESTING

## E2E Tests

For e2e tests we use [KUTTL](https://kuttl.dev/). It starts a kind cluster and runs tests in `e2e` folder.

To run all e2e tests:
```
make e2e-tests
```

To run a single e2e test
```
TEST_NAME=testname make e2e-tests
```

Useful flags for debugging that can be set via `KUTTL_TEST_FLAGS` env var

```
--skip-cluster-delete
If set, do not delete the mocked control plane or kind cluster.

--skip-delete
If set, do not delete resources created during tests (helpful for debugging test failures, implies --skip-cluster-delete).
To run a single e2e test and keep the kind cluster and the kutt
```

For example:

```
KUTTL_TEST_FLAGS="--skip-cluster-delete --skip-delete" \ 
  TEST_NAME=create-topic-given-cm-secret-client-auth \
  make e2e-tests
```