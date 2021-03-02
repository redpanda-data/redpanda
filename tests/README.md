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