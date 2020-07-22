# bamboo

## Usage

Start a redpanda cluster

```
#/bin/bash
bamboo session init --sname leia
bamboo session add-node --sname leia --package /path/to/redpanda.tar.gz --node-id 6
bamboo session --sname --destroy
```

Run a test

```bash
bamboo session run-test --sname leia --path /path/to/test
```

## Writing tests

The bamboo test runner assumes that each test is contained in a directory with a
Dockerfile whose entry point accepts well-known parameters that define the test
environment.

Parameters:

* `--brokers`: a comma-separated list of `broker:port`
* `--namespace`: a unique context string (e.g. topic prefix)

The test runner will automatically build the docker image and run a container
with the above parameters passed in as the container command.
