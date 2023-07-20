#!/bin/bash

set -x
docker run -v $(pwd)/:/workspace/ localhost/migration-cli:latest $1
