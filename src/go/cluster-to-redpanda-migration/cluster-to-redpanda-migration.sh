#!/bin/bash

set -x
docker run -v $(pwd)/:/workspace/ localhost/cluster-to-redpanda-migration:latest $1
