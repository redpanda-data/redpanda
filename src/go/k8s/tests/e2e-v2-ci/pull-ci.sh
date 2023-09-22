#!/bin/bash

# Pull CI here, some of these cannot be tested yet
git clone -n --depth=1 --filter=tree:0 https://github.com/redpanda-data/helm-charts.git
cd helm-charts
git sparse-checkout set --no-cone charts/redpanda/ci
git checkout

# Filter out files here



