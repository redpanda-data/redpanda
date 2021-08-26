#!/bin/bash
set -eux

# build wasm
JS_ROOT=../src/js/
JS_BUILD_ROOT=../build/node/v-output
rm -rf $JS_BUILD_ROOT
mkdir -p $JS_BUILD_ROOT/dist
npm --prefix $JS_ROOT run clean
npm --prefix $JS_ROOT install
npm run --prefix $JS_ROOT generate:serialization
npm run --prefix $JS_ROOT bundle
cp $JS_ROOT/dist/* $JS_BUILD_ROOT/dist
npm --prefix $JS_ROOT run clean

# build js sources
JS_BUILD_ROOT=../build/node/output
rm -rf $JS_BUILD_ROOT
mkdir -p $JS_BUILD_ROOT
npm --prefix $JS_ROOT install
npm run --prefix $JS_ROOT generate:serialization
npm run --prefix $JS_ROOT build:ts -- --project . --outDir $JS_BUILD_ROOT
npm run --prefix $JS_ROOT test
cp $JS_ROOT/build-package.json $JS_BUILD_ROOT/package.json
npm --prefix $JS_BUILD_ROOT install
npm --prefix ../src/js/ run publish:wasm-api -- --skip-publish

# build test node image
docker build -t vectorized/redpanda-test-node -f ./docker/Dockerfile .

# start compose
pushd docker/
docker-compose up --detach
popd

# run ducktape
mkdir -p ../build/ducktape/config/metadata
docker run --rm --privileged --ulimit nofile=65535:65535 \
  --name ducktape \
  --network redpanda-test \
  --volume "$PWD/../build/ducktape/config/:/root/.ducktape/" \
  --volume "$PWD/../build/ducktape/:/build/tests/" \
  --volume "$PWD/../build/:$PWD/../build" \
  --volume "$PWD:/root/tests/" \
  --volume "$PWD/tests/docker/ducktape_cluster.json:/cluster.json" \
  --volume "$PWD/../src/js/public:/root/js-public" \
  --entrypoint ducktape \
  --workdir /root \
  vectorized/redpanda-test-node \
  --cluster=ducktape.cluster.json.JsonCluster \
  --cluster-file=/cluster.json \
  --results-root=/build/tests/results \
  --max-parallel=1 \
  --globals="{\"rp_install_path_root\":\"$PWD/..build/\",\"redpanda_log_level\":\"$REDPANDA_LOG_LEVEL\",\"s3_bucket\":\"$S3_BUCKET\",\"s3_region\":\"$S3_REGION\",\"s3_access_key\":\"$S3_ACCESS_KEY\",\"s3_secret_key\":\"$S3_SECRET_KEY\",\"scale\":\"local\"}" \
  tests/rptest/test_suite_quick.yml

# stop compose cluster
pushd docker
docker-compose down
popd
