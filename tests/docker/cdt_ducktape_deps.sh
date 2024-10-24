#!/bin/bash

set -ex

REMOTE_FILES_PATH="/tmp" # https://github.com/redpanda-data/vtools/blob/dev/qa/image/packer/aws-ubuntu.pkr.hcl#L79

"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/java-dev-tools"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/tool-pkgs"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/omb"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/kafka-tools"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/librdkafka"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/kcat"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/golang"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/kaf"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/rust"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/client-swarm"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/sarama-examples"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/franz-bench"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/kcl"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/kgo-verifier"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/addr2line"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/kafka-streams-examples"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/arroyo"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/byoc-mock"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/flink"

mkdir -p /opt/redpanda-tests/ /opt/remote /opt/scripts
pushd "$REMOTE_FILES_PATH"
cp -r tests/* /opt/redpanda-tests/
cp -r tests/rptest/remote_scripts/* /opt/remote
cp -r tools/offline_log_viewer /opt/scripts
cp -r tools/rp_storage_tool /
popd

"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/java-verifiers"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/golang-test-clients"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/rp-storage-tool"
"$REMOTE_FILES_PATH/tests/docker/ducktape-deps/teleport"
