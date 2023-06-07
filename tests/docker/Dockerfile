# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM public.ecr.aws/docker/library/ubuntu:kinetic-20230412 as base

ENV TZ="UTC" \
    DEBIAN_FRONTEND=noninteractive

RUN mkdir -p /opt/redpanda-tests
COPY --chown=0:0 tests/protobuf /opt/redpanda-tests/protobuf

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/java-dev-tools /
COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/protobuf /
RUN /java-dev-tools && \
    rm /java-dev-tools && \
    rm -rf /var/lib/apt/lists/*

# - install distro-packaged depedencies
# - install dependencies of 'rpk debug' system scan
COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/tool-pkgs /
RUN /tool-pkgs && \
    rm /tool-pkgs && \
    rm -rf /var/lib/apt/lists/*

#################################

FROM base as kafka-streams-examples

# Install kafka streams examples.  This is a very slow step (tens of minutes), doing
# many maven dependency downloads without any parallelism.  To avoid re-running it
# on unrelated changes in other steps, this step is as early on the Dockerfile as possible.
COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/kafka-streams-examples /
RUN /kafka-streams-examples && \
    rm /kafka-streams-examples

#################################

FROM base as omb

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/omb /
RUN /omb && \
    rm /omb

#################################

FROM base as java-verifiers

COPY --chown=0:0 tests/java/e2e-verifiers /opt/redpanda-tests/java/e2e-verifiers
COPY --chown=0:0 tests/java/verifiers /opt/redpanda-tests/java/verifiers
COPY --chown=0:0 tests/java/kafka-serde /opt/redpanda-tests/java/kafka-serde
COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/java-verifiers /
RUN /java-verifiers && \
    rm /java-verifiers

#################################

FROM base as kafka-tools

ENV KAFKA_MIRROR="https://s3-us-west-2.amazonaws.com/kafka-packages"
COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/kafka-tools /
RUN /kafka-tools && \
    rm /kafka-tools

#################################

FROM base as librdkafka

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/librdkafka /
RUN /librdkafka && \
    rm /librdkafka

#################################

FROM librdkafka as kcat

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/kcat /
RUN /kcat && \
    rm /kcat

#################################

FROM base as golang

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/golang /
RUN /golang && \
    rm /golang
ENV PATH="${PATH}:/usr/local/go/bin"

#################################

FROM golang as kaf

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/kaf /
RUN /kaf && \
    rm /kaf

#################################

FROM base as rust
COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/rust /
RUN /rust && \
    rm /rust
ENV PATH="${PATH}:/root/.cargo/bin"

#################################

FROM rust as client-swarm

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/client-swarm /
RUN /client-swarm && \
    rm /client-swarm

#################################

FROM rust as rp-storage-tool

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/rp-storage-tool /
COPY --chown=0:0 --chmod=0755 tools/rp_storage_tool /rp_storage_tool
RUN /rp-storage-tool && \
    rm -rf /rp_storage_tool

#################################

FROM golang as sarama-examples

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/sarama-examples /
RUN /sarama-examples && \
    rm /sarama-examples

#################################

FROM golang as golang-test-clients

COPY --chown=0:0 --chmod=0755 tests/go /opt/redpanda-tests/go
COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/golang-test-clients /
RUN /golang-test-clients && \
    rm /golang-test-clients

#################################

FROM golang as franz-bench

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/franz-bench /
RUN /franz-bench && \
    rm /franz-bench

#################################

FROM golang as kcl

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/kcl /
RUN /kcl && \
    rm /kcl

#################################

FROM golang as kgo-verifier

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/kgo-verifier /
RUN /kgo-verifier && \
    rm /kgo-verifier

#################################

FROM base as k8s

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/k8s /
RUN /k8s && \
    rm /k8s

#################################

FROM librdkafka as final

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/teleport /
RUN /teleport && \
    rm /teleport && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/arroyo /
RUN /arroyo && \
    rm /arroyo

RUN mkdir -p /opt/scripts/consumer_offsets_recovery
COPY --chown=0:0 --chmod=0755 tools/consumer_offsets_recovery /opt/scripts/consumer_offsets_recovery
RUN python3 -m pip install --force --no-cache-dir -r  /opt/scripts/consumer_offsets_recovery/requirements.txt

# install python dependencies and rptest package.
# rptest package installed in editable mode so it can be overridden.
# passes --force so system pip packages can be updated
COPY --chown=0:0 --chmod=0755 tests/setup.py /root/tests/
RUN python3 -m pip install --upgrade --force pip && \
    python3 -m pip install --force --no-cache-dir -e /root/tests/

# seastar addrress to line utility depends on 'file' pkg
RUN apt update && \
    apt install -y file && \
    rm -rf /var/lib/apt/lists/*
COPY --chown=0:0 --chmod=0755 tests/docker/ducktape-deps/addr2line /
RUN /addr2line && \
    rm /addr2line

# copy offline_log_viewer
RUN mkdir -p /opt/scripts/offline_log_viewer
COPY --chown=0:0 --chmod=0755 tools/offline_log_viewer /opt/scripts/offline_log_viewer

# copy remote_scripts
RUN mkdir -p /opt/remote
COPY --chown=0:0 --chmod=0755 tests/rptest/remote_scripts /opt/remote

# expose port 8080 for any http examples within clients
EXPOSE 8080

# copy known ssh keys
COPY --chown=0:0 --chmod=0755 tests/docker/ssh /root/.ssh

# allow user env variables in ssh sessions and increase nofile limit
RUN echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config && \
    echo 'UsePAM yes' >> /etc/ssh/sshd_config && \
    echo 'root soft nofile 65535' >> /etc/security/limits.conf && \
    echo 'root hard nofile 65535' >> /etc/security/limits.conf

# copy from other images
COPY --from=kafka-streams-examples /opt/kafka-streams-examples /opt/kafka-streams-examples
COPY --from=kafka-streams-examples /root/.m2 /root/.m2
COPY --from=omb /opt/openmessaging-benchmark /opt/openmessaging-benchmark
COPY --from=omb /root/.m2 /root/.m2
COPY --from=java-verifiers /opt/redpanda-tests/java /opt/redpanda-tests/java
COPY --from=java-verifiers /opt/verifiers /opt/verifiers
COPY --from=java-verifiers /opt/kafka-serde /opt/kafka-serde
COPY --from=java-verifiers /root/.m2 /root/.m2
COPY --from=kafka-tools /opt /opt
COPY --from=kcat /usr/local/bin/kcat /usr/local/bin/
COPY --from=sarama-examples /opt/sarama /opt/sarama
COPY --from=golang-test-clients /opt/redpanda-tests/go /opt/redpanda-tests/go
COPY --from=client-swarm /usr/local/bin/client-swarm /usr/local/bin/
COPY --from=rp-storage-tool /usr/local/bin/rp-storage-tool /usr/local/bin/
COPY --from=franz-bench /opt/franz-go/examples/bench /opt/franz-go/examples/bench
COPY --from=k8s /usr/local/bin/kubectl /usr/local/bin/helm /usr/local/bin/
COPY --from=kaf /usr/local/bin/kaf /usr/local/bin/
COPY --from=kcl /usr/local/bin/kcl /usr/local/bin/
COPY --from=kgo-verifier /opt/kgo-verifier /opt/kgo-verifier

RUN ldconfig

# define sshd as the entrypoint
CMD service ssh start && tail -f /dev/null
