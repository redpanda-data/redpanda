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
FROM public.ecr.aws/docker/library/ubuntu:kinetic-20220830

ENV TZ="UTC" \
    DEBIAN_FRONTEND=noninteractive

COPY --chown=0:0 tests/docker/ducktape-deps.sh /opt/ducktape-deps.sh
RUN chmod +x /opt/ducktape-deps.sh

# Install dependencies of Java client builds.
RUN /opt/ducktape-deps.sh install_java_client_deps && \
    rm -rf /var/lib/apt/lists/*

# Install kafka streams examples.  This is a very slow step (tens of minutes), doing
# many maven dependency downloads without any parallelism.  To avoid re-running it
# on unrelated changes in other steps, this step is as early on the Dockerfile as possible.
RUN /opt/ducktape-deps.sh install_kafka_streams_examples

# Install the OMB tool
RUN /opt/ducktape-deps.sh install_omb

# Install Protobuf for test clients / workloads
RUN mkdir -p /opt/redpanda-tests
COPY --chown=0:0 tests/protobuf /opt/redpanda-tests/protobuf

# Install Kafka Java test clients / workloads
RUN mkdir -p /opt/redpanda-tests/java
COPY --chown=0:0 tests/java/e2e-verifiers /opt/redpanda-tests/java/e2e-verifiers
COPY --chown=0:0 tests/java/verifiers /opt/redpanda-tests/java/verifiers
COPY --chown=0:0 tests/java/kafka-serde /opt/redpanda-tests/java/kafka-serde
RUN /opt/ducktape-deps.sh install_java_test_clients

# - install distro-packaged depedencies
# - allow user env variables in ssh sessions
# - install dependencies of 'rpk debug' system scan
RUN /opt/ducktape-deps.sh install_system_deps && \
    rm -rf /var/lib/apt/lists/* && \
    echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config && \
    echo 'UsePAM yes' >> /etc/ssh/sshd_config && \
    echo 'root soft nofile 65535' >> /etc/security/limits.conf && \
    echo 'root hard nofile 65535' >> /etc/security/limits.conf

# install kafka binary dependencies, librdkafka dev, kcat and kaf tools
ENV KAFKA_MIRROR="https://s3-us-west-2.amazonaws.com/kafka-packages"
RUN /opt/ducktape-deps.sh install_kafka_tools && \
    /opt/ducktape-deps.sh install_librdkafka && \
    /opt/ducktape-deps.sh install_kcat

# install go
RUN /opt/ducktape-deps.sh install_golang
ENV PATH="${PATH}:/usr/local/go/bin"

# Install `kaf`
RUN /opt/ducktape-deps.sh install_kaf

ENV PATH="${PATH}:/usr/local/go/bin"
# Compile and install rust-based workload generator.
# Install & remove compiler in the same step, to avoid bulking
# out the resulting container image.
RUN /opt/ducktape-deps.sh install_rust_tools /root

# Install golang dependencies for kafka clients such as sarama
RUN /opt/ducktape-deps.sh install_sarama_examples

# Install our in-tree go tests
COPY --chown=0:0 tests/go /opt/redpanda-tests/go
RUN /opt/ducktape-deps.sh install_go_test_clients

# Install franz-go
RUN /opt/ducktape-deps.sh install_franz_bench

RUN /opt/ducktape-deps.sh install_kcl

# Install the kgo-verifier tool
RUN /opt/ducktape-deps.sh install_kgo_verifier

# Expose port 8080 for any http examples within clients
EXPOSE 8080

# copy ssh keys
COPY --chown=0:0 tests/docker/ssh /root/.ssh

# install python dependencies and rptest package.
# rptest package installed in editable mode so it can be overridden.
# passes --force so system pip packages can be updated
COPY --chown=0:0 tests/setup.py /root/tests/
RUN python3 -m pip install --upgrade --force pip && \
    python3 -m pip install --force --no-cache-dir -e /root/tests/

# Seastar addrress to line utility
RUN apt update && \
    apt install -y \
      file && \
    rm -rf /var/lib/apt/lists/*

RUN /opt/ducktape-deps.sh install_arroyo

RUN /opt/ducktape-deps.sh install_addr2line

RUN mkdir -p /opt/scripts/offline_log_viewer
COPY --chown=0:0 tools/offline_log_viewer /opt/scripts/offline_log_viewer

RUN mkdir -p /opt/scripts/consumer_offsets_recovery
COPY --chown=0:0 tools/consumer_offsets_recovery /opt/scripts/consumer_offsets_recovery
RUN python3 -m pip install --force --no-cache-dir -r  /opt/scripts/consumer_offsets_recovery/requirements.txt

# Install remote_scripts
RUN mkdir -p /opt/remote
COPY --chown=0:0 tests/rptest/remote_scripts /opt/remote

CMD service ssh start && tail -f /dev/null
