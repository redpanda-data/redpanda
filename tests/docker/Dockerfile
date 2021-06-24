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
FROM ubuntu:groovy-20210325

ENV TZ="UTC" \
    DEBIAN_FRONTEND=noninteractive

# - install openssh, jvm and kafka cli tools
# - allow user env variables in ssh sessions
RUN apt update && \
    apt install -y \
      build-essential \
      curl \
      default-jdk \
      cmake \
      git \
      golang \
      iptables \
      libatomic1 \
      libyajl-dev \
      libsasl2-dev \
      libssl-dev \
      maven \
      nodejs \
      npm \
      openssh-server \
      python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# install kafka binary dependencies, librdkafka dev, kafkacat and kaf tools
ENV KAFKA_MIRROR="https://s3-us-west-2.amazonaws.com/kafka-packages"
RUN mkdir -p "/opt/kafka-2.3.1" && chmod a+rw /opt/kafka-2.3.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.3.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.3.1" && \
    mkdir -p "/opt/kafka-2.4.1" && chmod a+rw /opt/kafka-2.4.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.4.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.4.1" && \
    mkdir -p "/opt/kafka-2.5.0" && chmod a+rw /opt/kafka-2.5.0 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.5.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.5.0" && \
    mkdir -p "/opt/kafka-2.7.0" && chmod a+rw /opt/kafka-2.7.0 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.7.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.7.0" && \
    mkdir /opt/librdkafka && \
    curl -SL "https://github.com/edenhill/librdkafka/archive/v1.6.1.tar.gz" | tar -xz --strip-components=1 -C /opt/librdkafka && \
    cd /opt/librdkafka && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    cd /opt/librdkafka/tests && \
    make build -j$(nproc) && \
    go get -u github.com/birdayz/kaf/cmd/kaf && \
    mv /root/go/bin/kaf /usr/local/bin/ && \
    mkdir /tmp/kafkacat && \
    curl -SL "https://github.com/edenhill/kafkacat/archive/1.6.0.tar.gz" | tar -xz --strip-components=1 -C /tmp/kafkacat && \
    cd /tmp/kafkacat && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    ldconfig

# copy source of test (java) programs
COPY --chown=0:0 tests/java /tmp/java
RUN mvn clean package --batch-mode --file /tmp/java/kafka-verifier --define buildDir=/opt/kafka-verifier/ && \
    mvn clean package --batch-mode --file /tmp/java/compacted-log-verifier --define buildDir=/opt/compacted-log-verifier

# copy ssh keys
COPY --chown=0:0 tests/docker/ssh /root/.ssh

# install python dependencies and rptest package.
# rptest package installed in editable mode so it can be overridden.
# passes --force so system pip packages can be updated
COPY --chown=0:0 tests/setup.py /root/tests/
RUN python3 -m pip install --upgrade --force pip && \
		python3 -m pip install --force --no-cache-dir -e /root/tests/ && \
    python3 -m pip install zstandard

CMD service ssh start && tail -f /dev/null
