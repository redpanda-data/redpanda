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
FROM ubuntu:focal

ENV TZ="UTC" \
    DEBIAN_FRONTEND=noninteractive

# - install openssh, jvm and kafka cli tools
# - allow user env variables in ssh sessions
RUN apt update && \
    apt install -y \
      python3-pip \
      openssh-server \
      default-jre \
      build-essential \
      curl \
      kafkacat && \
    rm -rf /var/lib/apt/lists/* && \
    echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# install kafka binary dependencies, kaf tool and librdkafka dev
ARG KAFKA_MIRROR="https://s3-us-west-2.amazonaws.com/kafka-packages"
RUN mkdir -p "/opt/kafka-2.3.1" && chmod a+rw /opt/kafka-2.3.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.3.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.3.1" && \
    mkdir -p "/opt/kafka-2.4.1" && chmod a+rw /opt/kafka-2.4.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.4.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.4.1" && \
    mkdir -p "/opt/kafka-2.5.0" && chmod a+rw /opt/kafka-2.5.0 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.5.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.5.0" && \
    curl https://raw.githubusercontent.com/birdayz/kaf/master/godownloader.sh | bash && \
    mkdir /opt/librdkafka && \
    curl -SL "https://github.com/edenhill/librdkafka/archive/v1.5.0.tar.gz" | tar -xz --strip-components=1 -C /opt/librdkafka && \
    cd /opt/librdkafka && \
    ./configure && \
    make -j$(nproc) && \
    cd /opt/librdkafka/tests && \
    make build -j$(nproc)

# install python dependencies
COPY . /root/tests/
RUN pip3 install --no-cache-dir --upgrade pip wheel setuptools && \
    pip3 install --no-cache-dir -e /root/tests/ && \
    pip3 list && \
    mv /root/tests/docker/ssh /root/.ssh && \
    rm -r /root/tests/ci /root/tests/docker

CMD service ssh start && tail -f /dev/null
