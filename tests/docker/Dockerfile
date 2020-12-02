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

MAINTAINER Vectorized
VOLUME ["/opt/v"]

# Set the timezone.
ENV TZ="/usr/share/zoneinfo/America/Los_Angeles"

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# Set the ducker.creator label so that we know that this is a ducker image.  This will make it
# visible to 'ducker purge'.  The ducker.creator label also lets us know what UNIX user built this
# image.
ARG ducker_creator=default
LABEL ducker.creator=$ducker_creator

RUN apt update && apt install -y \
    sudo \
    default-jre \
    netcat \
    iptables \
    rsync \
    unzip \
    wget \
    curl \
    jq \
    coreutils \
    openssh-server \
    net-tools \
    vim \
    python3-pip \
    python3-dev \
    libffi-dev \
    libssl-dev \
    cmake \
    pkg-config \
    iperf \
    traceroute \
    kafkacat \
    libsnappy1v5 \
    libsnappy-dev \
    libzstd1 \
    libzstd-dev \
    zlib1g \
    zlib1g-dev \
    git \
    fuse \
    && apt-get -y clean

RUN pip3 install --upgrade \
    cffi \
    virtualenv \
    pyasn1 \
    boto3 \
    pycrypto \
    pywinrm \
    ipaddress \
    crc32c \
    enum34 \
    pyyaml \
    requests \
    flask \
    fusepy \
    kafka-python \
    aiohttp \
    prometheus_client
RUN pip3 install --upgrade git+https://github.com/confluentinc/ducktape@v0.8.0

# Set up ssh
COPY ./ssh-config /root/.ssh/config
# NOTE: The paramiko library supports the PEM-format private key, but does not support the RFC4716 format.
RUN ssh-keygen -m PEM -q -t rsa -N '' -f /root/.ssh/id_rsa && cp -f /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# Install binary test dependencies.
ARG KAFKA_MIRROR="https://s3-us-west-2.amazonaws.com/kafka-packages"
RUN mkdir -p "/opt/kafka-2.3.1" && chmod a+rw /opt/kafka-2.3.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.3.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.3.1"
RUN mkdir -p "/opt/kafka-2.4.1" && chmod a+rw /opt/kafka-2.4.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.4.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.4.1"
RUN mkdir -p "/opt/kafka-2.5.0" && chmod a+rw /opt/kafka-2.5.0 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.5.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.5.0"

# Build librdkafka and its test suite
ENV LIBRDKAFKA_VER 1.5.0
ENV LIBRDKAFKA_URL https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VER}.tar.gz
RUN mkdir /opt/librdkafka && \
    curl -SL ${LIBRDKAFKA_URL} | tar -xz --strip-components=1 -C /opt/librdkafka
RUN cd /opt/librdkafka && ./configure && make -j$(nproc)
RUN cd /opt/librdkafka/tests && make build -j$(nproc)

# Install kaf tool
RUN curl https://raw.githubusercontent.com/birdayz/kaf/master/godownloader.sh | BINDIR=/usr/bin sudo bash

# Set up the ducker user.
RUN useradd -ms /bin/bash ducker && \
    mkdir -p /home/ducker/ && rsync -aiq /root/.ssh/ /home/ducker/.ssh && \
    chown -R ducker /home/ducker/ /mnt/ /var/log/ /opt/librdkafka && \
    echo "PATH=$(runuser -l ducker -c 'echo $PATH'):$JAVA_HOME/bin" >> /home/ducker/.ssh/environment && \
    echo 'PATH=$PATH:'"$JAVA_HOME/bin" >> /home/ducker/.profile && \
    echo 'ducker ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

USER ducker

CMD sudo service ssh start && tail -f /dev/null
