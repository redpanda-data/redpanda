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
      python3-pip \
      openssh-server \
      default-jre \
      build-essential \
      libatomic1 \
      curl \
      iptables \
      kafkacat && \
    rm -rf /var/lib/apt/lists/* && \
    echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# install nodejs
env NODE_VERSION="12.16.1"
env NODE_TAR=/opt/node/nodejs-${NODE_VERSION}.tar.xz
RUN mkdir -p /opt/node && \
		curl https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz > ${NODE_TAR} && \
		tar -xf ${NODE_TAR} -C /opt/node --strip 1 && \
		rm -rf ${NODE_TAR}

# install kafka binary dependencies, kaf tool and librdkafka dev
ENV KAFKA_MIRROR="https://s3-us-west-2.amazonaws.com/kafka-packages"
RUN mkdir -p "/opt/kafka-2.3.1" && chmod a+rw /opt/kafka-2.3.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.3.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.3.1" && \
    mkdir -p "/opt/kafka-2.4.1" && chmod a+rw /opt/kafka-2.4.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.4.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.4.1" && \
    mkdir -p "/opt/kafka-2.5.0" && chmod a+rw /opt/kafka-2.5.0 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.5.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.5.0" && \
    mkdir -p "/opt/kafka-2.7.0" && chmod a+rw /opt/kafka-2.7.0 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.7.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.7.0" && \
    curl https://raw.githubusercontent.com/birdayz/kaf/master/godownloader.sh | bash && \
    mkdir /opt/librdkafka && \
    curl -SL "https://github.com/edenhill/librdkafka/archive/v1.5.0.tar.gz" | tar -xz --strip-components=1 -C /opt/librdkafka && \
    cd /opt/librdkafka && \
    ./configure && \
    make -j$(nproc) && \
    cd /opt/librdkafka/tests && \
    make build -j$(nproc)

# copy ssh keys
COPY --chown=0:0 docker/ssh /root/.ssh

# install python dependencies and rptest package.
# rptest package installed in editable mode so it can be overridden.
# passes --force so system pip packages can be updated
COPY --chown=0:0 setup.py /root/tests/
RUN python3 -m pip install --upgrade --force pip && \
    python3 -m pip install --force --no-cache-dir -e /root/tests/

CMD service ssh start && tail -f /dev/null
