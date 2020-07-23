FROM centos:8

ENV VER 1.5.0
ENV URL https://github.com/edenhill/librdkafka/archive/v${VER}.tar.gz

RUN dnf install -y gcc-c++ python3 make
RUN mkdir /opt/librdkafka && \
    curl -SL ${URL} | tar -xz --strip-components=1 -C /opt/librdkafka
RUN cd /opt/librdkafka && ./configure && make -j$(nproc)
RUN cd /opt/librdkafka/tests && make build -j$(nproc)

COPY entrypoint.sh .
ENTRYPOINT ["./entrypoint.sh"]
