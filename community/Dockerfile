FROM ubuntu:21.10

RUN mkdir /src
WORKDIR /src

COPY redpanda redpanda/
WORKDIR /src/redpanda

RUN DEBIAN_FRONTEND=non-interactive ./install-dependencies.sh
RUN DEBIAN_FRONTEND=non-interactive ./build.sh
