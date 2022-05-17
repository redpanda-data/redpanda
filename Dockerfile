from ubuntu:jammy

run apt-get update

run DEBIAN_FRONTEND=noninteractive apt-get install -qq \
    python3 python3-pip python3-setuptools python3-wheel ninja-build \
    libboost1.74-all-dev libprotobuf-dev libprotoc-dev libcrypto++-dev \
    pkg-config libfmt-dev liblz4-dev libgnutls28-dev libc-ares-dev \
    libyaml-cpp-dev ragel clang libabsl-dev libsnappy-dev libxxhash-dev \
    libzstd-dev git python3-jsonschema xfslibs-dev valgrind systemtap-sdt-dev \
    libsctp-dev ccache python3-jinja2 libroaring-dev cmake lld

run pip3 install meson

workdir /src/redpanda
cmd ["bash", "-c", "meson builddir && cd builddir && meson compile"]
