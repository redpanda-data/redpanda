FROM us-docker.pkg.dev/deeplearning-platform-release/gcr.io/base-cu121.py310

# Setup libcuda
RUN echo "/usr/local/cuda-12.1/compat/" >> /etc/ld.so.conf.d/000_cuda.conf
RUN ldconfig

# Install recent cmake version
RUN cd /usr/local && \
	curl -SLo install.sh https://github.com/Kitware/CMake/releases/download/v3.29.2/cmake-3.29.2-linux-x86_64.sh && \
	bash install.sh --skip-license --exclude-subdir && \
	rm install.sh
RUN which cmake

# Install clang
RUN curl -SLO https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && ./llvm.sh 17 && rm llvm.sh
RUN which clang-17

# Install golang
RUN curl -SLO https://go.dev/dl/go1.22.2.linux-amd64.tar.gz && tar -C /usr/local -xzf go1.22.2.linux-amd64.tar.gz && rm go1.22.2.linux-amd64.tar.gz
ENV PATH="${PATH}:/usr/local/go/bin"
RUN which go

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="${PATH}:/root/.cargo/bin"
RUN which cargo

# Install select deps from install-deps.sh
RUN apt update && apt install -y ccache \
	gnutls-dev \
	libboost-all-dev \
	libc-ares-dev \
	libcrypto++-dev \
	libgssapi-krb5-2 \
	libkrb5-dev \
	libprotobuf-dev \
	libprotoc-dev \
	libre2-dev \
	libsctp-dev \
	libsnappy-dev \
	libssl-dev \
	libxxhash-dev \
	libyaml-cpp-dev \
	libzstd-dev \
	ninja-build \
	protobuf-compiler \
	python3 \
	python3-jinja2 \
	python3-jsonschema \
	ragel \
	systemtap-sdt-dev \
	valgrind \
	xfslibs-dev \
	libstdc++-12-dev

RUN git clone https://github.com/lz4/lz4
WORKDIR /lz4
RUN make && make install

WORKDIR /
# Download src
RUN git clone https://github.com/redpanda-data/redpanda.git
WORKDIR /redpanda
RUN git checkout llama

# Configure
RUN cmake --preset release-static \
	-DCMAKE_C_COMPILER=clang-17 \
	-DCMAKE_CXX_COMPILER=clang++-17
# Build
RUN cmake --build --preset release-static -- redpanda
