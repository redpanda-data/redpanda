FROM fedora@sha256:f4ca3e9814bef8468b59cc3dea90ab52bcb3747057731f7ab7645d923731fed6

ARG LLVM_REF="llvmorg-8.0.0"
ARG BUILD_TYPE=Release

# disable all but base repo so we install same version of packages
RUN rm -f /etc/yum.repos.d/fedora-* && \
    dnf install -y git which jq glibc-langpack-en && \
    dnf clean all

COPY tools/install-deps.sh /install-deps.sh

RUN /install-deps.sh && \
    rm /install-deps.sh && \
    dnf clean all

#######
## LLVM
#######

COPY cmake/caches/llvm.cmake /tmp/

RUN curl -Ls https://github.com/llvm/llvm-project/archive/${LLVM_REF}.tar.gz | tar xz -C /tmp/ && \
    mv /tmp/llvm-project-${LLVM_REF} /tmp/llvm-src/ && \
    mkdir -p /tmp/llvm-build/cmake/caches/ && \
    mv /tmp/llvm.cmake /tmp/llvm-build/cmake/caches/ && \
    cd /tmp/llvm-build && \
    cmake \
      -GNinja \
      -C/tmp/llvm-build/cmake/caches/llvm.cmake \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      /tmp/llvm-src/llvm && \
    ninja && \
    ninja install && \
    rm -rf /tmp/*

#######
## 3rdparty
#######

COPY 3rdparty.cmake.in /tmp/v/
COPY CMakeLists.txt /tmp/v/
COPY cmake/FindLibCxx.cmake /tmp/v/cmake/

RUN export CXX=/usr/local/bin/clang++ && \
    export CC=/usr/local/bin/clang    && \
    cd /tmp/v && \
    cmake \
      -GNinja \
      -B/tmp/v/build \
      -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
      -DV_DEPS_INSTALL_DIR=/usr/local \
      -DRP_ENABLE_GOLD_LINKER=OFF \
      -DV_DEPS_ONLY=ON \
      /tmp/v && \
    cd && \
    rm -rf /tmp/v && \
    sh -c 'echo "/usr/local/lib" > /etc/ld.so.conf.d/usrlocallib.conf' && \
    sh -c 'echo "/usr/local/lib64" > /etc/ld.so.conf.d/usrlocallib64.conf' && \
    ldconfig

# TODO: remove after github.com/vectorizedio/v/issues/191 is fixed
RUN sed -i \
      's/  set (_seastar_dep_args_.*)//' \
      /usr/local/lib64/cmake/Seastar/SeastarDependencies.cmake
