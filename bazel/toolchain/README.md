# Bazel Toolchains

The following dockerfiles in this directory support compiling clang and it's tools in a way that can be reused.
We install minimal dependencies and build clang toolchains that bazel loads directly. This makes the build more
hermetic and allows us to atomically upgrade the compiler as desired.

To build a toolchain with the latest LLVM release use the following command:

```
LLVM_VERSION="$(gh release list --repo llvm/llvm-project --exclude-drafts --exclude-pre-releases --json tagName --jq '[.[].tagName | ltrimstr("llvmorg-") | select(test("^20\\."))] | .[0]')"
OUTPUT_FILE="llvm-$LLVM_VERSION-debian-11-x86_64-$(date --rfc-3339=date -u).tar.zst"
echo "Building $OUTPUT_FILE"
LLVM_VERSION=$(echo $LLVM_VERSION | cut -d. -f1)
docker build --file Dockerfile.llvm --build-arg LLVM_VERSION=$LLVM_VERSION --output type=tar,dest=- . | zstd -o "$OUTPUT_FILE"
```

The compiler output will be in a tarball in the current directory, this can be uploaded to S3 or github, then bazel can pull it down as desired.

You can build an `aarch64` toolchain on a `x86_64` host by installing QEMU:

* sudo apt install qemu-system
* dnf install @virtualization

Note that this takes a _very long time_ to build under emulation on the standard issue Redpanda dev machine. It's probably better to do on a native arm64 VM.

Then build the docker image using buildx like so:

```
LLVM_VERSION="$(gh release list --repo llvm/llvm-project --exclude-drafts --exclude-pre-releases --json tagName --jq '[.[].tagName | ltrimstr("llvmorg-") | select(test("^20\\."))] | .[0]')"
OUTPUT_FILE="llvm-$LLVM_VERSION-debian-11-aarch64-$(date --rfc-3339=date -u).tar.zst"
echo "Building $OUTPUT_FILE"
LLVM_VERSION=$(echo $LLVM_VERSION | cut -d. -f1)
# install arm64 emulator image
docker run --privileged --rm tonistiigi/binfmt --install arm64
# verify emulator
docker run --rm --platform linux/arm64 debian:bullseye uname -a
docker buildx build --platform=linux/arm64 --build-arg LLVM_VERSION=$LLVM_VERSION --file Dockerfile.llvm --output type=tar,dest=- . | zstd -o "$OUTPUT_FILE"
```

### Building from a specific tag

By default the Dockerfile builds from the `release/${LLVM_VERSION}.x` branch (latest on that release branch). To pin a specific LLVM tag instead, use the `LLVM_REF` build arg:

```
LLVM_VERSION=22
LLVM_REF="llvmorg-22.1.0"
OUTPUT_FILE="llvm-22.1.0-debian-11-x86_64-$(date --rfc-3339=date -u).tar.zst"
echo "Building $OUTPUT_FILE"
docker build --file Dockerfile.llvm --build-arg LLVM_VERSION=$LLVM_VERSION --build-arg LLVM_REF=$LLVM_REF --output type=tar,dest=- . | zstd -o "$OUTPUT_FILE"
```

`LLVM_VERSION` is still required to install the bootstrap compiler from apt.llvm.org. `LLVM_REF` can be a tag (`llvmorg-22.1.0`) or branch (`main`).

### LTO Builds

By default we build with PGO+LTO, but if PGO is causing issues (like on AArch64), we can choose a different build (resulting
in a slower compiler) by adding the flag `--target=lto`. The current default target is `--target=pgo`


## Sysroot

To make builds more hermetic we build with a sysroot from an older linux distro. These sysroots are crafted by creating a docker image with
the correct packages in it, then extracting out the exact set of headers and shared libraries that are needed.

To build an `x86_64` sysroot on an `x86_64` machine the following command can be used

```
OUTPUT_FILE="sysroot-ubuntu-22.04-x86_64-$(date --rfc-3339=date -u).tar.zst"
docker build --file Dockerfile.sysroot --output type=tar,dest=- . | zstd -o "$OUTPUT_FILE"
```

Building for `arm64` can be done from an `x86_64` host with the following command

```
OUTPUT_FILE="sysroot-ubuntu-22.04-aarch64-$(date --rfc-3339=date -u).tar.zst"
docker buildx build --platform=linux/arm64 --file Dockerfile.sysroot --output type=tar,dest=- . | zstd -o "$OUTPUT_FILE"
```
