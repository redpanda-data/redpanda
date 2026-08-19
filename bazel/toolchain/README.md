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
docker build --file Dockerfile.llvm --build-arg LLVM_VERSION=$LLVM_VERSION --output type=tar,dest=- . | zstd -19 -o "$OUTPUT_FILE"
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
docker buildx build --platform=linux/arm64 --build-arg LLVM_VERSION=$LLVM_VERSION --file Dockerfile.llvm --output type=tar,dest=- . | zstd -19 -o "$OUTPUT_FILE"
```

### Building from a specific tag

By default the Dockerfile builds from the `release/${LLVM_VERSION}.x` branch (latest on that release branch). To pin a specific LLVM tag instead, use the `LLVM_REF` build arg:

```
LLVM_VERSION=22
LLVM_REF="llvmorg-22.1.0"
OUTPUT_FILE="llvm-22.1.0-debian-11-x86_64-$(date --rfc-3339=date -u).tar.zst"
echo "Building $OUTPUT_FILE"
docker build --file Dockerfile.llvm --build-arg LLVM_VERSION=$LLVM_VERSION --build-arg LLVM_REF=$LLVM_REF --output type=tar,dest=- . | zstd -19 -o "$OUTPUT_FILE"
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
OUTPUT_FILE="sysroot-ubuntu-24.04-x86_64-$(date --rfc-3339=date -u).tar.zst"
docker build --file Dockerfile.sysroot --output type=tar,dest=- . | zstd -19 -o "$OUTPUT_FILE"
```

Building for `arm64` can be done from an `x86_64` host with the following command

```
OUTPUT_FILE="sysroot-ubuntu-24.04-aarch64-$(date --rfc-3339=date -u).tar.zst"
docker buildx build --platform=linux/arm64 --file Dockerfile.sysroot --output type=tar,dest=- . | zstd -19 -o "$OUTPUT_FILE"
```

### Checking a freshly built sysroot

Run `check-sysroot.sh` on the extracted tarball before pinning it:

```
mkdir /tmp/s && zstd -dc "$OUTPUT_FILE" | tar -x -C /tmp/s
./check-sysroot.sh /tmp/s
```

It catches the failure mode that a file listing does not. Several glibc
libraries are GNU ld scripts holding **absolute paths** that are resolved inside
the sysroot at link time - `libm.so` names `libmvec.so.1` via `AS_NEEDED`,
`libm.a` names `libm-<version>.a` and `libmvec.a`, `libc.so` names `libc.so.6`
and `libc_nonshared.a`. If one of those files was not copied in, the sysroot
looks complete and then `ld.lld` fails on some unrelated target with
`cannot find /lib/<triple>/... inside <sysroot>`.

Diffing the file list against the previous sysroot does **not** find these: when
a glibc bump makes a script reference something new, the file is missing from
both old and new, so the diff is empty. The reference is what changed. This is
exactly how the 24.04 bump first broke aarch64 - glibc 2.39 added `libmvec` for
aarch64, where 2.35 had none, so the previously x86_64-only `libmvec` copy left
`libm.so` dangling on arm64 only.

Note the script reports the same `libm.a` dangle for the **22.04** sysroots as
well; it is pre-existing and harmless in practice only because nothing links
static libm.

### The two floors a distro choice sets

The distro the sysroot is built from sets two independent floors, and both
matter when picking it:

* **Kernel API surface.** The `linux-libc-dev` in the image decides which uapi
  constants the build can see, regardless of the kernel the broker runs on.
  24.04 carries 6.8 headers; 22.04 carried 5.15, which silently compiled out
  `MADV_COLLAPSE` in Seastar's memory prefaulter and forced a hand-rolled
  `struct statx` stand-in for `STATX_DIOALIGN`.
* **glibc.** The sysroot's glibc is *shipped with the package* (see
  `//bazel/packaging`, which installs the sysroot's shared libraries and sets
  the binaries' interpreter to the bundled loader), so the host's glibc does
  not constrain us. The floor that does move is the loader's minimum kernel,
  which is 3.2.0 on both 22.04 (glibc 2.35) and 24.04 (glibc 2.39).
