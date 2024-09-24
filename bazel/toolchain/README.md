# Bazel Toolchains

The following dockerfiles in this directory support compiling clang and it's tools in a way that can be reused.
We install minimal dependencies and build clang toolchains that bazel loads directly. This makes the build more
hermetic and allows us to atomically upgrade the compiler as desired. 

To build a toolchain use the following command:

```
docker build --build-arg "LLVM_VERSION=17.0.6" --file Dockerfile.llvm --output $PWD .
```

The compiler output will be in a tarball in the current directory, this can be uploaded to S3, then bazel can pull
it down as desired.

You can build an `aarch64` toolchain on a `x86_64` host by installing QEMU:

* sudo apt install qemu-system
* dnf install @virtualization

Then build the docker image using buildx like so:

```
docker buildx build --platform=linux/arm64 --build-arg "LLVM_VERSION=17.0.6" --file Dockerfile.llvm --output $PWD .
```

### LTO Builds

By default we build with PGO+LTO, but if PGO is causing issues (like on AArch64), we can choose a different build (resulting
in a slower compiler) by adding the flag `--target=lto`. The current default target is `--target=pgo`
