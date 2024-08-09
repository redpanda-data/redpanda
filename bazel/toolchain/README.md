# Bazel Toolchains

The following dockerfiles in this directory support compiling clang and it's tools in a way that can be reused.
We install minimal dependencies and build clang toolchains that bazel loads directly. This makes the build more
hermetic and allows us to atomically upgrade the compiler as desired. 

To build a toolchain use `docker build --file Dockerfile.oldlinux --output $PWD .`

The compiler output will be in a tarball in the current directory, this can be uploaded to S3, then bazel can pull
it down as desired.
