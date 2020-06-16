# Toolchain in docker

Toolchain image is layered this order:

 1. fedora
 2. base
 3. golang, clang
 4. builder (c++ deps)

The last layer is built in a multi-stage build and combines both 
golang and clang images.

With the exception of the last one, all images have only one single 
tag category, as it doesn't depend on compiler or build type. For the 
last one, we have one tag for each compiler/build-type combination (4 
in total).

These images are available in <https://gcr.io/redpandaci>

# Build the toolchain image using vtools

The `vtools` command implements an efficient way of building these 
images by taking into account previously built images that have been 
uploaded to <https://gcr.io/redpandaci>. To build the toolchain for 
GCC and `release` build type:

```bash
vtools dbuild toolchain
```

To fetch the image containing dependencies built with Clang:


```bash
vtools dbuild toolchain --clang
```

The `vtools dbuild toolchain` command fetches (or builds) a 
`gcr.io/redpandaci/builder:latest` image that is ready to be used to 
build and test the distinct components contained in the Vectorized 
codebase. For example, to build the `redpanda` binary:

```bash
vtools dbuild cpp
```

And the output is placed in a `dbuild/` folder on the root of the 
project.
