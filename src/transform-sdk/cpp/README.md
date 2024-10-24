# Experimental Redpanda Data Transforms C++ SDK

This directory contains an experimental C++ SDK for Redpanda Data Transforms.
C++23 is required for both language features and standard library support.

When writing C++ in WebAssembly, note that certain language features are not supported.
Noteably C++ exceptions, `setjmp`/`longjmp`, threads and networking is not supported, WebAssembly and WASI are quickly evolving, so some of these features may be supported in the future.

## Usage 

A specialized clang toolchain is needed for WebAssembly + WASI compilation. The easiest way to
do this is using [zig cc][zig] or [WASI SDK][wasi-sdk] via docker.

First create the identity transform that mirrors records to the output topic from the input topic.

Create your `transform.cc` file that does this:

```cpp
#include <redpanda/transform_sdk.h>

int main() {
    redpanda::on_record_written([](redpanda::write_event event, redpanda::record_writer* writer) {
      return writer->write(event.record);
    });
}
```

### CMake

Using CMake create a `CMakeLists.txt` with the initial project boilerplate, which pulls down our C++ SDK and builds the transform.

```cmake
cmake_minimum_required(VERSION 3.22)

project(
  MyTransform
  VERSION 0.1
  LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

include(FetchContent)
set(FETCHCONTENT_QUIET FALSE)

FetchContent_Declare(
  redpanda-transform-sdk
  URL                    https://github.com/redpanda-data/redpanda/archive/refs/heads/dev.tar.gz
  SOURCE_SUBDIR          src/transform-sdk/cpp
)

FetchContent_MakeAvailable(redpanda-transform-sdk)

add_executable(
  my_transform
  transform.cc
)
target_link_libraries(
  my_transform
  Redpanda::transform_sdk
)
```

Once this file is created generate the build and build the `my_transform` Wasm binary via:

```shell
docker run -v `pwd`:/src -w /src ghcr.io/webassembly/wasi-sdk /bin/bash -c 'cmake -Bbuild && cmake --build build'
```

### Standalone

It can be compiled to WebAssembly using `zig`, the result will be a WebAssembly binary named `a.out` in the current directory.

```shell
zig c++ --target=wasm32-wasi -std=c++23 -fno-exceptions -flto -O3 -Iinclude transform.cc src/transform_sdk.cc
```

Alternatively using docker and WASI SDK, the same program can be compiled using the following:

```shell
docker run -v `pwd`:/src -w /src ghcr.io/webassembly/wasi-sdk /bin/bash -c '$CXX $CXX_FLAGS -std=c++23 -fno-exceptions -O3 -flto -Iinclude transform.cc src/transform_sdk.cc'
```

## Development

CMake can be used for local development. There is a small test suite embeded that can be compiled via `-DREDPANDA_TRANSFORM_SDK_ENABLE_TESTING`. We keep the code to a single header and source file so it is easy to drop into other build systems.

[zig]: https://andrewkelley.me/post/zig-cc-powerful-drop-in-replacement-gcc-clang.html
[wasi-sdk]: https://github.com/WebAssembly/wasi-sdk
