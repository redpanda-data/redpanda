include(FetchContent)

set(FETCHCONTENT_QUIET FALSE)

# don't require that cache entries be created for crc32c options so we can use
# normal variables via set(). a better solution here would be to go update the
# crc32c cmake build.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

function(fetch_dep NAME)
  cmake_parse_arguments(fetch_dep_args "" "REPO;TAG" "" ${ARGN})
  FetchContent_Declare(
    ${NAME}
    GIT_REPOSITORY ${fetch_dep_args_REPO}
    GIT_TAG ${fetch_dep_args_TAG}
    GIT_SHALLOW ON
    GIT_SUBMODULES ""
    GIT_PROGRESS TRUE
    USES_TERMINAL_DOWNLOAD TRUE
    OVERRIDE_FIND_PACKAGE
    SYSTEM
    ${fetch_dep_args_UNPARSED_ARGUMENTS})
endfunction()

set(ABSL_PROPAGATE_CXX_STD ON)
fetch_dep(absl
  REPO https://github.com/abseil/abseil-cpp
  TAG 20230802.1)

fetch_dep(fmt
  REPO https://github.com/fmtlib/fmt.git
  TAG 9.1.0)

# CMakeLists.txt is patched to avoid registering tests. We still want the
# Seastar testing library to be built, but we don't want the tests to run. This
# could be accomplished with Seastar_INSTALL=ON, but this doesn't play nice with
# the add_subdirectory method of using Seastar.
set(Seastar_TESTING ON CACHE BOOL "" FORCE)
set(Seastar_API_LEVEL 6 CACHE STRING "" FORCE)
set(Seastar_CXX_FLAGS -Wno-error)
set(Seastar_WITH_OSSL ON CACHE BOOL "" FORCE)
set(CMAKE_CXX_STANDARD
  "${CMAKE_CXX_STANDARD}"
  CACHE
  STRING
  "C++ standard to build with.")
fetch_dep(seastar
  REPO https://github.com/redpanda-data/seastar.git
  TAG v24.3.x
  PATCH_COMMAND sed -i "s/add_subdirectory (tests/# add_subdirectory (tests/g" CMakeLists.txt)

fetch_dep(avro
  REPO https://github.com/redpanda-data/avro
  TAG release-1.12.0-redpanda
  SOURCE_SUBDIR redpanda_build)

fetch_dep(rapidjson
  REPO https://github.com/redpanda-data/rapidjson.git
  TAG 14a5dd756e9bef26f9b53d3b4eb1b73c6a1794d5
  SOURCE_SUBDIR redpanda_build)

FetchContent_Declare(jsoncons
  URL https://github.com/danielaparker/jsoncons/archive/ffd2540bc9cfb54c16ef4d29d80622605d8dfbe8.tar.gz
  URL_HASH MD5=8984d54668cdeb924fe1e37ea8dcc236
  OVERRIDE_FIND_PACKAGE)

fetch_dep(unordered_dense
  REPO https://github.com/redpanda-data/unordered_dense
  TAG 9338f301522a965309ecec58ce61f54a52fb5c22
)

set(CRC32C_BUILD_TESTS OFF)
set(CRC32C_BUILD_BENCHMARKS OFF)
set(CRC32C_USE_GLOG OFF)
set(CRC32C_INSTALL OFF)
fetch_dep(crc32c
  REPO https://github.com/google/crc32c.git
  TAG 1.1.2)

set(BASE64_BUILD_CLI OFF)
set(BASE64_BUILD_TESTS OFF)
fetch_dep(base64
  REPO https://github.com/aklomp/base64.git
  TAG v0.5.0)

fetch_dep(roaring
  REPO https://github.com/redpanda-data/CRoaring.git
  TAG redpanda
  SOURCE_SUBDIR redpanda_build)

fetch_dep(GTest
  REPO https://github.com/google/googletest
  TAG v1.14.0)

set(ADA_TESTING OFF)
set(ADA_TOOLS OFF)
set(ADA_BENCHMARKS OFF)
fetch_dep(ada
  REPO https://github.com/ada-url/ada
  TAG v2.7.3)

if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64")
  set(TINYGO_TARBALL "tinygo-linux-amd64.tar.gz")
  set(TINYGO_MD5 "e3ca01e1012df48bbe1c913fe1de5cfb")
elseif(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
  set(TINYGO_TARBALL "tinygo-linux-arm64.tar.gz")
  set(TINYGO_MD5 "b7223641fbe05b9e23e24e39338fb965")
endif()

FetchContent_Declare(tinygo
  URL https://github.com/redpanda-data/tinygo/releases/download/v0.31.0-rpk2/${TINYGO_TARBALL}
  URL_HASH MD5=${TINYGO_MD5}
  DOWNLOAD_EXTRACT_TIMESTAMP ON)
FetchContent_GetProperties(tinygo)

fetch_dep(hdrhistogram
  REPO https://github.com/HdrHistogram/HdrHistogram_c
  TAG 0.11.5)

list(APPEND WASMTIME_USER_CARGO_BUILD_OPTIONS --no-default-features)
list(APPEND WASMTIME_USER_CARGO_BUILD_OPTIONS --features=async)
list(APPEND WASMTIME_USER_CARGO_BUILD_OPTIONS --features=addr2line)
list(APPEND WASMTIME_USER_CARGO_BUILD_OPTIONS --features=wat)

# We need submodules for wasmtime to compile
FetchContent_Declare(
  wasmtime
  GIT_REPOSITORY https://github.com/bytecodealliance/wasmtime
  GIT_TAG v20.0.0
  GIT_PROGRESS TRUE
  USES_TERMINAL_DOWNLOAD TRUE
  OVERRIDE_FIND_PACKAGE
  SYSTEM
  SOURCE_SUBDIR crates/c-api)

FetchContent_MakeAvailable(
    absl
    fmt
    rapidjson
    seastar
    GTest
    crc32c
    base64
    roaring
    avro
    tinygo
    wasmtime
    hdrhistogram
    ada
    unordered_dense
    jsoncons)

add_library(Crc32c::crc32c ALIAS crc32c)
add_library(aklomp::base64 ALIAS base64)
add_library(Hdrhistogram::hdr_histogram ALIAS hdr_histogram)

list(APPEND CMAKE_PROGRAM_PATH ${tinygo_SOURCE_DIR}/bin)
