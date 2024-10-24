find_package(xxHash REQUIRED)
find_package(Crc32c REQUIRED)

v_cc_library(
  NAME hashing
  SRCS murmur.cc
  COPTS
    -Wno-implicit-fallthrough
  DEPS
    xxHash::xxhash
    Crc32c::crc32c
    v::crypto
    v::bytes
  DEFINES
    -DXXH_PRIVATE_API
)

add_subdirectory(tests)
