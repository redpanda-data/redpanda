find_package(xxHash REQUIRED)
find_package(Crc32c REQUIRED)
v_cc_library(
  NAME rphashing
  SRCS murmur.cc
  COPTS
    -Wno-implicit-fallthrough
  DEPS
    xxHash::xxhash
    Crc32c::crc32c
    GnuTLS::gnutls
  DEFINES
    -DXXH_PRIVATE_API
)

add_subdirectory(tests)
