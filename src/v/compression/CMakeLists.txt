find_package(Zstd REQUIRED)
find_package(LZ4 REQUIRED)
find_package(Snappy REQUIRED)
v_cc_library(
  NAME
    compression
  HDRS
    "compression.h"
  SRCS
    "compression.cc"
  DEPS
    v::bytes
    Zstd::zstd
    LZ4::LZ4
    Snappy::snappy
)
