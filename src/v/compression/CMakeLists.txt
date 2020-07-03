find_package(Zstd REQUIRED)
find_package(LZ4 REQUIRED)
find_package(Snappy REQUIRED)
v_cc_library(
  NAME
    compression
  HDRS
    "compression.h"
    "stream_zstd.h"
  SRCS
    "compression.cc"
    "stream_zstd.cc"
    "internal/snappy_compressor.cc"
    "internal/lz4_frame_compressor.cc"
  DEPS
    v::bytes
    Zstd::zstd
    LZ4::LZ4
    Snappy::snappy
  DEFINES
    -DZSTD_STATIC_LINKING_ONLY
)

add_subdirectory(tests)
