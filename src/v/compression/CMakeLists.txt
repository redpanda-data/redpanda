find_package(Zstd REQUIRED)
find_package(LZ4 REQUIRED)
find_package(Snappy REQUIRED)
find_package(ZLIB REQUIRED)

v_cc_library(
  NAME
    compression
  SRCS
    "compression.cc"
    "stream_zstd.cc"
    "async_stream_zstd.cc"
    "snappy_standard_compressor.cc"
    "lz4_decompression_buffers.cc"
    "gzip_stream_decompression.cc"
    "internal/snappy_java_compressor.cc"
    "internal/lz4_frame_compressor.cc"
    "internal/gzip_compressor.cc"
  DEPS
    v::bytes
    v::ssx
    Zstd::zstd
    LZ4::LZ4
    Snappy::snappy
    ZLIB::ZLIB
  DEFINES
    -DZSTD_STATIC_LINKING_ONLY
)

add_subdirectory(tests)
