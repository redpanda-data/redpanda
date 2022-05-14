libcompression = library('compression',
  'compression.cc',
  'stream_zstd.cc',
  'logger.cc',
  'snappy_standard_compressor.cc',
  'internal/snappy_java_compressor.cc',
  'internal/lz4_frame_compressor.cc',
  'internal/gzip_compressor.cc',
  cpp_args: ['-DZSTD_STATIC_LINKING_ONLY'],
  implicit_include_directories: false,
  dependencies: [bytes, zlib, zstd, snappy, random])

compression = declare_dependency(
  dependencies: [bytes, zlib, zstd, snappy, random],
  link_with: libcompression)

t = executable('bench_zstd_stream',
  'tests/zstd_stream_bench.cc',
  dependencies: [compression, seastar_perf_testing])
benchmark('zstd stream', t)

t = executable('test_zstd',
  'tests/zstd_tests.cc',
  dependencies: [compression, seastar_testing_main])
test('zstd testing', t)
