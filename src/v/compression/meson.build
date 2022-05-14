libcompression = library('compression',
  'compression.cc',
  'stream_zstd.cc',
  'async_stream_zstd.cc',
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

exe = executable('test_compression',
  'tests/zstd_tests.cc',
  dependencies: [seastar_testing_main, compression])
test('compression', exe, args: '-- -c1')

exe = executable('bench_compression',
  'tests/zstd_stream_bench.cc',
  dependencies: [seastar_perf_testing, compression])
benchmark('compression', exe)
