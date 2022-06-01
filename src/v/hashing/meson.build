libhashing = library('hashing',
  'murmur.cc',
  implicit_include_directories: false)

hashing = declare_dependency(
  dependencies: [crc32c, bytes],
  compile_args: '-DXXH_PRIVATE_API',
  link_with: libhashing)

exe = executable('test_xxhash',
  'tests/xx_tests.cc',
  dependencies: [hashing, boost])
test('hashing', exe)

exe = executable('test_secure_hash',
  'tests/secure_tests.cc',
  dependencies: [hashing, boost])
test('hashing', exe)

exe = executable('bench_hash',
  'tests/hash_bench.cc',
  dependencies: [hashing, seastar_perf_testing, random])
benchmark('hashing', exe)
