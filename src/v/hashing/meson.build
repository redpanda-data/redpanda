libhashing = library('hashing',
  'murmur.cc',
  implicit_include_directories: false)

hashing = declare_dependency(
  dependencies: [crc32c, bytes],
  compile_args: '-DXXH_PRIVATE_API',
  link_with: libhashing)

t = executable('test_xxhash',
  'tests/xx_tests.cc',
  dependencies: [hashing, boost])
test('xxhash tests', t)

t = executable('test_secure_hashing',
  'tests/secure_tests.cc',
  dependencies: [hashing, boost])
test('secure hashing tests', t)

t = executable('bench_hashing',
  'tests/hash_bench.cc',
  dependencies: [hashing, seastar_perf_testing, random])
benchmark('hashing benchmark', t)
