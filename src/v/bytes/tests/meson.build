t = executable('test_bytes',
  'iobuf_tests.cc',
  'iobuf_utils_tests.cc',
  'bytes_tests.cc',
  dependencies: [seastar_testing_main, bytes, random])

test('bytes test', t)
