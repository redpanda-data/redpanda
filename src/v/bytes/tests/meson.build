exe = executable('test_bytes',
  'iobuf_tests.cc',
  'iobuf_utils_tests.cc',
  'bytes_tests.cc',
  dependencies: [seastar_testing_main, bytes, random])
test('bytes', exe)

if meson.get_compiler('cpp').get_id() == 'clang'
  if get_option('buildtype').startswith('debug')
    exe = executable('test_iobuf_fuzz',
      'iobuf_fuzz.cc',
      cpp_args: ['-g', '-O0', '-fsanitize=address,fuzzer'],
      link_args: ['-fsanitize=address,fuzzer,undefined'],
      dependencies: [serde])
    test('bytes', exe,
      timeout: 70,
      env: ['PLAIN=1'],
      args: ['-max_total_time=60', '-rss_limit_mb=8192'])
  endif
else
  warning('iobuf fuzz tests not supported with compiler:', meson.get_compiler('cpp').get_id())
endif
