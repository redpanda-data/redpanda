exe = executable('test_serde',
  'serde_test.cc',
  cpp_args: ['-DSERDE_TEST'],
  dependencies: [seastar_testing_main, serde, random])
test('serde', exe)

exe = executable('bench_serde',
  'bench.cc',
  dependencies: [seastar_perf_testing, serde])
benchmark('serde', exe)

if meson.get_compiler('cpp').get_id() == 'clang'
  serde_test_codegen = find_program('struct_gen.py')

  serde_test_structs_h = custom_target(
    output: 'generated_structs.h',
    command: [serde_test_codegen, '@OUTPUT@'])

  exe = executable('serde_fuzz_main',
    'fuzz.cc',
    serde_test_structs_h,
    cpp_args: ['-DMAIN=1'],
    dependencies: [serde])

  if get_option('buildtype').startswith('debug')
    exe = executable('serde_fuzz',
      'fuzz.cc',
      serde_test_structs_h,
      cpp_args: ['-g', '-O0', '-fsanitize=address,fuzzer'],
      link_args: ['-fsanitize=address,fuzzer,undefined'],
      dependencies: [serde])
    test('serde', exe,
      timeout: 70,
      env: ['PLAIN=1'],
      args: ['-max_total_time=60', '-rss_limit_mb=8192'])
  endif
else
  warning('serde fuzz tests not supported with compiler:', meson.get_compiler('cpp').get_id())
endif
