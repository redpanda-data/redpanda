t = executable('test_serde',
  'serde_test.cc',
  dependencies: [seastar_testing_main, serde, random])
test('serde', t)

t = executable('bench_serde',
  'bench.cc',
  dependencies: [seastar_perf_testing, serde])
benchmark('serde bench', t)

serde_test_codegen = find_program('struct_gen.py')

serde_test_structs_h = custom_target(
  output: 'generated_structs.h',
  command: [serde_test_codegen, '@OUTPUT@'])

if meson.get_compiler('cpp').get_id() == 'clang'
# debug only
tt = executable('serde_fuzz',
  'fuzz.cc',
  serde_test_structs_h,
  cpp_args: ['-g', '-O0', '-fsanitize=address,fuzzer,undefined'],
  link_args: ['-fsanitize=address,fuzzer,undefined'],
  dependencies: [serde])
test('serde fuzz', tt,
  args: ['-max_total_time=60', '-rss_limit_mb=8192'])
endif

# all builds
t = executable('serde_fuzz_main',
  'fuzz.cc',
  serde_test_structs_h,
  cpp_args: ['-DMAIN=1'],
  dependencies: [serde])
test('serde fuzz main', t)
