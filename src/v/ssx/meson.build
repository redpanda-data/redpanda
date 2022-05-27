ssx = declare_dependency(
  dependencies: [utils])

exe = executable('test_ssx',
  'tests/async_transforms.cc',
  'tests/sformat.cc',
  'tests/future_util.cc',
  'tests/thread_worker.cc',
  'tests/sleep_abortable_test.cc',
  implicit_include_directories: false,
  dependencies: [seastar_testing_main, ssx])
test('ssx', exe)

exe = executable('bench_ssx',
  'tests/sformat_bench.cc',
  'tests/thread_worker_bench.cc',
  implicit_include_directories: false,
  dependencies: [seastar_perf_testing, ssx])
benchmark('ssx', exe)
