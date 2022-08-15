libcompat = library('compat',
  'run.cc',
  implicit_include_directories: false,
  dependencies: [seastar, json, cluster, utils, raft, model_test_utils])

compat = declare_dependency(
  dependencies: [seastar, json, cluster, utils, raft, model_test_utils],
  link_with: libcompat)

exe = executable('test_compat',
  'tests/compat_test.cc',
  dependencies: [seastar_testing_main, compat])
test('compat', exe)
