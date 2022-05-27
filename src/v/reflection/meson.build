exe = executable('test_reflection_arity',
  'test/arity_test.cc',
  dependencies: [boost, bytes])
test('reflection', exe)

exe = executable('test_async_adl',
  'test/async_adl_test.cc',
  dependencies: [seastar_testing_main, bytes, model, random])
test('reflection', exe)
