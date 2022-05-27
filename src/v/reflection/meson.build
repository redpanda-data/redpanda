t = executable('test_reflection_arity',
  'test/arity_test.cc',
  dependencies: [boost, bytes])
test('reflection arity', t)

t = executable('test_async_adl',
  'test/async_adl_test.cc',
  dependencies: [seastar_testing_main, bytes, model, random])
test('reflection async adl', t)
