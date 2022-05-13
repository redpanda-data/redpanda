librandom = library('random',
  'generators.cc',
  dependencies: [seastar, bytes],
  implicit_include_directories: false)

random = declare_dependency(
  link_with: librandom,
  dependencies: [seastar, bytes, absl])

t = executable('test_random',
  'random_test.cc',
  dependencies: [random, boost])

test('random tests', t)
