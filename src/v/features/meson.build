libfeatures = library('features',
  'feature_table.cc',
  'logger.cc',
  implicit_include_directories: false,
  dependencies: [seastar, model])

features = declare_dependency(
  dependencies: [seastar, model],
  link_with: libfeatures)

t = executable('test_feature_table',
  'tests/feature_table_test.cc',
  dependencies: [seastar_testing_main, features])
test('feature table', t)
