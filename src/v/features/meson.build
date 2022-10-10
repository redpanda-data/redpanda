libfeatures = library('features',
  'feature_table.cc',
  'feature_table_snapshot.cc',
  'feature_migrator.cc',
  'migrators/cloud_storage_config.cc',
  'logger.cc',
  implicit_include_directories: false,
  dependencies: [seastar, model, config])

features = declare_dependency(
  dependencies: [seastar, model, config],
  link_with: libfeatures)

exe = executable('test_feature_table',
  'tests/feature_table_test.cc',
  dependencies: [seastar_testing_main, features])
test('features', exe)
