libmodel = library('model',
  'model.cc',
  'record_batch_reader.cc',
  'record_utils.cc',
  'async_adl_serde.cc',
  'adl_serde.cc',
  'validation.cc',
  implicit_include_directories: false,
  dependencies: [seastar, hashing, serde])

model = declare_dependency(
  dependencies: [seastar, hashing, serde],
  link_with: libmodel)

libmodel_test_utils = library('model_test_utils',
  'tests/random_batch.cc',
  implicit_include_directories: false,
  dependencies: [seastar, compression, model])

model_test_utils = declare_dependency(
  dependencies: [seastar, compression, model],
  link_with: libmodel_test_utils)

t = executable('test_model',
  'tests/lexical_cast_tests.cc',
  'tests/ntp_path_test.cc',
  'tests/topic_view_tests.cc',
  implicit_include_directories: false,
  dependencies: [model, boost, model_test_utils])
test('model testing', t)

# ARGS "-- -c 1"
t = executable('test_model_thread',
  'tests/record_batch_test.cc',
  'tests/record_batch_reader_test.cc',
  'tests/model_serialization_test.cc',
  implicit_include_directories: false,
  dependencies: [model, seastar_testing_main, model_test_utils])
test('model thread tests', t)
