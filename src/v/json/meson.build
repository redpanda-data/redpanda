libjson = library('json',
  'json.cc',
  dependencies: [seastar, rapidjson, model],
  implicit_include_directories: false)

json = declare_dependency(
  dependencies: [seastar, rapidjson, model],
  link_with: libjson)

t = executable('test_json',
  'tests/json_serialization_test.cc',
  dependencies: [json, seastar_testing_main])
test('json serialization tests', t)
