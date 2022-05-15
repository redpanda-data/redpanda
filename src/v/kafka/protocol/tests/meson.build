exe = executable('test_kafka_protocol',
  'security_test.cc',
  'protocol_test.cc',
  dependencies: [kafka_server, storage_test_utils, seastar_testing_main,
    model_test_utils])
test('kafka_protocol', exe,
  depends: kafka_request_generator,
  env: {'GENERATOR_BIN': kafka_request_generator.full_path()})

exe = executable('test_kafka_protocol_parser',
  'field_parser_test.cc',
  'batch_reader_test.cc',
  dependencies: [kafka_server, storage_test_utils, seastar_testing_main,
    model_test_utils])
test('kafka_protocol_parser', exe)
