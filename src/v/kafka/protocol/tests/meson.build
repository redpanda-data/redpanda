# batch_reader_test was run with ARGS "-- -c 1"
t = executable('test_kafka_protocol',
  'batch_reader_test.cc',
  'security_test.cc',
  dependencies: [kafka_server, storage_test_utils, seastar_testing_main,
    model_test_utils])
test('kafka protocol', t)
