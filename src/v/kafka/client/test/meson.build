exe = executable('test_kafka_client_unit',
  'partitioners.cc',
  dependencies: [kafka_client, boost])
test('kafka client unit', exe)

exe = executable('test_kafka_client_threaded',
  'fetch_session.cc',
  'produce_batcher.cc',
  'produce_partition.cc',
  'retry_with_mitigation.cc',
  dependencies: [kafka_client, seastar_testing_main, kafka_server])
test('kafka client threaded', exe)

exe = executable('test_kafka_client_rpfixture',
  'consumer_group.cc',
  'fetch.cc',
  'produce.cc',
  'reconnect.cc',
  'retry.cc',
  dependencies: [kafka_client, seastar_testing_main, redpanda_app,
    storage_test_utils, model_test_utils])
test('kafka client fixture', exe, timeout: 60, is_parallel: false)
