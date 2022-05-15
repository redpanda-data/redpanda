exe = executable('test_kafka_server',
  'error_mapping_test.cc',
  'timeouts_conversion_test.cc',
  'types_conversion_tests.cc',
  'topic_utils_test.cc',
  'handler_interface_test.cc',
  'quota_managers_test.cc',
  dependencies: [kafka_server, boost])
test('kafka server', exe)

foreach t : ['consumer_groups_test.cc',
  'token_bucket_algorithm_test.cc',
  'member_test.cc',
  'group_test.cc',
  'read_write_roundtrip_test.cc',
  'fetch_test.cc',
  'delete_topics_test.cc',
  'offset_fetch_test.cc',
  'api_versions_test.cc',
  'create_topics_test.cc',
  'find_coordinator_test.cc',
  'list_offsets_test.cc',
  'topic_recreate_test.cc',
  'fetch_session_test.cc',
  'alter_config_test.cc',
  'produce_consume_test.cc',
  'group_metadata_serialization_test.cc',
  'partition_reassignments_test.cc']
  exe = executable('test_' + t.substring(0, -3) + '_rpfixture',
  t,
  dependencies: [kafka_server, seastar_testing_main, redpanda_app,
    storage_test_utils, model_test_utils])
  test('kafka server', exe, is_parallel: false)
endforeach
