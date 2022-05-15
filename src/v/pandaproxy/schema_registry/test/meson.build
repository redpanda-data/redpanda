t = executable('test_scheam_reg_unit',
  'sanitize_avro.cc',
  'util.cc',
  'storage.cc',
  'store.cc',
  dependencies: [boost, pandaproxy_schema_registry])
test('schema registry unit', t)

# ARGS "-- -c 1"
t = executable('test_scheam_reg_single_thread',
  'one_shot.cc',
  'sharded_store.cc',
  'consume_to_store.cc',
  'compatibility_store.cc',
  'compatibility_3rdparty.cc',
  'compatibility_avro.cc',
  'compatibility_protobuf.cc',
  dependencies: [seastar_testing_main, pandaproxy_schema_registry])
test('schema registry single thread', t)

# TODO(Ben): Make schema_registry properly sharded
# ARGS "-- -c 1"
t = executable('test_scheam_reg_fixture',
  'get_schema_types.cc',
  'post_subjects_subject_version.cc',
  'delete_subject_endpoints.cc',
  dependencies: [seastar_testing_main, pandaproxy_schema_registry, redpanda_app])
test('schema registry fixture', t)
