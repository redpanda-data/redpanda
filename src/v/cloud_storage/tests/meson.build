# ARGS "-- -c 1"
t = executable('test_cloud_storage',
  'directory_walker_test.cc',
  'partition_manifest_test.cc',
  'topic_manifest_test.cc',
  's3_imposter.cc',
  'remote_test.cc',
  'cache_test.cc',
  'offset_translation_layer_test.cc',
  'remote_segment_test.cc',
  'remote_partition_test.cc',
  'remote_segment_index_test.cc',
  dependencies: [seastar_testing_main, cloud_storage, storage_test_utils,
    model_test_utils, cluster])
test('cloud storage', t)
