exe = executable('test_cloud_storage_basic',
  'directory_walker_test.cc',
  'partition_manifest_test.cc',
  'topic_manifest_test.cc',
  'tx_range_manifest_test.cc',
  'offset_translation_layer_test.cc',
  'remote_segment_index_test.cc',
  'recovery_request_test.cc',
  'segment_meta_cstore_test.cc',
  dependencies: [seastar_testing_main, cloud_storage, storage_test_utils,
    model_test_utils])
test('cloud_storage', exe)

exe = executable('test_cloud_storage',
  's3_imposter.cc',
  'remote_test.cc',
  'remote_segment_test.cc',
  'remote_partition_test.cc',
  'topic_recovery_service_test.cc',
  dependencies: [seastar_testing_main, cloud_storage, storage_test_utils,
    model_test_utils, redpanda_app])
test('cloud_storage', exe, timeout: 120, args: '-- -c1')

exe = executable('test_cloud_storage_smp',
  'cache_test.cc',
  dependencies: [seastar_testing_main, cloud_storage, storage_test_utils,
    model_test_utils])
test('cloud_storage', exe, args: '-- -c8')

exe = executable('test_cloud_storage_fuzzy_rpfixture',
  's3_imposter.cc',
  'remote_partition_fuzz_test.cc',
  dependencies: [seastar_testing_main, cloud_storage, storage_test_utils,
    model_test_utils, redpanda_app])
test('cloud_storage', exe, timeout: 120, args: '-- -c1')

exe = executable('bench_cloud_storage',
  'cache_bench.cc',
  'segment_meta_cstore_bench.cc',
  dependencies: [seastar_perf_testing, cloud_storage, storage_test_utils,
    model_test_utils])
benchmark('cloud_storage', exe)
