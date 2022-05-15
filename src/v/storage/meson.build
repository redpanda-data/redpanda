libstorage = library('storage',
  'segment_reader.cc',
  'log_manager.cc',
  'mem_log_impl.cc',
  'disk_log_impl.cc',
  'disk_log_appender.cc',
  'parser.cc',
  'log_reader.cc',
  'log_replayer.cc',
  'offset_translator_state.cc',
  'probe.cc',
  'record_batch_builder.cc',
  'logger.cc',
  'segment_appender.cc',
  'segment_set.cc',
  'segment.cc',
  'segment_index.cc',
  'segment_appender_utils.cc',
  'storage_resources.cc',
  'batch_cache.cc',
  'index_state.cc',
  'lock_manager.cc',
  'types.cc',
  'spill_key_index.cc',
  'compacted_index_chunk_reader.cc',
  'snapshot.cc',
  'kvstore.cc',
  'segment_utils.cc',
  'compaction_reducers.cc',
  'parser_utils.cc',
  'readers_cache.cc',
  'backlog_controller.cc',
  'compaction_controller.cc',
  'offset_to_filepos.cc',
  'fs_utils.cc',
  implicit_include_directories: false,
  dependencies: [seastar, model, config, compression, roaring,
    syschecks, utils, resource_mgmt, features])

storage = declare_dependency(
  dependencies: [seastar, model, config, compression, roaring,
    syschecks, utils, resource_mgmt, features],
  link_with: libstorage)

libstorage_test_utils = library('storage_test_utils',
  'tests/utils/disk_log_builder.cc',
  'tests/utils/log_gap_analysis.cc',
  dependencies: [storage, model_test_utils])

storage_test_utils = declare_dependency(
  dependencies: [storage],
  link_with: libstorage_test_utils)

exe = executable('test_storage',
  'tests/log_segment_appender_test.cc',
  'tests/segment_size_jitter_test.cc',
  'tests/log_segment_reader_test.cc',
  'tests/log_manager_test.cc',
  'tests/offset_assignment_test.cc',
  'tests/log_truncate_test.cc',
  'tests/offset_index_utils_tests.cc',
  'tests/compaction_index_format_tests.cc',
  'tests/appender_chunk_manipulations.cc',
  'tests/disk_log_builder_test.cc',
  'tests/log_retention_tests.cc',
  'tests/produce_consume_test.cc',
  'tests/half_page_concurrent_dispatch.cc',
  'tests/timequery_test.cc',
  'tests/kvstore_test.cc',
  'tests/backlog_controller_test.cc',
  'tests/readers_cache_test.cc',
  'tests/concat_segment_reader_test.cc',
  'tests/offset_to_filepos_test.cc',
  'tests/offset_translator_state_test.cc',
  dependencies: [storage, seastar_testing_main, model_test_utils,
    storage_test_utils])
test('storage', exe, timeout: 60, args: '-- -c1')

exe = executable('test_storage_e2e_single_thread',
  'tests/storage_e2e_test.cc',
  dependencies: [storage, seastar_testing_main, model_test_utils,
    storage_test_utils])
test('storage_e2e', exe, timeout: 120, args: '-- -c1')

exe = executable('test_storage_log_replayer',
  'tests/log_replayer_test.cc',
  dependencies: [storage, seastar_testing_main, model_test_utils,
    storage_test_utils])
test('storage', exe, args: '-- -c1')

exe = executable('test_storage_multi_thread',
  'tests/batch_cache_test.cc',
  'tests/record_batch_builder_test.cc',
  'tests/snapshot_test.cc',
  dependencies: [storage, seastar_testing_main, model_test_utils,
    storage_test_utils])
test('storage', exe)

exe = executable('test_segment_index_state',
  'tests/index_state_test.cc',
  dependencies: [boost, storage])
test('storage', exe)

# this test is only run on release builds because non-release builds use the
# default allocator which doesn't support reclaim
exe = executable('test_batch_reclaim',
  'tests/batch_cache_reclaim_test.cc',
  dependencies: [seastar_testing_main, storage, boost])
if get_option('buildtype') == 'release'
  test('storage', exe, args: ['--', '-c1', '-m100'])
endif

exe = executable('test_storage_opfuzz',
  'opfuzz/opfuzz.cc',
  'opfuzz/opfuzz_test.cc',
  dependencies: [seastar_testing_main, model_test_utils, storage])
test('storage', exe, args: '-- -c1')

exe = executable('bench_compaction_index',
  'tests/compaction_idx_bench.cc',
  dependencies: [seastar_perf_testing, storage])
benchmark('storage', exe)
