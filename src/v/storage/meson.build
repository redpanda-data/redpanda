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
  implicit_include_directories: false,
  dependencies: [seastar, model, config, compression, roaring,
    syschecks, utils])

storage = declare_dependency(
  dependencies: [seastar, model, config, compression, roaring,
    syschecks, utils],
  link_with: libstorage)

libstorage_test_utils = library('storage_test_utils',
  'tests/utils/disk_log_builder.cc',
  'tests/utils/log_gap_analysis.cc',
  dependencies: [storage, model_test_utils])

storage_test_utils = declare_dependency(
  dependencies: [storage],
  link_with: libstorage_test_utils)

# ARGS "-- -c 1"
t = executable('test_storage',
  'tests/log_segment_appender_test.cc',
  'tests/segment_size_jitter_test.cc',
  'tests/log_segment_reader_test.cc',
  'tests/log_manager_test.cc',
  'tests/offset_assignment_test.cc',
  'tests/storage_e2e_test.cc',
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
  dependencies: [storage, seastar_testing_main, model_test_utils,
    storage_test_utils])
test('storage basic', t)

# Putting this first, last, or in the middle of the other single thread tests
# results in:
# runtime error: member access within null pointer of type
# 'shared_ptr_no_esft<storage::segment>'
# ARGS "-- -c 1"
t = executable('test_log_replayer_single_thread',
  'tests/log_replayer_test.cc',
  dependencies: [storage, seastar_testing_main, model_test_utils,
    storage_test_utils])
test('storage log replayer', t)

t = executable('test_segment_index_state',
  'tests/index_state_test.cc',
  dependencies: [storage, boost])
test('segment index state', t)

t = executable('test_storage_threaded',
  'tests/batch_cache_test.cc',
  'tests/record_batch_builder_test.cc',
  'tests/snapshot_test.cc',
  dependencies: [storage, seastar_testing_main, model_test_utils,
    storage_test_utils])
test('storage threaded', t)

#rp_test(
#  UNIT_TEST
#  BINARY_NAME batch_cache_reclaim_test
#  SOURCES batch_cache_reclaim_test.cc
#  LIBRARIES v::seastar_testing_main v::storage_test_utils
#  ARGS "-- -c1 -m100"
#  LABELS storage
#  SKIP_BUILD_TYPES "Debug"
#)
t = executable('test_batch_reclaim',
  'tests/batch_cache_reclaim_test.cc',
  dependencies: [storage, boost, seastar_testing_main])
# this test is only run on release builds because non-release builds use the
# default allocator which doesn't support reclaim
# test('batch cache reclaim', t)

t = executable('bench_compaction_index',
  'tests/compaction_idx_bench.cc',
  dependencies: [storage, seastar_perf_testing])
benchmark('compaction index', t)

# ARGS "-- -c 1"
t = executable('test_storage_opfuzz',
  'opfuzz/opfuzz.cc',
  'opfuzz/opfuzz_test.cc',
  dependencies: [storage, seastar_testing_main, model_test_utils])
test('storage opfuzz', t)
