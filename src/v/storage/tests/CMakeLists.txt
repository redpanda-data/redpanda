v_cc_library(
  NAME
    storage_test_utils
  HDRS
    "utils/disk_log_builder.h"
    "utils/log_gap_analysis.h"
  SRCS
    "utils/disk_log_builder.cc"
    "utils/log_gap_analysis.cc"
  DEPS
    v::storage v::model_test_utils v::features
)

rp_test(
  UNIT_TEST
  BINARY_NAME storage_single_thread
  SOURCES
    log_segment_appender_test.cc
    segment_size_jitter_test.cc
    log_segment_reader_test.cc
    log_manager_test.cc
    offset_assignment_test.cc
    log_truncate_test.cc
    offset_index_utils_tests.cc
    compaction_index_format_tests.cc
    appender_chunk_manipulations.cc
    disk_log_builder_test.cc
    log_retention_tests.cc
    produce_consume_test.cc
    half_page_concurrent_dispatch.cc
    timequery_test.cc
    kvstore_test.cc
    backlog_controller_test.cc
    readers_cache_test.cc
    concat_segment_reader_test.cc
    offset_to_filepos_test.cc
    offset_translator_state_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils v::model_test_utils
  LABELS storage
  ARGS "-- -c 1"
)


rp_test(
        UNIT_TEST
        BINARY_NAME storage_e2e_single_thread
        SOURCES
        storage_e2e_test.cc
        LIBRARIES v::seastar_testing_main v::storage_test_utils v::model_test_utils
        LABELS storage
        ARGS "-- -c 1"
)

# Putting this first, last, or in the middle of the other single thread tests
# results in:
# runtime error: member access within null pointer of type
# 'shared_ptr_no_esft<storage::segment>'
rp_test(
  UNIT_TEST
  BINARY_NAME storage_log_replayer_single_thread
  SOURCES
    log_replayer_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils v::model_test_utils
  LABELS storage
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME storage_log_index
  SOURCES
    index_state_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::storage
  LABELS storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME storage_multi_thread
  SOURCES
    batch_cache_test.cc
    record_batch_builder_test.cc
    snapshot_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
  ARGS "-- -c 8"
)

# this test is only run on release builds because non-release builds use the
# default allocator which doesn't support reclaim
rp_test(
  UNIT_TEST
  BINARY_NAME batch_cache_reclaim_test
  SOURCES batch_cache_reclaim_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c1 -m100"
  LABELS storage
  SKIP_BUILD_TYPES "Debug"
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME storage
  SOURCES compaction_idx_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::storage
  LABELS storage
)

