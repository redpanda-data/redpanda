v_cc_library(
  NAME
    storage_test_utils
  HDRS
    "utils/random_batch.h"
    "utils/disk_log_builder.h"
  SRCS
    "utils/random_batch.cc"
    "utils/disk_log_builder.cc"
  DEPS
    v::storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_segment_appender_test
  SOURCES log_segment_appender_test.cc
          segment_size_jitter_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME parser_test
  SOURCES parser_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_segment_reader_test
  SOURCES log_segment_reader_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_replayer_test
  SOURCES log_replayer_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_manager_test
  SOURCES log_manager_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME offset_assignment_test
  SOURCES offset_assignment_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME storage_e2e_test
  SOURCES storage_e2e_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_truncate_test
  SOURCES log_truncate_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
)
rp_test(
  UNIT_TEST
  BINARY_NAME index_test
  SOURCES
    offset_index_utils_tests.cc
    compaction_index_format_tests.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
)
rp_test(
  UNIT_TEST
  BINARY_NAME appender_chunk_manip
  SOURCES appender_chunk_manipulations.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME batch_cache_test
  SOURCES batch_cache_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
)
rp_test(
  UNIT_TEST
  BINARY_NAME disk_log_builder_test
  SOURCES disk_log_builder_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
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
  UNIT_TEST
  BINARY_NAME log_retention_test
  SOURCES log_retention_tests.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME produce_consume_test
  SOURCES produce_consume_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
)
rp_test(
  UNIT_TEST
  BINARY_NAME half_page_write_test
  SOURCES half_page_concurrent_dispatch.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
  )

rp_test(
  UNIT_TEST
  BINARY_NAME timequery_test
  SOURCES timequery_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage
  )

rp_test(
  UNIT_TEST
  BINARY_NAME snapshot_test
  SOURCES snapshot_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME kvstore_test
  SOURCES kvstore_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  ARGS "-- -c 1"
  LABELS storage)


rp_test(
  BENCHMARK_TEST
  BINARY_NAME compacting_bench
  SOURCES compaction_idx_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::storage
  LABELS storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME file_stm_test
  SOURCES file_stm_test.cc
  LIBRARIES v::seastar_testing_main v::storage_test_utils
  LABELS storage
  ARGS "-- -c 1"
)
