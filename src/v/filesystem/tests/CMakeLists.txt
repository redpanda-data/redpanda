find_package(GTest REQUIRED CONFIG)
find_package(benchmark REQUIRED)
find_package(smf REQUIRED)
include(smfc_generator)
rp_test(
  UNIT_TEST
  BINARY_NAME epoch_extractor
  SOURCES wal_epoch_extractor_test.cc
  LIBRARIES GTest::gtest rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
rp_test(
  UNIT_TEST
  BINARY_NAME wal_opts_validators
  SOURCES wal_opts_tests.cc
  LIBRARIES GTest::gtest rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
rp_test(
  UNIT_TEST
  BINARY_NAME file_size_utils
  SOURCES file_size_utils_test.cc
  LIBRARIES GTest::gtest rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME wal_segment
  SOURCES wal_segment_test.cc
  LIBRARIES rpfs
  ARGS "-c 2 --write-ahead-log-dir=."
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME wal_read_write
  SOURCES wal_read_write_test.cc
  LIBRARIES rpfs
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/v/filesystem
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/third_party
  ARGS "-c 2 --write-ahead-log-dir=."
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME wal_segment_failure_recovery
  SOURCES wal_segment_failure_recovery_test.cc
  LIBRARIES rpfs
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/v/filesystem
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/third_party
  # we only write one file in this test
  ARGS "-c 1 --write-ahead-log-dir=."
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME wal_reader_marked_deleted
  SOURCES wal_reader_marked_deleted_test.cc
  LIBRARIES rpfs
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/v/filesystem
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/third_party
  ARGS "-c 2 --write-ahead-log-dir=."
)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME topic_partition_hashing
  SOURCES topic_partition_hashing_bench.cc
  TIMEOUT 120
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/v/filesystem
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/include
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/third_party
  LIBRARIES benchmark::benchmark rphashing
  )
rp_test(
  BENCHMARK_TEST
  BINARY_NAME core_balance
  SOURCES core_balance_bench.cc
  TIMEOUT 120
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/v/filesystem
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/third_party
  LIBRARIES benchmark::benchmark rphashing
  )
rp_test(
  BENCHMARK_TEST
  BINARY_NAME xxhash_vs_stringview
  SOURCES index_hash_vs_stringview_bench.cc
  TIMEOUT 120
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
  LIBRARIES benchmark::benchmark rphashing smf::smf
  )
rp_test(
  UNIT_TEST
  BINARY_NAME incremental_vs_static
  SOURCES incremental_vs_static_tests.cc
  LIBRARIES GTest::gtest smf::smf rphashing
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME wal_segment_indexer_basic
  SOURCES wal_segment_indexer_basic_test.cc
  LIBRARIES rpfs
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/v/filesystem
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/third_party
  ARGS "-c 2 --write-ahead-log-dir=."
)
rp_test(
  HBADGER_TEST
  BINARY_NAME wal_segment_indexer_append_fail
  SOURCES wal_segment_indexer_basic_test.cc
  LIBRARIES rpfs_hbadger
  INPUT_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/hb_fail_wal_segment_append.lua=honey_badger.lua"
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/v/filesystem
    ${PROJECT_SOURCE_DIR}/src/third_party/smf/src/third_party
  ARGS "-c 1 --write-ahead-log-dir=.  2>/dev/null | grep 'wal_segment::append: Inject Permanent Failure'"
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME wal_segment_record_roundtrip
  SOURCES wal_segment_record_test.cc
  LIBRARIES rpfs
  INCLUDES
    ${PROJECT_SOURCE_DIR}/src/v/filesystem
  ARGS "-c 1"
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME create_dir
  SOURCES nested_dir.cc
  LIBRARIES rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
  ARGS "-c 1"
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME double_indexing
  SOURCES simulate_cold_index.cc
  LIBRARIES rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
  ARGS "-c 2"
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME delayed_flush
  SOURCES delayed_flush_twice.cc wal_smash.cc
  LIBRARIES rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
  ARGS "-c 2"
)
rp_test(
  UNIT_TEST
  BINARY_NAME wal_binary_record_order
  SOURCES binary_record_tests.cc
  LIBRARIES GTest::gtest rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
rp_test(
  UNIT_TEST
  BINARY_NAME page_cache_result_tests
  SOURCES page_cache_result_tests.cc
  LIBRARIES GTest::gtest rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME out_of_bounds_read
  SOURCES out_of_bounds_read.cc wal_smash.cc
  LIBRARIES rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
  ARGS "-c 1"
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME page_cache_file_idx
  SOURCES page_cache_file_idx_tests.cc
  LIBRARIES rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
rp_test(
  UNIT_TEST
  BINARY_NAME page_cache_clamp_tests
  SOURCES page_cache_clamp_tests.cc
  LIBRARIES GTest::gtest rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/filesystem
)
