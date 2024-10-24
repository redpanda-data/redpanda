v_cc_library(
  NAME
    s3_imposter
  HDRS
    "s3_imposter.h"
  SRCS
    "s3_imposter.cc"
  DEPS
    v::config
    v::http
    v::http_test_utils
    v::model
    v::cloud_storage_clients
)

# Regular unit tests that don't open ports and run on a single core
rp_test(
  UNIT_TEST
  BINARY_NAME cloud_storage
  SOURCES
    s3_imposter.cc
    directory_walker_test.cc
    partition_manifest_test.cc
    tx_range_manifest_test.cc
    remote_segment_index_test.cc
    recovery_request_test.cc
    segment_meta_cstore_test.cc
    segment_chunk_test.cc
    materialized_manifest_cache_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::cloud_storage v::storage_test_utils v::cloud_roles v::raft
  ARGS "-- -c 1"
  LABELS cloud_storage
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_cloud_storage
  SOURCES
    topic_mount_manifest_test.cc
  LIBRARIES
    v::gtest_main
    v::seastar_testing_main
    v::cloud_storage
    v::storage_test_utils
    v::cloud_roles
    v::application
    v::kafka_test_utils
    v::http_test_utils
  ARGS "-- -c 1"
  LABELS cloud_storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME topic_manifest
  SOURCES topic_manifest_test.cc
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::cluster
  ARGS "-- -c 1"
  LABELS cluster
)

# Unit tests that rely on doing remote I/O via s3_imposter
rp_test(
  FIXTURE_TEST
  BINARY_NAME cloud_storage
  SOURCES
    util.cc
    s3_imposter.cc
    remote_file_test.cc
    remote_segment_test.cc
    remote_partition_test.cc
    topic_recovery_service_test.cc
    delete_records_e2e_test.cc
    async_manifest_view_test.cc
    read_replica_test.cc
    anomalies_detector_test.cc
    segment_chunk_hydration_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    Boost::unit_test_framework
    v::seastar_testing_main
    v::cloud_storage
    v::storage_test_utils
    v::cloud_roles
    v::application
    v::kafka_test_utils
    v::http_test_utils
  ARGS "-- -c 1"
  LABELS cloud_storage
)

rp_test(
  FIXTURE_TEST
  GTEST
  TIMEOUT 1000
  BINARY_NAME gtest_cloud_storage
  SOURCES
    cloud_storage_e2e_test.cc
    partition_manifest_downloader_test.cc
    remote_path_provider_test.cc
    remote_test.cc
    s3_imposter.cc
    topic_manifest_downloader_test.cc
    topic_namespace_override_recovery_test.cc
    topic_mount_handler_test.cc
    util.cc
  LIBRARIES
    v::gtest_main
    v::seastar_testing_main
    v::cloud_storage
    v::storage_test_utils
    v::cloud_roles
    v::application
    v::kafka_test_utils
    v::http_test_utils
  ARGS "-- -c 1"
  LABELS cloud_storage
)

rp_test(
  FIXTURE_TEST
  BINARY_NAME cloud_storage_fuzzy
  SOURCES
    util.cc
    s3_imposter.cc
    remote_partition_fuzz_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
  Boost::unit_test_framework
    v::seastar_testing_main
    v::cloud_storage
    v::storage_test_utils
    v::cloud_roles
    v::application
  ARGS "-- -c 1"
  LABELS cloud_storage
)

# Unit tests that rely on having multiple cores available
rp_test(
  UNIT_TEST
  BINARY_NAME cloud_storage_smp
  SOURCES
    cache_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::cloud_storage v::storage_test_utils
  LABELS cloud_storage
  ARGS "-- -c 8"
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME cloud_storage
  SOURCES cache_bench.cc segment_meta_cstore_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::cloud_storage v::storage
  LABELS cloud_storage
)

# Fuzz test for segment_meta_cstore
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_executable(segment_meta_cstore_fuzz_rpfixture segment_meta_cstore_fuzz.cc)
  target_compile_options(segment_meta_cstore_fuzz_rpfixture PRIVATE -Og -fsanitize=address,undefined,fuzzer -fprofile-instr-generate -fcoverage-mapping)
  target_link_libraries(segment_meta_cstore_fuzz_rpfixture v::segment_meta_cstore fmt::fmt -fsanitize=address,undefined,fuzzer -fprofile-instr-generate -fcoverage-mapping)
  add_test (
    NAME segment_meta_cstore_fuzz_rpfixture
    COMMAND $<TARGET_FILE:segment_meta_cstore_fuzz_rpfixture> -max_total_time=30 -rss_limit_mb=8192
  )
endif()
endif()
