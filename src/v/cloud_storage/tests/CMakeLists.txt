
# Regular unit tests that don't open ports and run on a single core
rp_test(
  UNIT_TEST
  BINARY_NAME cloud_storage_basic
  SOURCES
    directory_walker_test.cc
    partition_manifest_test.cc
    topic_manifest_test.cc
    tx_range_manifest_test.cc
    remote_segment_index_test.cc
    recovery_request_test.cc
    segment_meta_cstore_test.cc 
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::cloud_storage v::storage_test_utils v::cloud_roles
  ARGS "-- -c 1"
  LABELS cloud_storage
)

# Unit tests that rely on doing remote I/O via s3_imposter
rp_test(
  UNIT_TEST
  BINARY_NAME cloud_storage
  SOURCES
    s3_imposter.cc
    remote_test.cc
    remote_segment_test.cc
    remote_partition_test.cc
    topic_recovery_service_test.cc
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

rp_test(
  FIXTURE_TEST
  BINARY_NAME cloud_storage_fuzzy
  SOURCES
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
