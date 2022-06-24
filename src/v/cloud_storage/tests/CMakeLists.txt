rp_test(
  UNIT_TEST
  BINARY_NAME test_cloud_storage
  SOURCES
    directory_walker_test.cc
    partition_manifest_test.cc
    topic_manifest_test.cc
    s3_imposter.cc
    remote_test.cc
    offset_translation_layer_test.cc
    remote_segment_test.cc
    remote_partition_test.cc
    remote_segment_index_test.cc 
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::cloud_storage v::storage_test_utils
  ARGS "-- -c 1"
  LABELS cloud_storage
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_cloud_storage_smp
  SOURCES
    cache_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::cloud_storage v::storage_test_utils
  LABELS cloud_storage
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME cloud_storage_bench
  SOURCES cache_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::cloud_storage
  LABELS cloud_storage
)
