rp_test(
  BENCHMARK_TEST
  BINARY_NAME partition_allocator_b
  SOURCES allocation_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::cluster
)

rp_test(
  UNIT_TEST
  BINARY_NAME metadata_dissemination_utils_test
  SOURCES metadata_dissemination_utils_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::cluster
)

set(srcs
    partition_allocator_tests.cc
    simple_batch_builder_test.cc
    serialization_rt_test.cc
    cluster_utils_tests.cc
    cluster_tests.cc
    metadata_dissemination_test.cc
    notification_latch_test.cc
    autocreate_test.cc
    controller_state_test.cc
    commands_serialization_test.cc
    topic_table_test.cc
    topic_updates_dispatcher_test.cc
    controller_backend_test.cc
    configuration_change_test.cc
    seq_stm_tests.cc)

rp_test(
  UNIT_TEST
  BINARY_NAME test_cluster
  SOURCES ${srcs}
  LIBRARIES v::seastar_testing_main v::application v::storage_test_utils
)

# These 2 files have a `using namespace cluster;` and removing that
# would require modifying the FIXTURE_TEST and PERF_TEST_F macros.
set_source_files_properties(
    partition_allocator_tests.cc
    allocation_bench.cc
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION 1)
  
rp_test(
  UNIT_TEST
  BINARY_NAME partition_moving_test
  SOURCES partition_moving_test.cc
  LIBRARIES v::seastar_testing_main v::application v::storage_test_utils
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)
