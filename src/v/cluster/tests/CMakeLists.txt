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
    controller_recovery_test.cc
    cluster_utils_tests.cc
    cluster_tests.cc
    metadata_dissemination_test.cc
    notification_latch_test.cc
    autocreate_test.cc
    controller_state_test.cc
    commands_serialization_test.cc
    topic_table_test.cc
    topic_updates_dispatcher_test.cc
    configuration_change_test.cc)

rp_test(
  UNIT_TEST
  BINARY_NAME test_cluster
  SOURCES ${srcs}
  LIBRARIES v::seastar_testing_main v::application
)
