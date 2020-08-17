
rp_test(
  UNIT_TEST
  BINARY_NAME partition_allocator
  SOURCES partition_allocator_tests.cc
  LIBRARIES v::seastar_testing_main v::cluster

)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME partition_allocator_b
  SOURCES allocation_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::cluster
)
rp_test(
  UNIT_TEST
  BINARY_NAME simple_batch_builder_test
  SOURCES simple_batch_builder_test.cc
  LIBRARIES v::seastar_testing_main v::cluster
)
rp_test(
  UNIT_TEST
  BINARY_NAME serialization_rt_test
  SOURCES serialization_rt_test.cc
  LIBRARIES v::seastar_testing_main v::cluster
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
 UNIT_TEST
 BINARY_NAME controller_recovery_test
 SOURCES controller_recovery_test.cc
 LIBRARIES v::seastar_testing_main v::cluster
)
rp_test(
  UNIT_TEST
  BINARY_NAME cluster_utils_tests
  SOURCES cluster_utils_tests.cc
  LIBRARIES v::seastar_testing_main v::cluster
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME cluster_tests
  SOURCES cluster_tests.cc
  LIBRARIES v::seastar_testing_main v::application
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME metadata_dissemination_utils_test
  SOURCES metadata_dissemination_utils_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::cluster
)
rp_test(
  UNIT_TEST
  BINARY_NAME metadata_dissemination_test
  SOURCES metadata_dissemination_test.cc
  LIBRARIES v::seastar_testing_main v::application
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME notification_latch_test
  SOURCES notification_latch_test.cc
  LIBRARIES v::seastar_testing_main v::cluster
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME autocreate_test
  SOURCES autocreate_test.cc
  LIBRARIES v::seastar_testing_main v::cluster
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME controller_state_test
  SOURCES controller_state_test.cc
  LIBRARIES v::seastar_testing_main v::application
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME commands_serialization_test
  SOURCES commands_serialization_test.cc
  LIBRARIES v::seastar_testing_main v::application
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME topic_table_test
  SOURCES topic_table_test.cc
  LIBRARIES v::seastar_testing_main v::application
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME topic_updates_dispatcher_test
  SOURCES topic_updates_dispatcher_test.cc
  LIBRARIES v::seastar_testing_main v::application
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME configuration_change_test
  SOURCES configuration_change_test.cc
  LIBRARIES v::seastar_testing_main v::application
  DEFINITIONS SEASTAR_TESTING_MAIN
)