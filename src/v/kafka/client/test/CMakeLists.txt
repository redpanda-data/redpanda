rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_client_unit
  SOURCES
    partitioners.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka_client
  LABELS kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_client_single_thread
  SOURCES
    fetch_session.cc
    produce_batcher.cc
    produce_partition.cc
    retry_with_mitigation.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::kafka_client
  ARGS "-- -c 1"
  LABELS kafka
)

v_cc_library(
  NAME kc_test_utils
  HDRS
    utils.h
  SRCS
    utils.cc
  DEPS
    v::utils
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME test_kc_record_batcher
  SOURCES
    record_batcher_test.cc
  LIBRARIES
    v::gtest_main
    v::kc_record_batcher
    v::kc_test_utils
  LABELS kafka
)

rp_test(
  FIXTURE_TEST
  BINARY_NAME kafka_client
  SOURCES
    consumer_group.cc
    fetch.cc
    produce.cc
    reconnect.cc
    retry.cc
    test_config_utils.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::kafka_client
    v::application
    v::storage_test_utils
  LABELS kafka
)
