rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_client
  SOURCES
    fetch_session.cc
    produce_batcher.cc
    produce_partition.cc
    retry_with_mitigation.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::kafka_client
  LABELS kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_client_fixture
  SOURCES
    consumer_group.cc
    fetch.cc
    produce.cc
    reconnect.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::kafka_client
    v::application
    v::storage_test_utils
  LABELS kafka
)
