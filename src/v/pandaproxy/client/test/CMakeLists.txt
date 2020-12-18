rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_client
  SOURCES
    produce_batcher.cc
    produce_partition.cc
    retry_with_mitigation.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::pandaproxy_client
  LABELS pandaproxy
)

rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_client_fixture
  SOURCES
    consumer_group.cc
    fetch.cc
    produce.cc
    reconnect.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::pandaproxy_client
    v::application
    v::storage_test_utils
  LABELS pandaproxy
)
