rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_client
  SOURCES
    retry_with_mitigation.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::pandaproxy_client
  LABELS pandaproxy
)
