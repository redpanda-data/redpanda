rp_test(
  UNIT_TEST
  BINARY_NAME net_connection_rate
  SOURCES
    connection_rate_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::net
  ARGS "-- -c 1"
  LABELS net
)
