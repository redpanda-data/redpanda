rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_unit
  SOURCES
    errors.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::pandaproxy_common v::kafka_protocol
  LABELS pandaproxy
)
