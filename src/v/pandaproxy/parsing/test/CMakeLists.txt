rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_parsing_unit
  SOURCES
    from_chars.cc
    httpd.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::pandaproxy_parsing
  LABELS pandaproxy
)
