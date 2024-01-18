rp_test(
  UNIT_TEST
  BINARY_NAME strings
  SOURCES
    constexpr_string_switch.cc
    string_switch_test.cc
    utf8_control_chars.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::strings
)
