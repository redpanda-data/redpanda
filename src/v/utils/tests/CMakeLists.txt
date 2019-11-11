find_package(Boost REQUIRED)
find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME vint_test
  SOURCES vint_test.cc
  LIBRARIES Seastar::seastar_testing bytes
)

rp_test(
  UNIT_TEST
  BINARY_NAME remote_test
  SOURCES remote_test.cc
  LIBRARIES Seastar::seastar_testing

  ARGS "-- -c 2"
)
rp_test(
  UNIT_TEST
  BINARY_NAME string_switch_test
  SOURCES string_switch_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
)
rp_test(
  UNIT_TEST
  BINARY_NAME named_type_test
  SOURCES named_type_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
  )
rp_test(
  UNIT_TEST
  BINARY_NAME constexpr_string_switch
  SOURCES constexpr_string_switch.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
  )
