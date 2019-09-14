find_package(Boost REQUIRED)
find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME vint_test
  SOURCES vint_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework bytes
)

rp_test(
  UNIT_TEST
  BINARY_NAME fragbuf_test
  SOURCES fragbuf_test.cc
  LIBRARIES Seastar::seastar_testing bytes
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME remote_test
  SOURCES remote_test.cc
  LIBRARIES Seastar::seastar_testing
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 2"
)
rp_test(
  UNIT_TEST
  BINARY_NAME string_switch_test
  SOURCES string_switch_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
)
