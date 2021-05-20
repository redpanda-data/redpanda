rp_test(
  UNIT_TEST
  BINARY_NAME v8_instance
  SOURCES
    instance_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::v8_engine
  ARGS "-- -c 1"
  LABELS v8_engine
)
