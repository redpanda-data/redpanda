rp_test(
  UNIT_TEST
  BINARY_NAME arity_test
  SOURCES arity_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::reflection
)
