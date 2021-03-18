rp_test(
  UNIT_TEST
  BINARY_NAME arity_test
  SOURCES arity_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::reflection
  LABELS reflection
)

rp_test(
  UNIT_TEST
  BINARY_NAME reflection_async_adl_test
  SOURCES async_adl_test.cc
  LIBRARIES v::seastar_testing_main v::reflection v::bytes v::model
  LABELS reflection
)
