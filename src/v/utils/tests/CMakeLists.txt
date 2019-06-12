find_package(Boost REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME vint_test
  SOURCES vint_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework $<TARGET_NAME_IF_EXISTS:Libcxx::libcxx>
)
