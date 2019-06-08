find_package(Boost REQUIRED)
find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME vint_test
  SOURCES vint_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework bytes $<TARGET_NAME_IF_EXISTS:Libcxx::libcxx>
)

rp_test(
  UNIT_TEST
  BINARY_NAME fragmented_temporary_buffer_test
  SOURCES fragmented_temporary_buffer_test.cc
  LIBRARIES Seastar::seastar_testing bytes $<TARGET_NAME_IF_EXISTS:Libcxx::libcxx>
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)
