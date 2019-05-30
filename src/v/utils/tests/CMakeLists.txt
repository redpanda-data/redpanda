find_package(Boost REQUIRED)
add_library(utils INTERFACE)
target_include_directories(utils INTERFACE ${PROJECT_SOURCE_DIR}/src/v)

rp_test(
  UNIT_TEST
  BINARY_NAME vint_test
  SOURCES vint_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework utils $<TARGET_NAME_IF_EXISTS:Libcxx::libcxx>
)
