find_package(Boost REQUIRED)

add_library(rprandom INTERFACE)
target_include_directories(rprandom
  INTERFACE ${PROJECT_SOURCE_DIR}/src/v)

rp_test(
  UNIT_TEST
  BINARY_NAME basic_fast_random_test
  SOURCES random_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework rprandom
)
