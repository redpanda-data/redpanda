find_package(Boost REQUIRED)
add_library(utils INTERFACE)
target_include_directories(utils INTERFACE ${PROJECT_SOURCE_DIR}/src/v)

rp_test(
  UNIT_TEST
  BINARY_NAME vint_test
  SOURCES vint_test.cc
  LIBRARIES Boost::unit_test_framework utils
)
