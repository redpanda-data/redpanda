find_package (GTest REQUIRED CONFIG)
find_package(smf)

add_library(rprandom INTERFACE)
target_include_directories(rprandom
  INTERFACE ${PROJECT_SOURCE_DIR}/src/rp)
target_link_libraries(rprandom INTERFACE smf::smf)

rp_test(
  UNIT_TEST
  BINARY_NAME basic_fast_random_test
  SOURCES random_test.cc
  LIBRARIES GTest::gtest rprandom
)
