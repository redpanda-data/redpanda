find_package(GTest REQUIRED CONFIG)
find_package(Roaring REQUIRED)
add_library(adt INTERFACE)
target_link_libraries(adt INTERFACE rphashing Roaring::roaring)
target_include_directories(adt INTERFACE ${PROJECT_SOURCE_DIR}/src/v)

rp_test(
  UNIT_TEST
  BINARY_NAME basic_bloom_filter
  SOURCES bloom_filter_test.cc
  LIBRARIES GTest::gtest adt
)
rp_test(
  UNIT_TEST
  BINARY_NAME tagged_ptr
  SOURCES tagged_ptr_tests.cc
  LIBRARIES GTest::gtest adt
)
