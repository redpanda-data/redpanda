find_package(xxHash REQUIRED)
add_library (rphashing STATIC murmur.cc)
target_include_directories(rphashing PUBLIC ${PROJECT_SOURCE_DIR}/src/v)
target_link_libraries(rphashing PUBLIC xxHash::xxhash)
target_compile_definitions(rphashing PUBLIC -DXXH_PRIVATE_API)

rp_test(
  UNIT_TEST
  BINARY_NAME xx_hashes
  SOURCES xx_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework rphashing
)
