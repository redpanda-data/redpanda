find_package(xxHash REQUIRED)
find_package(Crc32c REQUIRED)
add_library (rphashing STATIC murmur.cc)
target_link_libraries(rphashing PUBLIC xxHash::xxhash Crc32c::crc32c)
target_compile_definitions(rphashing PUBLIC -DXXH_PRIVATE_API)

rp_test(
  UNIT_TEST
  BINARY_NAME xx_hashes
  SOURCES xx_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework rphashing
)
