rp_test(
  UNIT_TEST
  BINARY_NAME parallel_transform
  SOURCES parallel_transform.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::ssx
)
