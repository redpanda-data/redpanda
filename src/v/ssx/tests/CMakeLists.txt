rp_test(
  UNIT_TEST
  BINARY_NAME async_transforms
  SOURCES async_transforms.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::ssx
)
