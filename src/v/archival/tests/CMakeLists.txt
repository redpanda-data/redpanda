
rp_test(
  UNIT_TEST
  BINARY_NAME test_arch_service
  SOURCES manifest_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::archival v::storage_test_utils
  ARGS "-- -c 1"
)


