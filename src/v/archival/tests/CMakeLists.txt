
rp_test(
  UNIT_TEST
  BINARY_NAME test_archival_manifest
  SOURCES manifest_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::archival v::storage_test_utils
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_archival_service
  SOURCES service_fixture.cc ntp_archiver_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::archival v::storage_test_utils
  ARGS "-- -c 1"
)



