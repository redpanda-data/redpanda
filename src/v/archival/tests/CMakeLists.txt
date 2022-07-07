rp_test(
  UNIT_TEST
  BINARY_NAME test_archival_service
  SOURCES service_fixture.cc ntp_archiver_test.cc service_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::archival v::storage_test_utils v::cloud_roles
  ARGS "-- -c 1"
  LABELS archival
)
