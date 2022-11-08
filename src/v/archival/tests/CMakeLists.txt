# The intention here is to temporarely disable the test_upload_segments_leadership_transfer
# unit test in the ARM CI runs. Once issue #7116 is fixed, the test should be reinstated.
set(TEST_TO_IGNORE "")
if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^arm|aarch64")
    set(TEST_TO_IGNORE "--run_test=!test_upload_segments_leadership_transfer")
endif()

rp_test(
  UNIT_TEST
  BINARY_NAME test_archival_service
  SOURCES service_fixture.cc ntp_archiver_test.cc service_test.cc segment_reupload_test.cc ntp_archiver_reupload_test.cc retention_strategy_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::archival v::storage_test_utils v::cloud_roles
  ARGS "${TEST_TO_IGNORE} -- -c 1"
  LABELS archival
)
