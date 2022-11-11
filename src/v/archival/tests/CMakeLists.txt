# These tests are flaky on ARM architectures. They have been
# disabled temporarely to unblock the v22.3.1 release process.
if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^arm|aarch64")
    message(WARNING "Skipping test_archival_service_rpunit on ARM.")
else()
    rp_test(
      UNIT_TEST
      BINARY_NAME test_archival_service
      SOURCES service_fixture.cc ntp_archiver_test.cc service_test.cc segment_reupload_test.cc ntp_archiver_reupload_test.cc retention_strategy_test.cc
      DEFINITIONS BOOST_TEST_DYN_LINK
      LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::archival v::storage_test_utils v::cloud_roles
      ARGS "-- -c 1"
      LABELS archival
    )
endif()
