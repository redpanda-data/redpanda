# These tests are flaky on ARM architectures. They have been
# disabled temporarely to unblock the v22.3.1 release process.
if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^arm|aarch64")
    message(WARNING "Skipping test_archival_service_rpunit on ARM.")
else()
    rp_test(
      FIXTURE_TEST
      BINARY_NAME test_archival_service
      SOURCES
        service_fixture.cc
        ntp_archiver_test.cc
        segment_reupload_test.cc
        ntp_archiver_reupload_test.cc
        retention_strategy_test.cc
        archival_metadata_stm_test.cc
        async_data_uploader_test.cc
      DEFINITIONS BOOST_TEST_DYN_LINK
      LIBRARIES
        v::seastar_testing_main
        v::application
        v::s3_imposter
        Boost::unit_test_framework
        v::cluster
        v::storage_test_utils
        v::cloud_roles
        v::http_test_utils
        v::kafka_test_utils
      ARGS "-- -c 1"
      LABELS archival
    )

    rp_test(
      FIXTURE_TEST
      BINARY_NAME test_archiver_manager_smp
      SOURCES
        archiver_manager_test.cc
      DEFINITIONS BOOST_TEST_DYN_LINK
      LIBRARIES
        v::seastar_testing_main
        v::application
        Boost::unit_test_framework
        v::cluster
        v::storage_test_utils
        v::cloud_roles
        v::http_test_utils
      ARGS "-- -c 2"
      LABELS archival
    )

    rp_test(
      UNIT_TEST
      BINARY_NAME test_archival_service
      SOURCES
        upload_housekeeping_service_test.cc
        scrubber_scheduler_test.cc
      DEFINITIONS BOOST_TEST_DYN_LINK
      LIBRARIES
        v::seastar_testing_main
        Boost::unit_test_framework
        v::cluster
      ARGS "-- -c 1"
      LABELS archival
    )

    rp_test(
      UNIT_TEST
      GTEST
      BINARY_NAME gtest_archival
      SOURCES
        archival_metadata_stm_gtest.cc
        archiver_operations_impl_gtest.cc
        archiver_scheduler_impl_gtest.cc
      LIBRARIES
        v::raft
        v::raft_fixture
        v::cluster
        v::cloud_roles
        v::http_test_utils
        v::storage_test_utils
        v::gtest_main
      LABELS archival
      ARGS "-- -c 1"
    )
endif()
