rp_test(
  UNIT_TEST
  BINARY_NAME cluster_cloud_metadata
  SOURCES
    cluster_manifest_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    Boost::unit_test_framework
    v::seastar_testing_main
    v::bytes
    v::cluster
  ARGS "-- -c 1"
  LABELS cluster
)

rp_test(
  FIXTURE_TEST
  BINARY_NAME cluster_cloud_metadata
  SOURCES
    controller_snapshot_reconciliation_test.cc
    manifest_downloads_test.cc
    offsets_lookup_batcher_test.cc
    offsets_lookup_test.cc
    offsets_recovery_test.cc
    uploader_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    Boost::unit_test_framework
    v::application
    v::cluster
    v::kafka_test_utils
    v::seastar_testing_main
    v::storage
    v::s3_imposter
  ARGS "-- -c 1"
  LABELS cluster
)


rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME gtest_cluster_cloud_metadata
  SOURCES
    cluster_recovery_backend_test.cc
    producer_id_recovery_test.cc
  LIBRARIES
    v::application
    v::kafka_test_utils
    v::gtest_main
    v::s3_imposter
    v::storage_test_utils
  ARGS "-- -c 1"
  LABELS cluster
)
