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
    manifest_downloads_test.cc
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
