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

