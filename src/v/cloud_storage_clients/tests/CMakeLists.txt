
rp_test(
  UNIT_TEST
  BINARY_NAME s3_single_thread
  SOURCES
    s3_client_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http v::cloud_storage_clients v::cloud_roles
  ARGS "-- -c 1"
  LABELS s3
)
