
rp_test(
  UNIT_TEST
  BINARY_NAME cloud_storage_clients_single_thread
  SOURCES
    backend_detection_test.cc
    s3_client_test.cc
    xml_sax_parser_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http v::cloud_storage_clients v::cloud_roles
  ARGS "-- -c 1"
  LABELS s3
)

rp_test(
  UNIT_TEST
  BINARY_NAME cloud_storage_clients_multi_thread
  SOURCES
    client_pool_mt_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http v::cloud_storage_clients v::cloud_roles
  ARGS "-- -c 2"
  LABELS s3
)

