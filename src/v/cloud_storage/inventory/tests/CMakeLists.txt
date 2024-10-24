rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME inv_api
  SOURCES
    common.cc
    create_inventory_config_test.cc
    fetch_report_tests.cc
    report_parser_tests.cc
    inv_consumer_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    Boost::unit_test_framework
    v::cloud_storage
    v::gtest_main
  ARGS "-- -c 1"
  LABELS cloud_storage
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME inv_svc
  SOURCES
    common.cc
    inv_svc_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    Boost::unit_test_framework
    v::cloud_storage
    v::cluster
    v::gtest_main
  ARGS "-- -c 1"
  LABELS cloud_storage
)
