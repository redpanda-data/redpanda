rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME create_inv_cfg
  SOURCES
    create_inventory_config_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    Boost::unit_test_framework
    v::cloud_storage
    v::gtest_main
  ARGS "-- -c 1"
  LABELS cloud_storage
)
