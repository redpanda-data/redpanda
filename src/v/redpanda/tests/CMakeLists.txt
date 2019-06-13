find_package(GTest REQUIRED CONFIG)

rp_test(
  INTEGRATION_TEST
  BINARY_NAME redpanda_rw
  SOURCES read_write.cc
  LIBRARIES redpanda_services redpanda_api
  ARGS "-c 1"
)
rp_test(
  UNIT_TEST
  BINARY_NAME redpanda_config_tests
  SOURCES config_tests.cc
  LIBRARIES GTest::gtest rpfs
)
