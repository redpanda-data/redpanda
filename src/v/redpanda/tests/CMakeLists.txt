find_package(GTest REQUIRED CONFIG)

rp_test(
  INTEGRATION_TEST
  BINARY_NAME redpanda_rw
  SOURCES read_write.cc
  LIBRARIES redpanda_services redpanda_api
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v
  ARGS "-c 1"
)
rp_test(
  UNIT_TEST
  BINARY_NAME redpanda_config_tests
  SOURCES config_tests.cc
  LIBRARIES GTest::gtest rpfs
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v
)
