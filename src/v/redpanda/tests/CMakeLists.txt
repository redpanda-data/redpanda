rp_test(
  INTEGRATION_TEST
  BINARY_NAME redpanda_rw
  SOURCES read_write.cc
  LIBRARIES redpanda_services redpanda_api
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v
  ARGS "-c 1"
)
