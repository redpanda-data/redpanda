rp_test(
  INTEGRATION_TEST
  BINARY_NAME bmtl_rw
  SOURCES read_write.cc
  LIBRARIES bmtl_services bmtl_api
  INCLUDES ${PROJECT_SOURCE_DIR}/src/rp
  ARGS "-c 1"
)
