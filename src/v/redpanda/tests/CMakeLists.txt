find_package(GTest REQUIRED CONFIG)

rp_test(
  INTEGRATION_TEST
  BINARY_NAME redpanda_rw
  SOURCES read_write.cc
  LIBRARIES redpanda_services redpanda_api
  ARGS "-c 1"
)