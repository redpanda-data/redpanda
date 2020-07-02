
rp_test(
  UNIT_TEST
  BINARY_NAME iobuf_test
  SOURCES iobuf_tests.cc
  LIBRARIES v::seastar_testing_main v::rprandom v::bytes
  )

rp_test(
  UNIT_TEST
  BINARY_NAME iobuf_utils_test
  SOURCES iobuf_utils_tests.cc
  LIBRARIES v::seastar_testing_main v::bytes absl::hash
)
