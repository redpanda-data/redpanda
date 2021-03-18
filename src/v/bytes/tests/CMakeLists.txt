rp_test(
  UNIT_TEST
  BINARY_NAME test_bytes
  SOURCES iobuf_tests.cc iobuf_utils_tests.cc bytes_tests.cc
  LIBRARIES v::seastar_testing_main v::rprandom v::bytes absl::hash
  LABELS bytes
)
