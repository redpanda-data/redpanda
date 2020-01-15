
rp_test(
  UNIT_TEST
  BINARY_NAME iobuf_test
  SOURCES iobuf_tests.cc
  LIBRARIES Seastar::seastar_testing v::rprandom v::bytes
  )

rp_test(
  UNIT_TEST
  BINARY_NAME iobuf_utils_test
  SOURCES iobuf_utils_tests.cc
  LIBRARIES Seastar::seastar_testing v::bytes
)
