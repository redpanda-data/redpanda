rp_test(
  UNIT_TEST
  BINARY_NAME json_serialization_test
  SOURCES json_serialization_test.cc
  LIBRARIES Seastar::seastar_testing v::json

)