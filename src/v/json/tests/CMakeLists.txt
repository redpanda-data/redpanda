rp_test(
  UNIT_TEST
  BINARY_NAME json_serialization_test
  SOURCES json_serialization_test.cc
  LIBRARIES v::seastar_testing_main v::json
  LABELS json
)