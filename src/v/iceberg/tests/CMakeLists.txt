find_package(Avro)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME iceberg
  SOURCES
    datatypes_test.cc
    manifest_serialization_test.cc
    datatypes_json_test.cc
  LIBRARIES
    Avro::avro
    v::bytes
    v::gtest_main
    v::iceberg
  ARGS "-- -c1"
)
