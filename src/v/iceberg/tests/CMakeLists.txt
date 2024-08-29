find_package(Avro)

set(testdata_dir "${CMAKE_CURRENT_SOURCE_DIR}/testdata")

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME iceberg
  SOURCES
    datatypes_json_test.cc
    datatypes_test.cc
    manifest_serialization_test.cc
    partition_key_test.cc
    partition_key_type_test.cc
    partition_test.cc
    schema_json_test.cc
    schema_test.cc
    snapshot_json_test.cc
    struct_accessor_test.cc
    test_schemas.cc
    transform_json_test.cc
    transform_utils_test.cc
    values_test.cc
    value_generator.cc
  LIBRARIES
    Avro::avro
    Boost::iostreams
    v::bytes
    v::gtest_main
    v::iceberg
    v::utils
  INPUT_FILES
    "${testdata_dir}/nested_manifest.avro"
  ARGS "-- -c1"
)
