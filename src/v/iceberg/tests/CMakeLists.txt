find_package(Avro)

set(testdata_dir "${CMAKE_CURRENT_SOURCE_DIR}/testdata")

v_cc_library(
  NAME iceberg_test_utils
  HDRS
    value_generator.h
  SRCS
    value_generator.cc
  DEPS
    v::iceberg
)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME iceberg
  SOURCES
    datatypes_json_test.cc
    datatypes_test.cc
    filesystem_catalog_test.cc
    manifest_entry_type_test.cc
    manifest_file_packer_test.cc
    manifest_io_test.cc
    manifest_serialization_test.cc
    merge_append_action_test.cc
    partition_key_test.cc
    partition_key_type_test.cc
    partition_test.cc
    schema_avro_test.cc
    schema_json_test.cc
    schema_test.cc
    snapshot_json_test.cc
    struct_accessor_test.cc
    table_metadata_json_test.cc
    table_update_applier_test.cc
    test_schemas.cc
    test_table_metadata.cc
    transform_json_test.cc
    transform_utils_test.cc
    update_schema_action_test.cc
    values_test.cc
    values_avro_test.cc
    values_bytes_test.cc
    metadata_query_test.cc
  LIBRARIES
    Avro::avro
    Boost::iostreams
    v::bytes
    v::gtest_main
    v::iceberg
    v::utils
    v::cloud_io
    v::cloud_io_utils
    v::s3_imposter
    v::iceberg_test_utils
  INPUT_FILES
    "${testdata_dir}/nested_manifest.avro"
  ARGS "-- -c1"
)
