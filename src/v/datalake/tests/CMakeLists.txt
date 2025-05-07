find_package(Avro REQUIRED)

v_cc_library(
  NAME datalake_test_utils
  HDRS
    record_generator.h
    test_utils.h
  SRCS
    record_generator.cc
    test_utils.cc
  DEPS
    Avro::avro
    Seastar::seastar
    protobuf::libprotobuf
    v::avro_test_utils
    v::protobuf_test_utils
    v::schema
    v::schema_test_fixture
    v::storage
)

rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME datalake
  SOURCES
    record_schema_resolver_test.cc
    partitioning_writer_test.cc
  LIBRARIES
    v::bytes
    v::utils
    v::gtest_main
    v::datalake_test_utils
    v::datalake_writer
    v::iceberg_test_utils
    v::schema
    v::schema_test_fixture
  LABELS datalake
)

rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME catalog_schema_manager
  SOURCES catalog_schema_manager_test.cc
  LIBRARIES
    Boost::iostreams
    v::cloud_io_utils
    v::datalake_writer
    v::iceberg
    v::iceberg_test_utils
    v::gtest_main
    v::s3_imposter
  LABELS datalake
  ARGS "-- -c 1"
)



rp_test(
  GTEST
  BINARY_NAME gtest_datalake_schema_registry
  SOURCES schema_registry_test.cc
  LIBRARIES
    v::gtest_main
    v::datalake_writer
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME local_parquet_file_writer
  SOURCES local_parquet_file_writer_test.cc
  LIBRARIES
    v::gtest_main
    v::datalake_writer
    v::iceberg_test_utils
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME partition_spec_parser
  SOURCES partition_spec_parser_test.cc
  LIBRARIES
    v::gtest_main
    v::datalake_partition_spec_parser
  LABELS datalake
  ARGS "-- -c 1"
)