find_package(Avro REQUIRED)
set(testdata_dir "${CMAKE_CURRENT_SOURCE_DIR}/testdata")

v_cc_library(
  NAME datalake_test_utils
  HDRS
    record_generator.h
  SRCS
    record_generator.cc
  DEPS
    Avro::avro
    Seastar::seastar
    v::avro_test_utils
    v::schema
    v::schema_test_fixture
    v::storage
)

rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME gtest_record_multiplexer
  SOURCES gtest_record_multiplexer_test.cc
  LIBRARIES
    v::cloud_io_utils
    v::datalake_writer
    v::datalake_test_utils
    v::gtest_main
    v::model_test_utils
    v::schema
    v::s3_imposter
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_arrow_writer
  SOURCES arrow_writer_test.cc
  LIBRARIES
    v::application
    v::features
    v::gtest_main
    v::kafka_test_utils
    v::datalake_writer
    v::model_test_utils
    v::iceberg_test_utils
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME datalake
  SOURCES
    record_schema_resolver_test.cc
    parquet_writer_test.cc
    partitioning_writer_test.cc
  LIBRARIES
    v::bytes
    v::utils
    v::gtest_main
    v::datalake_writer
    v::iceberg_test_utils
    v::schema
    v::schema_test_fixture
  LABELS datalake
)

rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME datalake_cloud
  SOURCES
    record_multiplexer_test.cc
    translation_task_test.cc
  LIBRARIES
    Boost::iostreams
    v::datalake_test_utils
    v::gtest_main
    v::datalake_writer
    v::cloud_io_utils
    v::s3_imposter
  LABELS datalake
  ARGS "-- -c 1"
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
  FIXTURE_TEST
  GTEST
  BINARY_NAME translator_test
  SOURCES translator_test.cc
  LIBRARIES
    v::gtest_main
    v::datalake_translation
    v::application
    v::iceberg_test_utils
    v::kafka_test_utils
    v::model_test_utils
    v::http_test_utils
    v::s3_imposter
  LABELS datalake
  ARGS "-- -c 1"
)


set(PROTO_FILES
  testdata/person.proto
  testdata/complex.proto
  testdata/iceberg_ready_test_messages_edition2023.proto
  testdata/not_supported.proto
  testdata/proto2.proto
)

set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})


v_cc_library(
  NAME datalake_test_proto_cc_files
  SRCS ${PROTO_SRCS}
  DEPS protobuf::libprotobuf
)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME datalake_protobuf
  SOURCES 
    datalake_protobuf_tests.cc
  LIBRARIES
    v::gtest_main
    v::datalake_writer
    v::iceberg_test_utils
    v::datalake_test_proto_cc_files
  INPUT_FILES
    "${testdata_dir}/iceberg_ready_test_messages_edition2023.proto"
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

target_include_directories(v_datalake_test_proto_cc_files
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME batching_parquet_writer
  SOURCES batching_parquet_writer_test.cc
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
  USE_CWD
  BINARY_NAME datalake_avro
  SOURCES 
    datalake_avro_tests.cc
  LIBRARIES
    v::gtest_main
    v::datalake_writer
    v::iceberg_test_utils
    v::avro_test_utils
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME datalake_parquet
  SOURCES 
    datalake_parquet_tests.cc
  LIBRARIES
    v::gtest_main
    v::datalake_writer
    v::iceberg_test_utils
    v::avro_test_utils
  LABELS datalake
  ARGS "-- -c 1"
)
