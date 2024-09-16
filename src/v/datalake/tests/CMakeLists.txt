set(testdata_dir "${CMAKE_CURRENT_SOURCE_DIR}/testdata")

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_record_multiplexer
  SOURCES gtest_record_multiplexer_test.cc
  LIBRARIES
    v::application
    v::features
    v::gtest_main
    v::kafka_test_utils
    v::datalake
    v::model_test_utils
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
    v::datalake
    v::model_test_utils
    v::iceberg_test_utils
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME parquet_writer_test
  SOURCES parquet_writer_test.cc
  LIBRARIES
    v::gtest_main
    v::datalake
    v::iceberg_test_utils
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
    v::datalake
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
    v::datalake
  LABELS datalake
  ARGS "-- -c 1"
)

target_include_directories(v_datalake_test_proto_cc_files
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
)

rp_test(
  GTEST
  BINARY_NAME batching_parquet_writer_test
  SOURCES batching_parquet_writer_test.cc
  LIBRARIES
    v::gtest_main
    v::datalake
    v::iceberg_test_utils
  LABELS storage
  ARGS "-- -c 1"
)

