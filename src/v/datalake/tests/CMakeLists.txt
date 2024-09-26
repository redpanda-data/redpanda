set(testdata_dir "${CMAKE_CURRENT_SOURCE_DIR}/testdata")

rp_test(
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

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME schema_protobuf
  SOURCES schema_protobuf_test.cc
  LIBRARIES
    v::gtest_main
    v::datalake
    v::iceberg_test_utils
  INPUT_FILES
    "${testdata_dir}/iceberg_ready_test_messages_edition2023.proto"
  ARGS "-- -c 1"
)
