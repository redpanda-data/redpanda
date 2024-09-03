rp_test(
    UNIT_TEST
    GTEST
    BINARY_NAME gtest_proto_to_arrow
    SOURCES
      proto_to_arrow_gtest.cc
    LIBRARIES
      v::gtest_main
      v::datalake
    LABELS datalake
    ARGS "-- -c 1"
)

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
  LABELS storage
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
  LABELS storage
  ARGS "-- -c 1"
)