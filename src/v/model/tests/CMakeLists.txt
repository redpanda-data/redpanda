v_cc_library(
  NAME
    model_test_utils
  HDRS
    "random_batch.h"
  SRCS
    "random_batch.cc"
  DEPS
    v::model
    v::random
    v::compression
    v::bytes
    v::bytes_random
)

rp_test(
  UNIT_TEST
  BINARY_NAME model
  SOURCES
    ktp_test.cc
    lexical_cast_tests.cc
    ntp_path_test.cc
    timeout_adl.cc
    topic_view_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::model
  LABELS model
)

rp_test(
  UNIT_TEST
  BINARY_NAME model_thread
  SOURCES
    record_batch_test.cc
    record_batch_reader_test.cc
    model_serialization_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::model v::model_test_utils
  LABELS model
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME transform_models
  SOURCES
    transform_test.cc
  LIBRARIES 
    v::model_test_utils
    v::gtest_main
    v::model
  LABELS model
)
