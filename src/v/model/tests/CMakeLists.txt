v_cc_library(
  NAME
    model_test_utils
  HDRS
    "random_batch.h"
  SRCS
    "random_batch.cc"
  DEPS
    v::model
    v::rprandom
    v::compression
)

rp_test(
  UNIT_TEST
  BINARY_NAME model
  SOURCES
    lexical_cast_tests.cc
    ntp_path_test.cc
    topic_view_tests.cc
    timeout_adl.cc
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
