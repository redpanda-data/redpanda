rp_test(
  UNIT_TEST
  BINARY_NAME model
  SOURCES
    compression_lexical_cast_test.cc
    ntp_path_test.cc
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
    materialized_type_test.cc
    model_serialization_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::model v::storage_test_utils
  LABELS model
  ARGS "-- -c 1"
)
