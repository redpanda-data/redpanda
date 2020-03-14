
rp_test(
  UNIT_TEST
  BINARY_NAME record_batch_reader_test
  SOURCES record_batch_reader_test.cc
  LIBRARIES Seastar::seastar_testing v::model v::storage_test_utils
  LABELS model
  ARGS "-- -c 1"
)
rp_test(
  UNIT_TEST
  BINARY_NAME ntp_tests
  SOURCES ntp_path_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::model
)
rp_test(
  UNIT_TEST
  BINARY_NAME compression_lexical_cast_test
  SOURCES compression_lexical_cast_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::model
)

rp_test(
  UNIT_TEST
  BINARY_NAME record_batch_attributes_test
  SOURCES record_batch_attributes_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::model
)

rp_test(
  UNIT_TEST
  BINARY_NAME topic_view_tests
  SOURCES topic_view_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::model
)

rp_test(
  UNIT_TEST
  BINARY_NAME model_serialization_test
  SOURCES model_serialization_test.cc
  LIBRARIES Seastar::seastar_testing v::model
)
