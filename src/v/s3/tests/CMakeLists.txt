
rp_test(
  UNIT_TEST
  BINARY_NAME test_aws_signature
  SOURCES signature_test.cc 
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::http v::s3
  ARGS "-- -c 1"
)
