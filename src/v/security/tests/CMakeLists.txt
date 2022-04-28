rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_security
  SOURCES
    scram_algorithm_test.cc
    credential_store_test.cc
    authorizer_test.cc
    mtls_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka
  LABELS kafka
)
