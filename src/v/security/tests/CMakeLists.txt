rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_security
  SOURCES
    scram_algorithm_test.cc
    credential_store_test.cc
    authorizer_test.cc
    mtls_test.cc
    license_test.cc
    gssapi_principal_mapper_test.cc
    jwt_test.cc
    url_test.cc
    oidc_principal_mapping_test.cc
    role_store_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka_protocol v::storage v::security
  LABELS kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_security_single_thread
  SOURCES
    ephemeral_credential_store_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::security
  ARGS "-- -c 1"
  LABELS
    kafka
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME role_store_bench
  SOURCES role_store_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::utils v::security
  LABELS security
)
