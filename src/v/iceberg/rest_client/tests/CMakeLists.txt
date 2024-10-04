rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME rc_parsers
  SOURCES
    parser_tests.cc
  LIBRARIES
    v::gtest_main
    v::iceberg
  ARGS "-- -c1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME rc_retry_policy
  SOURCES
    retry_policy_tests.cc
  LIBRARIES
    v::gtest_main
    v::iceberg
  ARGS "-- -c1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME rc_catalog_client
  SOURCES
    catalog_client_tests.cc
  LIBRARIES
    v::gtest_main
    v::iceberg
  ARGS "-- -c1"
)
