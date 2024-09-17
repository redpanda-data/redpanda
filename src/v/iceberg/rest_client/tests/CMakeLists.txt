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
