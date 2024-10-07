rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME coordinator
  SOURCES
    state_update_test.cc
  LIBRARIES
    v::datalake_coordinator
    v::gtest_main
  LABELS datalake
  ARGS "-- -c 1"
)

