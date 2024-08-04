rp_test(
    UNIT_TEST
    GTEST
    BINARY_NAME gtest_proto_to_arrow
    SOURCES
      proto_to_arrow_gtest.cc
    LIBRARIES
      v::gtest_main
      v::datalake
    LABELS datalake
    ARGS "-- -c 1"
  )
  