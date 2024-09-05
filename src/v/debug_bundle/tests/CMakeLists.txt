rp_test(
    UNIT_TEST
    GTEST
    BINARY_NAME gtest_debug_bundle
    SOURCES
      types_test.cc
    LIBRARIES
      v::debug_bundle
      v::gtest_main
    LABELS debug_bundle
    ARGS "-- -c 1"
)
