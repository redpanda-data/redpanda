rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME leb128
  SOURCES
    leb128_test.cc
  LIBRARIES 
    v::gtest_main
    v::wasm_parser
  ARGS "-- -c 1"
  LABELS wasm_parser wasm
)
