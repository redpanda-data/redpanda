rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME work_queue_test
  SOURCES
    work_queue_test.cc
  LIBRARIES 
    v::gtest_main
    v::wasm
  LABELS wasm
)

