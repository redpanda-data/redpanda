v_cc_library(
  NAME wasm_test_fixture
  HDRS
    wasm_fixture.h
  SRCS
    "wasm_fixture.cc"
  DEPS
    v::wasm 
    Seastar::seastar
    v::model_test_utils
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME wasm_transform
  SOURCES
    wasm_transform_test.cc
    wasi_test.cc
  LIBRARIES 
    v::gtest_main
    v::wasm_test_fixture
  INPUT_FILES 
    ${CMAKE_CURRENT_SOURCE_DIR}/identity.wasm
  ARGS "-- -c 1"
  LABELS wasm
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME wasi_logs
  SOURCES
    wasi_logs_test.cc
  LIBRARIES 
    v::gtest_main
    v::wasm
  LABELS wasm
)

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

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME ffi_helpers_test
  SOURCES
    ffi_helpers_test.cc
  LIBRARIES 
    v::gtest_main
    v::wasm
  LABELS wasm
)
