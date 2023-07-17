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
  BINARY_NAME wasm_transform
  SOURCES
    wasm_transform_test.cc
    wasi_test.cc
  LIBRARIES 
    v::seastar_testing_main
    v::wasm_test_fixture
  # TODO: Hookup this to the build system properly
  INPUT_FILES 
    ${CMAKE_SOURCE_DIR}/src/go/transform-sdk/internal/testdata/identity.wasm
    ${CMAKE_SOURCE_DIR}/src/go/transform-sdk/internal/testdata/transform-error.wasm
    ${CMAKE_SOURCE_DIR}/src/go/transform-sdk/internal/testdata/transform-panic.wasm
    ${CMAKE_SOURCE_DIR}/src/go/transform-sdk/internal/testdata/setup-panic.wasm
    ${CMAKE_SOURCE_DIR}/src/go/transform-sdk/internal/testdata/wasi.wasm
  ARGS "-- -c 1"
  LABELS wasm
)

rp_test(
  UNIT_TEST
  BINARY_NAME wasi_logs
  SOURCES
    wasi_logs_test.cc
  LIBRARIES 
    Boost::unit_test_framework
    v::wasm
  DEFINITIONS BOOST_TEST_DYN_LINK
  LABELS wasm
)
