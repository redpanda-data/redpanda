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
    v::gtest_main
    v::schema_test_fixture
)

set(TESTDATA_DIR "${CMAKE_BINARY_DIR}/src/transform-sdk/go/transform/internal/testdata")

find_package(Avro)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME wasm_transform
  SOURCES
    wasm_transform_test.cc
    wasi_test.cc
    wasm_sr_test.cc
    wasm_lifecycle_test.cc
    wasm_resources_test.cc
  LIBRARIES 
    v::gtest_main
    v::wasm_test_fixture
    Avro::avro
  BUILD_DEPENDENCIES
    wasm_testdata_dynamic
    wasm_testdata_identity
    wasm_testdata_schema_registry
    wasm_testdata_setup_panic
    wasm_testdata_transform_error
    wasm_testdata_transform_panic
    wasm_testdata_wasi
  INPUT_FILES
    "${TESTDATA_DIR}/dynamic.wasm"
    "${TESTDATA_DIR}/identity.wasm"
    "${TESTDATA_DIR}/schema-registry.wasm"
    "${TESTDATA_DIR}/setup-panic.wasm"
    "${TESTDATA_DIR}/transform-error.wasm"
    "${TESTDATA_DIR}/transform-panic.wasm"
    "${TESTDATA_DIR}/wasi.wasm"
  ARGS "-- -c 1"
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
  ARGS "-- -c 1"
  LABELS wasm
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME wasm_cache
  SOURCES
    wasm_cache_test.cc
  LIBRARIES 
    v::gtest_main
    v::model_test_utils
    v::wasm
  ARGS "-- -c 4"
  LABELS wasm
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME wasm_probe
  SOURCES
    wasm_probe_test.cc
  LIBRARIES 
    v::gtest_main
    v::wasm
  ARGS "-- -c 1"
  LABELS wasm
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME wasm_allocator
  SOURCES
    wasm_allocator_test.cc
  LIBRARIES 
    v::gtest_main
    v::wasm
  ARGS "-- -c 1"
  LABELS wasm
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME wasm_transform
  SOURCES wasm_transform_bench.cc
  LIBRARIES 
    Seastar::seastar_perf_testing
    v::seastar_testing_main
    v::wasm 
    v::model_test_utils
  BUILD_DEPENDENCIES
    wasm_testdata_identity
  INPUT_FILES
    "${TESTDATA_DIR}/identity.wasm"
  LABELS
    wasm 
)
