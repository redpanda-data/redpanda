rp_test(
    UNIT_TEST
    GTEST
    BINARY_NAME gtest_debug_bundle
    SOURCES
      json_test.cc
      metadata_test.cc
      types_test.cc
    LIBRARIES
      v::debug_bundle
      v::features
      v::gtest_main
      v::storage
    LABELS debug_bundle
    ARGS "-- -c 1"
)

set(rpk_shim_path "${CMAKE_CURRENT_SOURCE_DIR}/rpk-shim.sh")

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_debug_bundle_service
  SOURCES
    debug_bundle_service_test.cc
  LIBRARIES
    v::debug_bundle
    v::features
    v::gtest_main
    v::storage
  LABELS debug_bundle
  ARGS "-- -c 1"
  ENV "RPK_SHIM=${rpk_shim_path}"
)
