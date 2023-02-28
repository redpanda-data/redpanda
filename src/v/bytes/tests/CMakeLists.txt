rp_test(
  UNIT_TEST
  BINARY_NAME test_bytes
  SOURCES iobuf_tests.cc iobuf_utils_tests.cc bytes_tests.cc
  LIBRARIES v::seastar_testing_main v::rprandom v::bytes absl::hash
  LABELS bytes
)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_executable(iobuf_fuzz_rpfixture iobuf_fuzz.cc)
  target_compile_options(iobuf_fuzz_rpfixture PRIVATE -g -O0 -fsanitize=address,fuzzer)
  target_link_libraries(iobuf_fuzz_rpfixture v::serde -fsanitize=address,undefined,fuzzer)
  add_test (
    NAME iobuf_fuzz_rpfixture
    COMMAND $<TARGET_FILE:iobuf_fuzz_rpfixture> -max_total_time=30 -rss_limit_mb=8192
  )
endif()
endif()
