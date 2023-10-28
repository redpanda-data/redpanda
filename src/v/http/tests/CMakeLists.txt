v_cc_library(
  NAME http_test_utils
  SRCS registered_urls.cc http_imposter.cc
  DEPS Seastar::seastar_testing v::http)

rp_test(
  UNIT_TEST
  BINARY_NAME test_http_client
  SOURCES http_client_test.cc framing_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http_test_utils
  ARGS "-- -c 1"
  LABELS http
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_http_imposter
  SOURCES
  http_imposter_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http_test_utils
  LABELS http_imposter
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_http_multipart
  SOURCES
    mime_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http
  ARGS "-- -c 1"
  LABELS http
)


if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_executable(mime_fuzz_rpunit mime_fuzz.cc)
  target_compile_options(mime_fuzz_rpunit PRIVATE -g -O0 -fsanitize=address,fuzzer)
  target_link_libraries(mime_fuzz_rpunit v::http -fsanitize=address,undefined,fuzzer)
  add_test (
    NAME mime_fuzz_rpunit
    COMMAND $<TARGET_FILE:mime_fuzz_rpunit> -max_total_time=5 -rss_limit_mb=8192
  )
endif()
endif()
