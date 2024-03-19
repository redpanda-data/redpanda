rp_test(
  UNIT_TEST
  BINARY_NAME serde
  SOURCES serde_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK SERDE_TEST
  LIBRARIES v::seastar_testing_main v::serde
  LABELS serde
)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME serde
  SOURCES bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::serde
  LABELS serde
)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(generated-structs-path ${CMAKE_CURRENT_BINARY_DIR}/generated_structs.h)
  add_custom_command(
    OUTPUT ${generated-structs-path}
    COMMAND RedpandaPython::Interpreter ${CMAKE_CURRENT_SOURCE_DIR}/struct_gen.py
    ARGS ${generated-structs-path}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/struct_gen.py
    COMMENT "Generating Fuzz Classes"
    VERBATIM)

  if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    add_executable(serde-fuzz fuzz.cc ${generated-structs-path})
    target_include_directories(serde-fuzz PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
    target_compile_options(serde-fuzz PRIVATE -g -O0 -fsanitize=address,fuzzer)
    target_link_libraries(serde-fuzz v::serde -fsanitize=address,undefined,fuzzer)

    add_test (
      NAME serde-fuzz-test
      COMMAND $<TARGET_FILE:serde-fuzz> -max_total_time=60 -rss_limit_mb=8192
    )
  endif()

  add_executable(serde-fuzz-main fuzz.cc ${generated-structs-path})
  target_include_directories(serde-fuzz-main PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
  target_compile_definitions(serde-fuzz-main PRIVATE MAIN=1)
  target_link_libraries(serde-fuzz-main v::serde)
endif()
