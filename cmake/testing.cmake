include(CMakeParseArguments)
enable_testing()
set(RUNNER "${PROJECT_SOURCE_DIR}/tools/cmake_test.py")
option(RP_ENABLE_TESTS "Useful for disabling all tests" ON)
option(RP_ENABLE_INTEGRATION_TESTS "control if integrations are bulit and ran" ON)
option(RP_ENABLE_UNIT_TESTS "control if unit tests are bulit and ran" ON)
option(RP_ENABLE_BENCHMARK_TESTS "control if benchmarks are bulit and ran" ON)
option(RP_ENABLE_HONEY_BADGER_TESTS "control if honey_badger tests are bulit and ran" ON)
if(NOT RP_ENABLE_TESTS)
  set(RP_ENABLE_INTEGRATION_TESTS  OFF)
  set(RP_ENABLE_UNIT_TESTS  OFF)
  set(RP_ENABLE_BENCHMARK_TESTS  OFF)
  set(RP_ENABLE_HONEY_BADGER_TESTS OFF)
endif()

set(INTEGRATION_TESTS "")
set(UNIT_TESTS "")
set(BENCHMARK_TESTS "")
set(HONEY_BADGER_TESTS "")

message(STATUS "RP_ENABLE_INTEGRATION_TESTS=${RP_ENABLE_INTEGRATION_TESTS}")
message(STATUS "RP_ENABLE_UNIT_TESTS=${RP_ENABLE_UNIT_TESTS}")
message(STATUS "RP_ENABLE_BENCHMARK_TESTS=${RP_ENABLE_BENCHMARK_TESTS}")
message(STATUS "RP_ENABLE_HONEY_BADGER_TESTS=${RP_ENABLE_HONEY_BADGER_TESTS}")

function (rp_test)
  set(options
    INTEGRATION_TEST UNIT_TEST BENCHMARK_TEST HBADGER_TEST)
  set(oneValueArgs BINARY_NAME TIMEOUT)
  set(multiValueArgs
    INCLUDES
    SOURCES
    LIBRARIES
    DEFINITIONS
    INPUT_FILES
    ARGS)
  cmake_parse_arguments(RP_TEST "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  if(RP_TEST_UNIT_TEST AND RP_ENABLE_UNIT_TESTS)
    set(RP_TEST_BINARY_NAME "${RP_TEST_BINARY_NAME}_rpunit")
    set(UNIT_TESTS "${UNIT_TESTS} ${RP_TEST_BINARY_NAME}")
  endif()
  if(RP_TEST_INTEGRATION_TEST AND RP_ENABLE_INTEGRATION_TESTS)
    set(RP_TEST_BINARY_NAME "${RP_TEST_BINARY_NAME}_rpint")
    set(INTEGRATION_TESTS "${INTEGRATION_TESTS} ${RP_TEST_BINARY_NAME}")
  endif()
  if(RP_TEST_BENCHMARK_TEST AND RP_ENABLE_BENCHMARK_TESTS)
    if(CMAKE_BUILD_TYPE MATCHES Debug)
      # given a benchmark test but build is not release
      return()
    endif()
    set(RP_TEST_BINARY_NAME "${RP_TEST_BINARY_NAME}_rpbench")
    set(BENCHMARK_TESTS "${BENCHMARK_TESTS} ${RP_TEST_BINARY_NAME}")
  endif()
  if(RP_TEST_HBADGER_TEST AND RP_ENABLE_HONEY_BADGER_TESTS)
    set(RP_TEST_BINARY_NAME "${RP_TEST_BINARY_NAME}_rphbadger")
    set(HONEY_BADGER_TESTS "${HONEY_BADGER_TESTS} ${RP_TEST_BINARY_NAME}")
  endif()

  set(files_to_copy "")
  foreach(i ${RP_TEST_INPUT_FILES})
    list(APPEND files_to_copy "--copy_file ${i}")
  endforeach()


  add_executable(
    ${RP_TEST_BINARY_NAME} "${RP_TEST_SOURCES}")
  target_link_libraries(
    ${RP_TEST_BINARY_NAME} "${RP_TEST_LIBRARIES}")
  add_test (
    NAME ${RP_TEST_BINARY_NAME}
    COMMAND bash -c "${RUNNER} --binary=$<TARGET_FILE:${RP_TEST_BINARY_NAME}> --base_directory=${CMAKE_CURRENT_BINARY_DIR} ${files_to_copy} ${RP_TEST_ARGS} "
    )

  if(RP_TEST_TIMEOUT)
    set_tests_properties(${RP_TEST_BINARY_NAME}
      PROPERTIES TIMEOUT ${RP_TEST_TIMEOUT})
  endif()
  foreach(i ${RP_TEST_INCLUDES})
    target_include_directories(${RP_TEST_BINARY_NAME} PUBLIC ${i})
  endforeach()
  target_include_directories(${RP_TEST_BINARY_NAME} PUBLIC ${PROJECT_SOURCE_DIR}/src/v)
  foreach(i ${RP_TEST_DEFINITIONS})
    target_compile_definitions(${RP_TEST_BINARY_NAME} PRIVATE "${i}")
  endforeach()
    # save it to binary install dir
  install(TARGETS ${RP_TEST_BINARY_NAME} DESTINATION bin)
endfunction()

if(RP_ENABLE_TESTS)
  add_custom_target(check
    COMMAND ctest --output-on-failure
    DEPENDS "${UNIT_TESTS} ${INTEGRATION_TESTS} ${BENCHMARK_TESTS}")
endif()
