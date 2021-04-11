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
  set(oneValueArgs BINARY_NAME TIMEOUT PREPARE_COMMAND POST_COMMAND)
  set(multiValueArgs
    INCLUDES
    SOURCES
    LIBRARIES
    DEFINITIONS
    INPUT_FILES
    LABELS
    ARGS
    SKIP_BUILD_TYPES)
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

  set(files_to_copy_list "")
  foreach(i ${RP_TEST_INPUT_FILES})
    list(APPEND files_to_copy_list "--copy_file ${i}")
  endforeach()

  string(JOIN " " files_to_copy ${files_to_copy_list})

  set(prepare_command "")
  if (RP_TEST_PREPARE_COMMAND)
      set(prepare_command "--pre='${RP_TEST_PREPARE_COMMAND}'")
  endif()

  set(post_command "")
  if (RP_TEST_POST_COMMAND)
      set(post_command "--post='${RP_TEST_POST_COMMAND}'")
  endif()

  add_executable(
    ${RP_TEST_BINARY_NAME} "${RP_TEST_SOURCES}")
  target_link_libraries(
    ${RP_TEST_BINARY_NAME} PUBLIC "${RP_TEST_LIBRARIES}")

  foreach(i ${RP_TEST_INCLUDES})
    target_include_directories(${RP_TEST_BINARY_NAME} PUBLIC ${i})
  endforeach()

  foreach(i ${RP_TEST_DEFINITIONS})
    target_compile_definitions(${RP_TEST_BINARY_NAME} PRIVATE "${i}")
  endforeach()

  install(TARGETS ${RP_TEST_BINARY_NAME} DESTINATION bin)

  # all tests are compiled for every build type
  # some tests are not run for every build type
  set(skip_test FALSE)
  foreach(type ${RP_TEST_SKIP_BUILD_TYPES})
    if(CMAKE_BUILD_TYPE STREQUAL ${type})
      set(skip_test TRUE)
    endif()
  endforeach()

  if(NOT skip_test)
    foreach(SOURCE_FILE_NAME ${RP_TEST_SOURCES})
      file(READ "${SOURCE_FILE_NAME}" SOURCE_FILE_CONTENTS)
      string(REGEX
        MATCHALL "((BOOST_AUTO_)|(BOOST_DATA_)|(SEASTAR_(THREAD_)?)|(FIXTURE_))TEST(_CASE)?(_TEMPLATE)?[ \t\r\n]*\\([^\\)]*\\)"
        FOUND_TESTS ${SOURCE_FILE_CONTENTS}
      )
      foreach(HIT ${FOUND_TESTS})
        string(REGEX REPLACE "[^\\(]*\\([ \t\r\n]*([A-Za-z_0-9]+)[^\\)]*\\)" "\\1" TEST_NAME ${HIT})
        add_test(NAME "${RP_TEST_BINARY_NAME}.${TEST_NAME}" 
          COMMAND COMMAND bash -c "${RUNNER} --binary=$<TARGET_FILE:${RP_TEST_BINARY_NAME}> ${prepare_command} ${post_command} ${files_to_copy} -t ${TEST_NAME} ${RP_TEST_ARGS} "
        )
        set_tests_properties(${RP_TEST_BINARY_NAME}.${TEST_NAME} PROPERTIES LABELS "${RP_TEST_LABELS}")
        if(RP_TEST_TIMEOUT)
          set_tests_properties(${RP_TEST_BINARY_NAME}.${TEST_NAME}
            PROPERTIES TIMEOUT ${RP_TEST_TIMEOUT})
        endif()
      endforeach()
    endforeach()
  endif()
endfunction()

if(RP_ENABLE_TESTS)
  add_custom_target(check
    COMMAND ctest --output-on-failure
    DEPENDS "${UNIT_TESTS} ${INTEGRATION_TESTS} ${BENCHMARK_TESTS}")
endif()
