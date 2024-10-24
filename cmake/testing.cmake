include(CMakeParseArguments)
enable_testing()
set(RUNNER "${PROJECT_SOURCE_DIR}/tools/cmake_test.py")
option(RP_ENABLE_TESTS "Useful for disabling all tests" ON)
option(RP_ENABLE_FIXTURE_TESTS "control if integrations are built and run" ON)
option(RP_ENABLE_UNIT_TESTS "control if unit tests are built and run" ON)
option(RP_ENABLE_BENCHMARK_TESTS "control if benchmarks are built and run" ON)
if(NOT RP_ENABLE_TESTS)
  set(RP_ENABLE_FIXTURE_TESTS  OFF)
  set(RP_ENABLE_UNIT_TESTS  OFF)
  set(RP_ENABLE_BENCHMARK_TESTS  OFF)
endif()

set(FIXTURE_TESTS "")
set(UNIT_TESTS "")
set(BENCHMARK_TESTS "")

message(STATUS "RP_ENABLE_FIXTURE_TESTS=${RP_ENABLE_FIXTURE_TESTS}")
message(STATUS "RP_ENABLE_UNIT_TESTS=${RP_ENABLE_UNIT_TESTS}")
message(STATUS "RP_ENABLE_BENCHMARK_TESTS=${RP_ENABLE_BENCHMARK_TESTS}")

function (rp_test)
  set(options
    FIXTURE_TEST UNIT_TEST BENCHMARK_TEST GTEST USE_CWD)
  set(oneValueArgs BINARY_NAME TIMEOUT PREPARE_COMMAND POST_COMMAND)
  set(multiValueArgs
    INCLUDES
    SOURCES
    LIBRARIES
    DEFINITIONS
    INPUT_FILES
    BUILD_DEPENDENCIES
    ENV
    LABELS
    ARGS
    SKIP_BUILD_TYPES)
  cmake_parse_arguments(RP_TEST "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  if(RP_TEST_UNIT_TEST AND RP_ENABLE_UNIT_TESTS)
    set(RP_TEST_BINARY_NAME "${RP_TEST_BINARY_NAME}_rpunit")
    set(UNIT_TESTS "${UNIT_TESTS} ${RP_TEST_BINARY_NAME}")
  endif()
  if(RP_TEST_FIXTURE_TEST AND RP_ENABLE_FIXTURE_TESTS)
    set(RP_TEST_BINARY_NAME "${RP_TEST_BINARY_NAME}_rpfixture")
    set(FIXTURE_TESTS "${FIXTURE_TESTS} ${RP_TEST_BINARY_NAME}")
  endif()
  if(RP_TEST_BENCHMARK_TEST AND RP_ENABLE_BENCHMARK_TESTS)
    if(CMAKE_BUILD_TYPE MATCHES Debug)
      # given a benchmark test but build is not release
      return()
    endif()
    set(RP_TEST_BINARY_NAME "${RP_TEST_BINARY_NAME}_rpbench")
    set(BENCHMARK_TESTS "${BENCHMARK_TESTS} ${RP_TEST_BINARY_NAME}")
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
  if (RP_TEST_BUILD_DEPENDENCIES)
    add_dependencies(${RP_TEST_BINARY_NAME} ${RP_TEST_BUILD_DEPENDENCIES})
  endif()

  foreach(i ${RP_TEST_INCLUDES})
    target_include_directories(${RP_TEST_BINARY_NAME} PUBLIC ${i})
  endforeach()

  foreach(i ${RP_TEST_DEFINITIONS})
    target_compile_definitions(${RP_TEST_BINARY_NAME} PRIVATE "${i}")
  endforeach()
  if (RP_TEST_GTEST)
    target_compile_definitions(${RP_TEST_BINARY_NAME} PRIVATE "IS_GTEST")
  endif()
  target_compile_options(${RP_TEST_BINARY_NAME} PRIVATE -Wall -Wextra -Werror -Wno-missing-field-initializers -Wno-unused-parameter -Wno-sign-compare)

  install(TARGETS ${RP_TEST_BINARY_NAME} DESTINATION bin)

  # all tests are compiled for every build type
  # some tests are not run for every build type
  set(skip_test FALSE)
  foreach(type ${RP_TEST_SKIP_BUILD_TYPES})
    if(CMAKE_BUILD_TYPE STREQUAL ${type})
      set(skip_test TRUE)
    endif()
  endforeach()

  if(RP_TEST_UNIT_TEST)
  if(NOT RP_TEST_ARGS)
    # For tests that don't set some explicit args (some of them do a -c 1), set
    # an explicit core count, to avoid unit tests running differently on machines
    # with different core counts (this also speeds up some tests running on many-core
    # machines.
    set(RP_TEST_ARGS "-- -c 4")
  endif()
  endif()

  if(RP_TEST_BENCHMARK_TEST)
    if(NOT RP_TEST_ARGS)
      # For tests that don't set some explicit args (some of them do a -c 1), set
      # an explicit core count, to avoid unit tests running differently on machines
      # with different core counts (this also speeds up some tests running on many-core
      # machines.
      set(RP_TEST_ARGS "-c 1")
    endif()
  endif()

  set(gtest_option "")
  if (RP_TEST_GTEST)
      set(gtest_option "--gtest")
  endif()

  set(root_option "--root /dev/shm/vectorized_io")
  if (RP_TEST_USE_CWD)
      set(root_option "")
  endif()

  if(NOT skip_test)
    add_test (
      NAME ${RP_TEST_BINARY_NAME}
      COMMAND bash -c "${RUNNER} --binary=$<TARGET_FILE:${RP_TEST_BINARY_NAME}> ${gtest_option} ${root_option} ${prepare_command} ${post_command} ${files_to_copy} ${RP_TEST_ARGS} "
      )
    set_tests_properties(${RP_TEST_BINARY_NAME} PROPERTIES LABELS "${RP_TEST_LABELS}")
    if(RP_TEST_TIMEOUT)
      set_tests_properties(${RP_TEST_BINARY_NAME}
        PROPERTIES TIMEOUT ${RP_TEST_TIMEOUT})
    endif()
    set_property(TEST ${RP_TEST_BINARY_NAME} PROPERTY ENVIRONMENT "${RP_TEST_ENV}")
    if(RP_TEST_FIXTURE_TEST)
      set_property(TEST ${RP_TEST_BINARY_NAME} APPEND PROPERTY ENVIRONMENT "RP_FIXTURE_ENV=1")
    endif()
  endif()
endfunction()

if(RP_ENABLE_TESTS)
  add_custom_target(check
    COMMAND ctest --output-on-failure
    DEPENDS "${UNIT_TESTS} ${FIXTURE_TESTS} ${BENCHMARK_TESTS}")
endif()

if (RP_ENABLE_TESTS)
  find_program(OPENSSL openssl)
  if (NOT OPENSSL)
    message(FATAL_ERROR "openssl is required for performing tests!")
  endif()

  set(OPENSSL_ENV)

  if(VECTORIZED_CMAKE_DIR)
    set(OPENSSL_ENV "LD_LIBRARY_PATH=${REDPANDA_DEPS_INSTALL_DIR}/lib;OPENSSL_CONF=${REDPANDA_DEPS_INSTALL_DIR}/etc/ssl/openssl.cnf")
  endif()

  # The following function can be used to setup a simple CA
  # Arguments:
  #   name (required) - Name to use for output files (crt, key, and csr)
  #   CA (optional) - If provided, will use specified name as the signing
  #                   CA, if not provided will generate self-signed certificate
  #   SERIAL_NUM (required if CA provided) - Serial number of cert when issued by CA
  #   COMMON_NAME (required) - The CN to use when setting the subject name
  #
  # This function was inspired by, and heavily copied from, a function foun in
  # ScyllaDB's Seastar:
  # https://github.com/scylladb/seastar/blob/998d29970b9207c4ba8a125684f883c363d1010a/tests/unit/CMakeLists.txt#L574-L629
  function(redpanda_sign_cert name)
    cmake_parse_arguments(CERT
    ""
    "CA;SERIAL_NUM;COMMON_NAME"
    ""
    ${ARGN})
    set(cert ${name}.crt)
    set(privkey ${name}.key)
    set(extension "subjectAltName = IP:127.0.0.1")
    set(subj "/C=US/ST=California/L=San Francisco/O=Redpanda Data/OU=Core/CN=${CERT_COMMON_NAME}")

    # The following command generates an ECDSA key pair using the prime256v1 curve
    add_custom_command(OUTPUT ${privkey}
      COMMAND ${CMAKE_COMMAND} -E env ${OPENSSL_ENV}
        ${OPENSSL} ecparam
        -name prime256v1 -genkey -noout
        -out ${privkey}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

      if(DEFINED CERT_CA)
        set(req ${name}.csr)
        # This will generate a certificate signing request (CSR) using the generated
        # private key
        add_custom_command(OUTPUT ${req}
          COMMAND ${CMAKE_COMMAND} -E env ${OPENSSL_ENV}
            ${OPENSSL} req
            -new -sha256
            -key ${privkey}
            -out ${req}
            -subj ${subj}
          DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${privkey}
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

        set(ca_cert ${CERT_CA}.crt)
        set(ca_privkey ${CERT_CA}.key)
        # This function will sign the generated CSR (above) with the specified CA
        add_custom_command(OUTPUT ${cert}
          COMMAND ${CMAKE_COMMAND} -E env ${OPENSSL_ENV}
            ${OPENSSL} x509
            -req -days 1000 -sha256
            -set_serial ${CERT_SERIAL_NUM}
            -in ${req}
            -CA ${ca_cert}
            -CAkey ${ca_privkey}
            -out ${cert}
          DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${req} ${CMAKE_CURRENT_BINARY_DIR}/${ca_cert} ${CMAKE_CURRENT_BINARY_DIR}/${ca_privkey}
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
      else()
        # This will generate a self-signed certificate to use as the root CA
        add_custom_command(OUTPUT ${cert}
          COMMAND ${CMAKE_COMMAND} -E env ${OPENSSL_ENV}
            ${OPENSSL} req
            -new -x509 -sha256
            -key ${privkey}
            -out ${cert}
            -subj ${subj}
            -addext ${extension}
          DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${privkey}
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
      endif()
  endfunction(redpanda_sign_cert)
endif()
