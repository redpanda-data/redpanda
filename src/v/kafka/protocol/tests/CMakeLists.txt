set(KAFKA_REQUEST_GENERATOR "${CMAKE_BINARY_DIR}/src/go/kreq-gen/kafka-request-generator")

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_kafka_protocol_unit
  SOURCES
    security_test.cc
    protocol_test.cc
    protocol_utils.cc
  DEFINITIONS
    BOOST_TEST_DYN_LINK
  LIBRARIES
  Boost::unit_test_framework
    v::kafka
    v::storage_test_utils
  LABELS
    kafka
    kafka_protocol
  BUILD_DEPENDENCIES
    kafka-request-generator
  ENV
    "GENERATOR_BIN=${KAFKA_REQUEST_GENERATOR}"
)

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_kafka_protocol_single_thread
  SOURCES
    field_parser_test.cc
    batch_reader_test.cc
  DEFINITIONS
    BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::kafka
    v::storage_test_utils
  ARGS "-- -c 1"
  LABELS
    kafka
    kafka_protocol
)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
        add_executable(fuzz_kafka_request_protocol_rpfixture request_protocol_fuzz.cc protocol_utils.cc)
        add_dependencies(fuzz_kafka_request_protocol_rpfixture kafka-request-generator)
        target_compile_definitions(fuzz_kafka_request_protocol_rpfixture PRIVATE USE_CUSTOM_MUTATOR BOOST_TEST_DYN_LINK)
        target_compile_options(fuzz_kafka_request_protocol_rpfixture PRIVATE -g -O0 -fsanitize=address,fuzzer)
        target_link_libraries(fuzz_kafka_request_protocol_rpfixture v::kafka -fsanitize=address,undefined,fuzzer Boost::unit_test_framework)
        add_test(
                NAME fuzz_kafka_request_protocol_rpfixture
                COMMAND $<TARGET_FILE:fuzz_kafka_request_protocol_rpfixture> -max_total_time=30 -rss_limit_mb=8192
        )
        set_property(TEST fuzz_kafka_request_protocol_rpfixture PROPERTY ENVIRONMENT "GENERATOR_BIN=${KAFKA_REQUEST_GENERATOR}")

        add_executable(fuzz_kafka_response_protocol_rpfixture response_protocol_fuzz.cc protocol_utils.cc)
        add_dependencies(fuzz_kafka_response_protocol_rpfixture kafka-request-generator)
        target_compile_definitions(fuzz_kafka_response_protocol_rpfixture PRIVATE USE_CUSTOM_MUTATOR BOOST_TEST_DYN_LINK)
        target_compile_options(fuzz_kafka_response_protocol_rpfixture PRIVATE -g -O0 -fsanitize=address,fuzzer)
        target_link_libraries(fuzz_kafka_response_protocol_rpfixture v::kafka -fsanitize=address,undefined,fuzzer Boost::unit_test_framework)
        add_test(
                NAME fuzz_kafka_response_protocol_rpfixture
                COMMAND $<TARGET_FILE:fuzz_kafka_response_protocol_rpfixture> -max_total_time=30 -rss_limit_mb=8192
        )
        set_property(TEST fuzz_kafka_response_protocol_rpfixture PROPERTY ENVIRONMENT "GENERATOR_BIN=${KAFKA_REQUEST_GENERATOR}")
    endif()
endif()

