include(rpcgen)
rpcgen(
  TARGET coproc_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/registration.h
  INCLUDES {$CMAKE_BINARY_DIR}/src/v
  )

v_cc_library(
  NAME coproc
  SRCS
    types.cc
    logger.cc
    service.cc
  DEPS
    v::rpc
    v::model
    v::storage
    coproc_gen
  )

rp_test(
  UNIT_TEST
  BINARY_NAME registration_tests
  SOURCES
    tests/registration_test.cc
    tests/materialized_type_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::coproc
  )

