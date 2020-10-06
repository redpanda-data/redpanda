include(rpcgen)
rpcgen(
  TARGET coproc_mgmnt_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/script_manager.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

rpcgen(
  TARGET coproc_engine_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen_engine.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/supervisor.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )


v_cc_library(
  NAME coproc
  SRCS
    types.cc
    logger.cc
    service.cc
    router.cc
  DEPS
    v::rpc
    v::model
    v::storage
    v::reflection
    coproc_mgmnt_gen
    coproc_engine_gen
  )

rp_test(
  UNIT_TEST
  BINARY_NAME coproc_unit_tests
  SOURCES
    tests/script_manager_service_test.cc
    tests/supervisor.cc
    tests/coproc_fixture_base.cc
    tests/router_test.cc
    tests/utils.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::coproc v::storage_test_utils
  )
