v_cc_library(
  NAME
    coproc_test_utils
  HDRS
    "utils/supervisor.h"
    "utils/supervisor_test_fixture.h"
    "utils/helpers.h"
    "utils/coprocessor.h"
    "utils/coproc_test_fixture.h"
    "utils/router_test_fixture.h"
  SRCS
    utils/helpers.cc
    utils/supervisor.cc
    utils/coproc_test_fixture.cc
    utils/router_test_fixture.cc
    utils/wasm_event_generator.cc
  DEPS
    v::coproc v::storage_test_utils v::application
  )

rp_test(
  UNIT_TEST
  BINARY_NAME script_manager_unit_tests
  SOURCES script_manager_service_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::coproc_test_utils
  LABELS coproc
  )

rp_test(
  UNIT_TEST
  BINARY_NAME router_unit_tests
  SOURCES router_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::coproc_test_utils
  LABELS coproc
  )

rp_test(
  UNIT_TEST
  BINARY_NAME wasm_event_tests
  SOURCES
    wasm_event_tests.cc
    wasm_event_listener_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::coproc_test_utils
  LABELS coproc
  )

rp_test(
  UNIT_TEST
  BINARY_NAME read_materialized_topic_unit_tests
  SOURCES read_materialized_topic_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::coproc_test_utils v::kafka
  LABELS coproc
  )
