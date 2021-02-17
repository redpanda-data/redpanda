set(fixture_srcs
    utils/helpers.cc
    utils/supervisor.cc
    utils/coproc_test_fixture.cc
    utils/router_test_fixture.cc
    utils/wasm_event_generator.cc
)

set(fixture_deps
    v::coproc v::storage_test_utils v::application
)

rp_test(
  UNIT_TEST
  BINARY_NAME coproc_fixture
  SOURCES
    ${fixture_srcs}
    script_manager_service_test.cc
    router_test.cc
    offset_storage_utils_tests.cc
    wasm_event_tests.cc
    wasm_event_listener_tests.cc
    read_materialized_topic_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main ${fixture_deps}
  LABELS coproc
)
