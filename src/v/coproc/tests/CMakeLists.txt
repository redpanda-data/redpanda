set(fixture_srcs
    utils/event_publisher.cc
    utils/supervisor.cc
    utils/wasm_event_generator.cc
    fixtures/coproc_slim_fixture.cc
    fixtures/coproc_test_fixture.cc
    fixtures/coproc_bench_fixture.cc
)

set(fixture_deps
    v::coproc v::storage_test_utils v::application
)

rp_test(
  UNIT_TEST
  BINARY_NAME coproc_fixture
  SOURCES
    ${fixture_srcs}
    topic_ingestion_policy_tests.cc
    failure_recovery_tests.cc
    pacemaker_tests.cc
    coproc_bench_tests.cc
    offset_storage_utils_tests.cc
    wasm_event_tests.cc
    event_listener_tests.cc
    read_materialized_topic_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main ${fixture_deps}
  LABELS coproc
)
