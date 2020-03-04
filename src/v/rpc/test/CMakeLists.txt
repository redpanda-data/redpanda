
rp_test(
  UNIT_TEST
  BINARY_NAME rpc_serialization_tests
  SOURCES serialization_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::rpc
  LABELS rpc
)
rp_test(
  UNIT_TEST
  BINARY_NAME roundtrip_deserialize
  SOURCES roundtrip_tests.cc
  LIBRARIES Seastar::seastar_testing v::rpc
  LABELS rpc
  ARGS "-- -c 1"
)
rp_test(
  UNIT_TEST
  BINARY_NAME netbuf_tests
  SOURCES netbuf_tests.cc
  LIBRARIES Seastar::seastar_testing v::rpc
  LABELS rpc
  ARGS "-- -c 1"
)
rp_test(
  UNIT_TEST
  BINARY_NAME rpcgenerator_cycling
  SOURCES rpc_gen_cycling_test.cc
  LIBRARIES Seastar::seastar_testing v::rpc
  LABELS rpc
  INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/redpanda.crt
              ${CMAKE_CURRENT_SOURCE_DIR}/redpanda.key
              ${CMAKE_CURRENT_SOURCE_DIR}/root_certificate_authority.chain_cert
  ARGS "-- -c 1"
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME rpc_serialization
  SOURCES rpc_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::rpc
  LABELS rpc
)
rp_test(
  UNIT_TEST
  BINARY_NAME exponential_backoff
  SOURCES exponential_backoff.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::rpc
  LABELS rpc
)
rp_test(
  UNIT_TEST
  BINARY_NAME response_handler_tests
  SOURCES response_handler_tests.cc
  LIBRARIES Seastar::seastar_testing v::raft
  LABELS rpc
)

rp_test(
  UNIT_TEST
  BINARY_NAME model_serialization_test
  SOURCES model_serialization_test.cc
  LIBRARIES Seastar::seastar_testing
  LABELS rpc
)
