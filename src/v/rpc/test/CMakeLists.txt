include(rpcgen)
rpcgen(
  TARGET cycling_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/cycling_service.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/cycling_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

rpcgen(
  TARGET echo_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/echo_service.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/echo_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

rpcgen(
  TARGET echo_v2_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/echo_v2_service.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/echo_v2_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

v_cc_library(
  NAME
    rpc_testing
  HDRS
    "test/rpc_integration_fixture.h"
  DEPS
    echo_gen
    echo_v2_gen
    cycling_gen
  )

rp_test(
  UNIT_TEST
  BINARY_NAME rpc
  SOURCES
    netbuf_tests.cc
    roundtrip_tests.cc
    response_handler_tests.cc
    serialization_test.cc
  LIBRARIES v::seastar_testing_main v::rpc
  LABELS rpc
  ARGS "-- -c 1"
)

set(cert_ca "root_certificate_authority")
set(other_cert_ca "root_certificate_authority.other")
set(cert "redpanda")
set(other_cert "redpanda.other")

redpanda_sign_cert(${cert_ca}
  COMMON_NAME "redpanda.com")

redpanda_sign_cert(${other_cert_ca}
  COMMON_NAME "redpanda.other.com")

redpanda_sign_cert(${cert}
  CA ${cert_ca}
  SERIAL_NUM 1
  COMMON_NAME "cert.com")

redpanda_sign_cert(${other_cert}
  CA ${other_cert_ca}
  SERIAL_NUM 2
  COMMON_NAME "cert.other.com")

add_custom_target(rpcgenerator_cycling_test_certs
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${cert_ca}.crt
          ${CMAKE_CURRENT_BINARY_DIR}/${other_cert_ca}.crt
          ${CMAKE_CURRENT_BINARY_DIR}/${cert}.crt
          ${CMAKE_CURRENT_BINARY_DIR}/${other_cert}.crt)
rp_test(
  UNIT_TEST
  BINARY_NAME rpcgenerator_cycling
  SOURCES rpc_gen_cycling_test.cc
  LIBRARIES v::seastar_testing_main v::rpc_testing
  LABELS rpc
  INPUT_FILES ${CMAKE_CURRENT_BINARY_DIR}/redpanda.crt
              ${CMAKE_CURRENT_BINARY_DIR}/redpanda.key
              ${CMAKE_CURRENT_BINARY_DIR}/root_certificate_authority.crt
              ${CMAKE_CURRENT_BINARY_DIR}/redpanda.other.crt
              ${CMAKE_CURRENT_BINARY_DIR}/redpanda.other.key
              ${CMAKE_CURRENT_BINARY_DIR}/root_certificate_authority.other.crt
  ARGS "-- -c 1"
  BUILD_DEPENDENCIES rpcgenerator_cycling_test_certs
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
  BINARY_NAME connection_cache
  SOURCES connection_cache_test.cc
  LIBRARIES v::seastar_testing_main v::rpc
  LABELS rpc
  ARGS "-- -c 1"
)
