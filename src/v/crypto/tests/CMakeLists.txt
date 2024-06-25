rp_test(
    UNIT_TEST
    GTEST
    BINARY_NAME gtest_crypto
    SOURCES
      digest_tests.cc
      hmac_tests.cc
      key_tests.cc
      random_tests.cc
      signature_tests.cc
    LIBRARIES
      v::crypto
      v::gtest_main
    LABELS crypto
    ARGS "-- -c 1"
)

if(Redpanda_BUILD_COMPLIANCE)
set(FIPS_BUILD_DEF "FIPS_MODULE_REQUIRED")
endif()

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_ossl_context_service
  SOURCES
    ossl_context_service_test.cc
  LIBRARIES
    v::crypto
    v::gtest_main
  DEFINITIONS
    ${FIPS_BUILD_DEF}
  LABELS crypto
  ARGS "-- -c 2"
  ENV "OPENSSL_CONF=${CMAKE_CURRENT_BINARY_DIR}/test/openssl_conf.cnf;MODULE_DIR=${REDPANDA_DEPS_INSTALL_DIR}/lib/ossl-modules"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_ossl_context_service_failures
  SOURCES
    ossl_context_service_failure_tests.cc
  LIBRARIES
    v::crypto
    v::gtest_main
  DEFINITIONS
    ${FIPS_BUILD_DEF}
  LABELS crypto
  ARGS "-- -c 2"
  ENV "OPENSSL_CONF=${CMAKE_CURRENT_BINARY_DIR}/test/openssl_conf.cnf;MODULE_DIR=${REDPANDA_DEPS_INSTALL_DIR}/lib/ossl-modules"
)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test_openssl_conf.cnf.in
               ${CMAKE_CURRENT_BINARY_DIR}/test/openssl_conf.cnf)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME crypto_bench
  SOURCES crypto_bench.cc
  LIBRARIES
    Seastar::seastar_perf_testing v::crypto v::random
  LABELS crypto
  ENV "OPENSSL_CONF=${CMAKE_CURRENT_BINARY_DIR}/test/openssl_conf.cnf;MODULE_DIR=${REDPANDA_DEPS_INSTALL_DIR}/lib/ossl-modules"
)

if(Redpanda_BUILD_COMPLIANCE)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME crypto_bench_fips
  SOURCES crypto_bench.cc
  DEFINITIONS
    PERF_FIPS_MODE
  LIBRARIES
    Seastar::seastar_perf_testing v::crypto v::random
  LABELS crypto
  ENV "OPENSSL_CONF=${CMAKE_CURRENT_BINARY_DIR}/test/openssl_conf.cnf;MODULE_DIR=${REDPANDA_DEPS_INSTALL_DIR}/lib/ossl-modules"
)
endif()