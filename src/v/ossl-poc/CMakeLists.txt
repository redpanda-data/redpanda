find_package(OpenSSL 3.0.9 EXACT REQUIRED)

add_executable(ossl-poc main.cc ssl_utils.cc ossl_tls_service.cc connection.cc context.cc ossl_context_service.cc)
target_link_libraries(ossl-poc PUBLIC OpenSSL::SSL Seastar::seastar v::bytes v::pandaproxy_json v::json v::storage v::kafka_protocol)

add_executable(ossl-poc-chain main-chain.cc ssl_utils.cc ossl_context_service.cc)
target_link_libraries(ossl-poc-chain PUBLIC OpenSSL::SSL Seastar::seastar)