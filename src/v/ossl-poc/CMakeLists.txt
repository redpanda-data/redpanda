find_package(OpenSSL 3.0.9 EXACT REQUIRED)

add_executable(ossl-poc main.cc ssl_utils.cc ossl_tls_service.cc connection.cc context.cc)
target_link_libraries(ossl-poc PUBLIC OpenSSL::SSL Seastar::seastar)