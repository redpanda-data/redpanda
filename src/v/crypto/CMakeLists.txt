find_package(OpenSSL 3.0.0 REQUIRED)

v_cc_library(
    NAME crypto
    SRCS
      crypto.cc
      digest.cc
      hmac.cc
      key.cc
      random.cc
      signature.cc
      ssl_utils.cc
      ossl_context_service.cc
    DEPS
      absl::node_hash_set
      v::bytes
      v::ssx
      OpenSSL::SSL
)

add_subdirectory(tests)
