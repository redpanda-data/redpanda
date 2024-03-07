if (RP_OPENSSL_VERSION)
  find_package(OpenSSL ${RP_OPENSSL_VERSION} EXACT REQUIRED)
else()
  find_package(OpenSSL 3.0.0 REQUIRED)
endif()

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
    DEPS
      absl::node_hash_set
      v::bytes
      OpenSSL::SSL
)

add_subdirectory(test)
