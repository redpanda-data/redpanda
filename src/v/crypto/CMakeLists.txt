find_package(OpenSSL 3.0.9 EXACT REQUIRED)

v_cc_library(
    NAME crypto
    SRCS
      crypto.cc
      digest.cc
      hmac.cc
      ssl_utils.cc
    DEPS
      absl::node_hash_set
      v::bytes
      OpenSSL::SSL
)

add_subdirectory(test)
