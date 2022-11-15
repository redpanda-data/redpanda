v_cc_library(
  NAME security
  SRCS
    ephemeral_credential.cc
    scram_algorithm.cc
    scram_credential.cc
    scram_authenticator.cc
    acl_store.cc
    mtls.cc
    license.cc
  DEPS
    v::bytes
    v::utils
    absl::flat_hash_map
    absl::flat_hash_set
    cryptopp
    re2
 )

add_subdirectory(tests)
