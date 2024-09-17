v_cc_library(
  NAME serde
  DEPS
    Seastar::seastar
    v::bytes
    v::hashing
    v::utils
    v::reflection
    absl::flat_hash_map
    v::compression
  )
add_subdirectory(protobuf)
add_subdirectory(test)
