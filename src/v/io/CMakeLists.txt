v_cc_library(
  NAME io
  SRCS
    persistence.cc
  DEPS
    Seastar::seastar
    absl::btree
    absl::flat_hash_map
)

add_subdirectory(tests)
