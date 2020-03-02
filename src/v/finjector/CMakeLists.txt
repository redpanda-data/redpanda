v_cc_library(
  NAME finjector
  SRCS hbadger.cc
  DEPS
    Seastar::seastar
    absl::flat_hash_map
  )
