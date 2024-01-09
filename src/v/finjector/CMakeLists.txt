v_cc_library(
  NAME finjector
  SRCS
    hbadger.cc
    stress_fiber.cc
  DEPS
    Seastar::seastar
    absl::flat_hash_map
    v::random
  )
