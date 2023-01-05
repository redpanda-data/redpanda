v_cc_library(
  NAME rprandom
  HRDS
    fast_prng.h
    generators.h
  SRCS
    "generators.cc"
  DEPS
    v::bytes
    absl::random_seed_sequences
    Seastar::seastar
)

add_subdirectory(tests)
