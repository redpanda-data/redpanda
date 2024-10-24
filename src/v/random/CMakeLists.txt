v_cc_library(
  NAME random
  SRCS
    generators.cc
  DEPS
    absl::random_seed_sequences
    Seastar::seastar
    v::crypto
)

add_subdirectory(tests)
