v_cc_library(
  NAME random
  SRCS
    generators.cc
  DEPS
    absl::random_seed_sequences
    Seastar::seastar
)

add_subdirectory(tests)
