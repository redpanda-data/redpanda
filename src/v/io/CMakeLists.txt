enable_clang_tidy()

v_cc_library(
  NAME io
  SRCS
    persistence.cc
    page.cc
    page_set.cc
    clang-tidy-helper.cc
    logger.cc
  DEPS
    Seastar::seastar
    absl::btree
    absl::flat_hash_map
)

add_subdirectory(tests)
