v_cc_library(
  NAME io
  SRCS
    persistence.cc
    page.cc
    page_set.cc
  DEPS
    Seastar::seastar
    absl::btree
    absl::flat_hash_map
)

add_subdirectory(tests)
