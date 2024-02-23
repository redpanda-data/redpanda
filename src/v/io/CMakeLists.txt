enable_clang_tidy()

v_cc_library(
  NAME io
  SRCS
    persistence.cc
    page.cc
    page_set.cc
    clang-tidy-helper.cc
    logger.cc
    io_queue.cc
    scheduler.cc
    page_cache.cc
    pager.cc
  DEPS
    Seastar::seastar
    absl::btree
    absl::flat_hash_map
    v::container
)

add_subdirectory(tests)
