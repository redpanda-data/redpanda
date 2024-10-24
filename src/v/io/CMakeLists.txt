enable_clang_tidy()

v_cc_library(
  NAME io
  SRCS
    persistence.cc
    page.cc
    page_set.cc
    logger.cc
    io_queue.cc
    scheduler.cc
    page_cache.cc
    pager.cc
    paging_data_source.cc
  DEPS
    Seastar::seastar
    absl::btree
    absl::flat_hash_map
    v::container
    v::ssx
)

add_subdirectory(tests)
