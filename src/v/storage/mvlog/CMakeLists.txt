#enable_clang_tidy()

v_cc_library(
  NAME mvlog
  SRCS
    batch_collector.cc
    entry.cc
    logger.cc
    skipping_data_source.cc
  DEPS
    Seastar::seastar
    v::base
    v::bytes
    v::io
    v::storage
)
add_subdirectory(tests)
