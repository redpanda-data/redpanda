#enable_clang_tidy()

v_cc_library(
  NAME mvlog
  SRCS
    batch_collector.cc
    entry.cc
    entry_stream_utils.cc
    logger.cc
    segment_appender.cc
    skipping_data_source.cc
  DEPS
    Seastar::seastar
    v::base
    v::bytes
    v::hashing
    v::io
    v::storage
)
add_subdirectory(tests)
