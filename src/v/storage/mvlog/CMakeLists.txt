#enable_clang_tidy()

v_cc_library(
  NAME mvlog
  SRCS
    batch_collecting_stream_utils.cc
    batch_collector.cc
    entry.cc
    entry_stream.cc
    entry_stream_utils.cc
    file.cc
    logger.cc
    readable_segment.cc
    segment_appender.cc
    segment_reader.cc
    skipping_data_source.cc
    versioned_log.cc
  DEPS
    Seastar::seastar
    v::base
    v::bytes
    v::hashing
    v::io
    v::storage
)
add_subdirectory(tests)
