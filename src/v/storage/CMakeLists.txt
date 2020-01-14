add_library(storage STATIC
    log_segment_reader.cc
    log_manager.cc
    mem_log_impl.cc
    disk_log_impl.cc
    log_writer.cc
    parser.cc
    log_reader.cc
    log_replayer.cc
    probe.cc
    record_batch_builder.cc
    logger.cc
    log_segment_appender.cc
    )

target_link_libraries(storage PUBLIC
  Seastar::seastar
  bytes
  config
  rphashing
  finjector)

add_subdirectory(tests)
