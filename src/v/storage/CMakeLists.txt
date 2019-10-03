add_library(storage STATIC
    log_segment.cc
    log_manager.cc
    log.cc
    log_writer.cc
    parser.cc
    log_reader.cc
    log_replayer.cc
    probe.cc)

target_include_directories(storage PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(storage PUBLIC
  Seastar::seastar
  bytes
  model
  rphashing
  finjector)

add_subdirectory(tests)
