add_library(storage STATIC
    logger.cc
    log_segment.cc
    log_manager.cc
    log.cc
    log_writer.cc
    parser.cc)

target_include_directories(storage PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(storage PUBLIC
  Seastar::seastar
  bytes
  model
  rphashing)

add_subdirectory(tests)
