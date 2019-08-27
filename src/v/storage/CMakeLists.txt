add_library(storage STATIC
    logger.cc
    log_segment.cc)

target_include_directories(storage PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(storage PUBLIC
  Seastar::seastar
  bytes
  model
  rphashing)

add_subdirectory(tests)
