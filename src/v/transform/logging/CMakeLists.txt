v_cc_library(
  NAME transform_logging
  HDRS
    record_batcher.h
    event.h
    io.h
    log_manager.h
    logger.h
  SRCS
    record_batcher.cc
    event.cc
    log_manager.cc
    logger.cc
  DEPS
    v::config
    v::model
)

add_subdirectory(tests)
