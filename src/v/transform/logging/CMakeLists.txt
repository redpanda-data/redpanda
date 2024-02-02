v_cc_library(
  NAME transform_logging
  HDRS
    event.h
    io.h
    log_manager.h
    logger.h
  SRCS
    event.cc
    log_manager.cc
    logger.cc
  DEPS
    v::config
    v::model
)

add_subdirectory(tests)
