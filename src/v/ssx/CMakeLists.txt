v_cc_library(
  NAME
    ssx
  HDRS
    "future-util.h"
    "metrics.h"
  SRCS
    "logger.cc"
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
