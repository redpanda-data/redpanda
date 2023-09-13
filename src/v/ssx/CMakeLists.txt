v_cc_library(
  NAME
    ssx
  HDRS
    "future-util.h"
    "metrics.h"
  SRCS
    work_queue.cc
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
