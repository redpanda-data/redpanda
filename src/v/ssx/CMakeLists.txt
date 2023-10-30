v_cc_library(
  NAME
    ssx
  HDRS
    "future-util.h"
  SRCS
    work_queue.cc
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
