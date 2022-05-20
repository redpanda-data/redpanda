v_cc_library(
  NAME
    ssx
  HDRS
    "future-util.h"
  SRCS
    metrics.cc
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
