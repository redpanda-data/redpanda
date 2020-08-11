v_cc_library(
  NAME
    ssx
  HDRS
    "future-util.h"
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
