v_cc_library(
  NAME
    ssx
  HDRS
    "future-util.h"
    "metrics.h"
    "this_fiber_id.h"
    "fiber_local.h"
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
