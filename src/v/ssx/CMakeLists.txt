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
    absl::flat_hash_map
  )

add_subdirectory(tests)
