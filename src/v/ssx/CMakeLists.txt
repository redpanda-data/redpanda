v_cc_library(
  NAME
    ssx
  SRCS
    work_queue.cc
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
