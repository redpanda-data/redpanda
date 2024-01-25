v_cc_library(
  NAME transform_logging
  HDRS
    event.h
    io.h
  SRCS
    event.cc
  DEPS
    v::model
)

add_subdirectory(tests)
