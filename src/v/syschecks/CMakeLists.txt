v_cc_library(
  NAME syschecks
  HRDS syschecks.h
  SRCS
    syschecks.cc
    pidfile.cc
  DEPS
    v::utils
    v::version
    )
