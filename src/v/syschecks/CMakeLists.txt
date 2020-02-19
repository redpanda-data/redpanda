v_cc_library(
  NAME syschecks
  HRDS syschecks.h
  SRCS syschecks.cc
  DEPS
    v::utils
    systemd)
