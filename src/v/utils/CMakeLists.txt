find_package(Hdrhistogram)
v_cc_library(
  NAME utils
  SRCS
    hdr_hist.cc
    human.cc
  DEPS
    Seastar::seastar
    Hdrhistogram::hdr_histogram)
add_subdirectory(tests)
