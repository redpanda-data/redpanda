find_package(Hdrhistogram)
v_cc_library(
  NAME utils
  SRCS
    hdr_hist.cc
    human.cc
    state_crc_file.cc
  DEPS
    Seastar::seastar
    Hdrhistogram::hdr_histogram
    v::rphashing
    v::bytes)
add_subdirectory(tests)