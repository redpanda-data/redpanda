find_package(Hdrhistogram)
find_package(Base64)
v_cc_library(
  NAME utils
  SRCS
    hdr_hist.cc
    human.cc
    file_io.cc
    base64.cc
  DEPS
    Seastar::seastar
    Hdrhistogram::hdr_histogram
    Base64::base64
    v::rphashing
    v::bytes)
add_subdirectory(tests)
