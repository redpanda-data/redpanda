v_cc_library(
  NAME
    strings
  SRCS
    utf8.cc
  DEPS
    absl::hash
    Seastar::seastar
  )

add_subdirectory(tests)
