
v_cc_library(
  NAME archival
  SRCS
    manifest.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
    v::s3
)
add_subdirectory(tests)
