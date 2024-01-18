v_cc_library(
  NAME
    bytes
  SRCS
    iobuf.cc
  DEPS
    absl::hash
    Seastar::seastar
    v::container
  )

v_cc_library(
  NAME
    bytes_random
  SRCS
    random.cc
  DEPS
    v::random
    v::bytes
)

add_subdirectory(tests)
