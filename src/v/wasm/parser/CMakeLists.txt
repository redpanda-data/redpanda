v_cc_library(
  NAME wasm_parser
  SRCS
    parser.cc
  DEPS
    v::bytes
    v::strings
    v::container
    Seastar::seastar
)

add_subdirectory(tests)
