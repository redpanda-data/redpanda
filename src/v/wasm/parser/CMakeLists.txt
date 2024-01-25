v_cc_library(
  NAME wasm_parser
  SRCS
    parser.cc
  DEPS
    v::bytes
    v::utils
    Seastar::seastar
)

add_subdirectory(tests)
