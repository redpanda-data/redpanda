v_cc_library(
  NAME wasm_parser
  SRCS
  DEPS
    v::bytes
    v::utils
    Seastar::seastar
)

add_subdirectory(tests)
