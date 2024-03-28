v_cc_library(
  NAME ai
  SRCS
    service.cc
    llama_bindings.cc
  DEPS
    Seastar::seastar
    absl::strings
    llama
    v::metrics
)

add_subdirectory(tests)
