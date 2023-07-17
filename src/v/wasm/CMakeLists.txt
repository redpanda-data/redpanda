v_cc_library(
  NAME wasm
  HDRS
    api.h
    fwd.h
  SRCS
    "api.cc"
    "logger.cc"
    "probe.cc"
    "wasi.cc"
    "wasmedge.cc"
    "ffi.cc"
    "rp_module.cc"
    "schema_registry.cc"
  DEPS
    wasmedge
    v::storage
    v::model
    v::cluster
    v::pandaproxy_schema_registry
    Seastar::seastar
)

add_subdirectory(tests)
