v_cc_library(
  NAME wasm
  HDRS
    api.h
    fwd.h
    cache.h
  SRCS
    api.cc
    ffi.cc
    logger.cc
    probe.cc
    schema_registry.cc
    schema_registry_module.cc
    transform_module.cc
    wasi.cc
    wasmtime.cc
    cache.cc
  DEPS
    wasmtime
    v::storage
    v::model
    v::pandaproxy_schema_registry
    Seastar::seastar
)

add_subdirectory(tests)
