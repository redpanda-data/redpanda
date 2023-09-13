v_cc_library(
  NAME wasm
  HDRS
    api.h
    fwd.h
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
    work_queue.cc
  DEPS
    wasmtime
    v::storage
    v::model
    v::pandaproxy_schema_registry
    Seastar::seastar
)

add_subdirectory(tests)
