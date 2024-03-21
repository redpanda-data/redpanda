v_cc_library(
  NAME wasm
  SRCS
    api.cc
    ffi.cc
    logger.cc
    transform_probe.cc
    schema_registry.cc
    schema_registry_module.cc
    transform_module.cc
    wasi.cc
    wasmtime.cc
    cache.cc
    allocator.cc
    engine_probe.cc
    ai_module.cc
  DEPS
    wasmtime
    v::wasm_parser
    v::storage
    v::model
    v::pandaproxy_schema_registry
    v::ai
    Seastar::seastar
)

add_subdirectory(tests)
add_subdirectory(parser)
