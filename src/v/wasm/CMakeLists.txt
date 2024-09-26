v_cc_library(
  NAME wasm
  SRCS
    ffi.cc
    logger.cc
    impl.cc
    transform_probe.cc
    schema_registry_module.cc
    transform_module.cc
    wasi.cc
    wasmtime.cc
    cache.cc
    allocator.cc
    engine_probe.cc
  DEPS
    wasmtime
    v::wasm_parser
    v::schema
    v::storage
    v::model
    v::pandaproxy_schema_registry
    Seastar::seastar
)

add_subdirectory(tests)
add_subdirectory(parser)
