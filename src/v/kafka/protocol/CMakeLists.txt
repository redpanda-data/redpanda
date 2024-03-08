add_subdirectory(schemata)

set(message_gen ${CMAKE_CURRENT_SOURCE_DIR}/schemata/generator.py)
set(message_hdrs)
set(schema_srcs)

set(msg_dir "${CMAKE_CURRENT_BINARY_DIR}/schemata")
set(msg_src "${msg_dir}/protocol.cc")

foreach(schema ${schemata})
  get_filename_component(msg_name ${schema} NAME_WE)
  set(schema_src ${CMAKE_CURRENT_SOURCE_DIR}/schemata/${schema})
  list(APPEND schema_srcs ${schema_src})
  set(msg_hdr "${msg_dir}/${msg_name}.h")
  list(APPEND message_hdrs ${msg_hdr})
endforeach()

add_custom_command(
  OUTPUT ${message_hdrs} ${msg_src}
  COMMAND RedpandaPython::Interpreter ${message_gen}
  ARGS ${schema_srcs} ${message_hdrs} ${msg_src}
  DEPENDS ${schema_srcs} ${message_gen}
  COMMENT "Running kafka request codegen on ${schemata}"
  VERBATIM)

add_custom_target(kafka_codegen_headers
  DEPENDS ${message_hdrs}
)

v_cc_library(
  NAME kafka_protocol
  SRCS
    errors.cc
    logger.cc
    flex_versions.cc
    ${message_hdrs}
    ${msg_src}
    types.cc
    batch_reader.cc
    kafka_batch_adapter.cc
  DEPS
    Seastar::seastar
    v::model
    v::bytes
    v::rpc
    absl::flat_hash_map
    absl::flat_hash_set
)

add_subdirectory(tests)
