add_subdirectory(schemata)

set(message_gen ${CMAKE_CURRENT_SOURCE_DIR}/schemata/generator.py)
set(message_srcs)

foreach(schema ${schemata})
  get_filename_component(msg_name ${schema} NAME_WE)
  set(schema_src ${CMAKE_CURRENT_SOURCE_DIR}/schemata/${schema})
  set(msg_dir "${CMAKE_CURRENT_BINARY_DIR}/schemata")
  set(msg_hdr "${msg_dir}/${msg_name}.h")
  set(msg_src "${msg_dir}/${msg_name}.cc")
  list(APPEND message_srcs ${msg_hdr})
  list(APPEND message_srcs ${msg_src})
  add_custom_command(
    OUTPUT ${msg_hdr} ${msg_src}
    COMMAND ${KAFKA_CODEGEN_VENV} ${message_gen}
    ARGS ${schema_src} ${msg_hdr} ${msg_src}
    DEPENDS ${schema_src} ${message_gen} ${KAFKA_CODEGEN_VENV}
    COMMENT "Running kafka request codegen on ${schema_src}"
    VERBATIM)
endforeach()


v_cc_library(
  NAME kafka_protocol
  SRCS
    errors.cc
    ${message_srcs}
  COPTS
    "-Wno-unused-lambda-capture"
  DEPS
    Seastar::seastar
    v::bytes
    v::rpc
    absl::flat_hash_map
    absl::flat_hash_set
)

add_subdirectory(tests)
