find_program(KAFKA_CODEGEN_VENV "kafka-codegen-venv")

set(schemata)

set(srcs)
foreach(schema ${schemata})
  get_filename_component(msg_name ${schema} NAME_WE)
  set(hdr "${CMAKE_CURRENT_BINARY_DIR}/${msg_name}.h")
  set(src "${CMAKE_CURRENT_BINARY_DIR}/${msg_name}.cc")
  list(APPEND srcs ${hdr})
  list(APPEND srcs ${src})
  add_custom_command(
    OUTPUT ${hdr} ${src}
    COMMAND ${KAFKA_CODEGEN_VENV} ${CMAKE_CURRENT_SOURCE_DIR}/generator.py
    ARGS ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/${schema}
    DEPENDS ${schema} ${CMAKE_CURRENT_SOURCE_DIR}/generator.py ${KAFKA_CODEGEN_VENV}
    COMMENT "Running kafka request codegen on ${schema}"
    VERBATIM)
endforeach()

v_cc_library(
  NAME kafka_request_schemata
  SRCS
    ${srcs}
  COPTS
    "-Wno-unused-lambda-capture"
  DEPS
    Seastar::seastar
    v::bytes
    v::rpc
    absl::flat_hash_map
    absl::flat_hash_set
)
