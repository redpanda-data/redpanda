find_package(Avro)

set(schemas_dir "${CMAKE_CURRENT_SOURCE_DIR}/avroschemas")

set(schemas
  manifest_file.schema.json
  manifest_entry.schema.json
)

set(avro_hdrs)
foreach (schema ${schemas})
  get_filename_component(struct_name ${schema} NAME_WE)
  set(avro_hdr "${CMAKE_CURRENT_BINARY_DIR}/${struct_name}.avrogen.h")

  add_custom_command (
    OUTPUT ${avro_hdr}
    COMMAND avrogencpp
    -i ${schemas_dir}/${schema}
    -o ${avro_hdr} -n iceberg::avrogen
    DEPENDS avrogencpp ${schemas_dir}/${schema}
    COMMENT "Running avrogencpp on ${schema}"
    VERBATIM)

  list(APPEND avro_hdrs ${avro_hdr})
endforeach()

v_cc_library(
  NAME iceberg
  SRCS
    ${avro_hdrs}
    action.cc
    datatypes.cc
    datatypes_json.cc
    json_utils.cc
    logger.cc
    manifest_avro.cc
    manifest_entry_type.cc
    manifest_entry_values.cc
    manifest_io.cc
    manifest_file_packer.cc
    manifest_list_avro.cc
    partition_key.cc
    partition_key_type.cc
    partition_json.cc
    schema.cc
    schema_avro.cc
    schema_json.cc
    snapshot_json.cc
    struct_accessor.cc
    table_metadata_json.cc
    table_update_applier.cc
    time_transform_visitor.cc
    transaction.cc
    transform.cc
    transform_json.cc
    transform_utils.cc
    update_schema_action.cc
    values.cc
    values_avro.cc
    values_bytes.cc
  DEPS
    Avro::avro
    v::bytes
    v::cloud_io
    v::container
    v::json
    v::strings
    v::utils
)

add_subdirectory(tests)
