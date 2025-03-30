find_package(Protobuf REQUIRED)

add_subdirectory(coordinator)
add_subdirectory(translation)

v_cc_library(
  NAME datalake_common
  SRCS
    table_definition.cc
  DEPS
    v::iceberg
)

v_cc_library(
  NAME datalake_manager
  SRCS
    datalake_manager.cc
    backlog_controller.cc
  DEPS
    v::datalake_translation
    v::datalake_translation_probe
    v::datalake_coordinator
    v::cluster
    v::raft
    v::storage
    v::schema
    Seastar::seastar
)

v_cc_library(
  NAME datalake_partition_spec_parser
  SRCS
    partition_spec_parser.cc
  DEPS
    v::iceberg_unresolved_partition_spec
    Seastar::seastar
)

v_cc_library(
  NAME datalake_writer
  SRCS
    catalog_schema_manager.cc
    data_writer_interface.cc
    record_schema_resolver.cc
    record_translator.cc
    schema_registry.cc
    partitioning_writer.cc
    base_types.cc
    cloud_data_io.cc
    table_creator.cc
    local_parquet_file_writer.cc
    serde_parquet_writer.cc
    partition_key_path.cc
  DEPS
    v::cloud_io
    v::datalake_common
    v::datalake_translation_probe
    v::pandaproxy_schema_registry
    v::schema
    v::storage
    v::schema
    Seastar::seastar
    protobuf::libprotobuf
    v::iceberg
    v::iceberg_conversion
    v::serde_protobuf
    v::serde_avro
    v::serde_parquet
    v::iceberg_conversion
)

add_subdirectory(tests)
