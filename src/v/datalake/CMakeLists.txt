find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
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
  DEPS
    v::datalake_translation
    v::datalake_coordinator
    v::cluster
    v::raft
    v::storage
    v::schema
    Seastar::seastar
)

v_cc_library(
  NAME datalake_writer
  SRCS
    arrow_translator.cc
    batching_parquet_writer.cc
    catalog_schema_manager.cc
    data_writer_interface.cc
    arrow_writer.cc
    record_multiplexer.cc
    record_schema_resolver.cc
    record_translator.cc
    schema_registry.cc
    schema_avro.cc
    schema_protobuf.cc
    partitioning_writer.cc
    protobuf_utils.cc
    values_avro.cc
    values_protobuf.cc
    base_types.cc
    cloud_data_io.cc
    table_creator.cc
    translation_task.cc
    schema_parquet.cc
    values_parquet.cc
    local_parquet_file_writer.cc
    serde_parquet_writer.cc
  DEPS
    v::cloud_io
    v::datalake_common
    v::pandaproxy_schema_registry
    v::schema
    v::storage
    v::schema
    Seastar::seastar
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
    v::iceberg
    v::serde_protobuf
    v::serde_avro
    v::serde_parquet
)

add_subdirectory(tests)
