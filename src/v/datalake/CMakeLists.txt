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
    Seastar::seastar
)

v_cc_library(
  NAME datalake_writer
  SRCS
    arrow_translator.cc
    batching_parquet_writer.cc
    catalog_schema_manager.cc
    data_writer_interface.cc
    parquet_writer.cc
    record_multiplexer.cc
    schema_registry.cc
    schemaless_translator.cc
    schema_protobuf.cc
    partitioning_writer.cc
    protobuf_utils.cc
    values_protobuf.cc
    base_types.cc
    cloud_data_io.cc
    translation_task.cc
  DEPS
    v::cloud_io
    v::datalake_common
    v::storage
    Seastar::seastar
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
    v::iceberg
    v::serde_protobuf
)

add_subdirectory(tests)
