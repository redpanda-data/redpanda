find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Protobuf REQUIRED)

add_subdirectory(coordinator)

v_cc_library(
  NAME datalake_common
  SRCS
    table_definition.cc
  DEPS
    v::iceberg
)

v_cc_library(
  NAME datalake
  SRCS
    arrow_translator.cc
    datalake_manager.cc
    batching_parquet_writer.cc
    data_writer_interface.cc
    parquet_writer.cc
    record_multiplexer.cc
    schema_registry.cc
    schemaless_translator.cc
    schema_protobuf.cc
    protobuf_utils.cc
    values_protobuf.cc
    base_types.cc
  DEPS
    v::datalake_common
    v::datalake_coordinator
    v::storage
    Seastar::seastar
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
    v::iceberg
    v::serde_protobuf
)

add_subdirectory(tests)
