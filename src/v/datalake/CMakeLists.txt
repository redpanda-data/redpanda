find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Protobuf REQUIRED)

add_subdirectory(coordinator)

v_cc_library(
  NAME datalake
  SRCS
    arrow_translator.cc
    datalake_manager.cc
    parquet_writer.cc
    record_multiplexer.cc
    schemaless_translator.cc
    schema_protobuf.cc
  DEPS
    v::datalake_coordinator
    v::storage
    Seastar::seastar
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
    v::iceberg
)

add_subdirectory(tests)
