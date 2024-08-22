find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Protobuf REQUIRED)


v_cc_library(
  NAME datalake
  SRCS
    arrow_schema_translator.cc
    protobuf_to_arrow_converter.cc
    proto_to_arrow_scalar.cc
    proto_to_arrow_struct.cc
    record_multiplexer.cc
    schemaless_translator.cc
  DEPS
    v::storage
    Seastar::seastar
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
    v::iceberg
)

add_subdirectory(tests)
