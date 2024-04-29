find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

v_cc_library(
  NAME datalake
  SRCS
    arrow_writing_consumer.cc
    configuration.cc
    parquet_uploader.cc
  DEPS
    v::storage
    v::model
    v::cloud_storage
    v::raft
    Seastar::seastar
    Arrow::arrow_shared
    Parquet::parquet_shared
)

add_subdirectory(tests)