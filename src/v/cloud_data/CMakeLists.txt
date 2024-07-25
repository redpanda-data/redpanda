add_subdirectory(tests)

v_cc_library(
  NAME cloud_data
  SRCS
    aggregated_log_reader.cc
    aggregated_uploader.cc
    dl_stm.cc
  DEPS
    v::model
    v::cloud_storage
    v::raft
    v::storage
)
