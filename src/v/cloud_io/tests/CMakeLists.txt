v_cc_library(
  NAME
    cloud_io_utils
  HDRS
    "scoped_remote.h"
  SRCS
    "scoped_remote.cc"
  DEPS
    v::cloud_io
    v::cloud_storage_clients
)


