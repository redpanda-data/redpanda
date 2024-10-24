rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME io
  SOURCES
    common.cc
    common_test.cc
    cache_test.cc
    interval_map_test.cc
    persistence_test.cc
    page_test.cc
    page_set_test.cc
    io_queue_test.cc
    scheduler_test.cc
    pager_test.cc
  LIBRARIES
    v::gtest_main
    v::io
    absl::btree
  # scheduler and io-queue test are memory hungary since they track copies of
  # all dispatched and completed ios. scaling down cpus give CI tests a bit more
  # breathing room to run.
  ARGS "-- -c2"
)
