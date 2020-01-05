rp_test(
   UNIT_TEST
   BINARY_NAME log_mgr_test
   SOURCES log_manager_test.cc
   LIBRARIES Seastar::seastar_testing storage2 storage

   ARGS "-- -c 1"
 )


rp_test(
  UNIT_TEST
  BINARY_NAME write_path_test
  SOURCES write_path_test.cc
  LIBRARIES Seastar::seastar_testing storage2 storage

  ARGS "-- -c 1"
)