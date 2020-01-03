rp_test(
   UNIT_TEST
   BINARY_NAME repository_test
   SOURCES repository_test.cc
   LIBRARIES Seastar::seastar_testing storage2 storage

   ARGS "-- -c 1"
 )