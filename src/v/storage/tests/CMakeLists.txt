
rp_test(
  UNIT_TEST
  BINARY_NAME log_segment_appender_test
  SOURCES log_segment_appender_test.cc
  LIBRARIES Seastar::seastar_testing v::storage

  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_segment_test
  SOURCES log_segment_test.cc
  LIBRARIES Seastar::seastar_testing v::storage

  ARGS "-- -c 1"
)

# FIXME(agallego)
# rp_test(
#   UNIT_TEST
#   BINARY_NAME log_writer_test
#   SOURCES log_writer_test.cc
#   LIBRARIES Seastar::seastar_testing v::storage
#   ARGS "-- -c 1"
# )

rp_test(
  UNIT_TEST
  BINARY_NAME parser_test
  SOURCES parser_test.cc
  LIBRARIES Seastar::seastar_testing v::storage

  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_segment_reader_test
  SOURCES log_segment_reader_test.cc
  LIBRARIES Seastar::seastar_testing v::storage

  ARGS "-- -c 1"
)

# obsolete test
# rp_test(
#   UNIT_TEST
#   BINARY_NAME log_reader_test
#   SOURCES log_reader_test.cc
#   LIBRARIES Seastar::seastar_testing v::storage
#   ARGS "-- -c 1"
# )

rp_test(
  UNIT_TEST
  BINARY_NAME log_replayer_test
  SOURCES log_replayer_test.cc
  LIBRARIES Seastar::seastar_testing v::storage

  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_manager_test
  SOURCES log_manager_test.cc
  LIBRARIES Seastar::seastar_testing v::storage

  ARGS "-- -c 1"
)

# TODO
# rp_test(
#   UNIT_TEST
#   BINARY_NAME log_builder_test
#   SOURCES log_builder_test.cc
#   LIBRARIES Seastar::seastar_testing v::storage
#   ARGS "-- -c 1"
# )

rp_test(
  UNIT_TEST
  BINARY_NAME offset_assignment_test
  SOURCES offset_assignment_test.cc
  LIBRARIES Seastar::seastar_testing v::storage
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME storage_e2e_test
  SOURCES storage_e2e_test.cc
  LIBRARIES Seastar::seastar_testing v::storage
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME log_truncate_test
  SOURCES log_truncate_test.cc
  LIBRARIES Seastar::seastar_testing v::storage
  ARGS "-- -c 1"
)
rp_test(
  UNIT_TEST
  BINARY_NAME offset_index_test
  SOURCES offset_index_utils_tests.cc
  LIBRARIES Seastar::seastar_testing v::storage
  ARGS "-- -c 1"
)
rp_test(
  UNIT_TEST
  BINARY_NAME appender_chunk_manip
  SOURCES appender_chunk_manipulations.cc
  LIBRARIES Seastar::seastar_testing v::storage
  ARGS "-- -c 1"
)
