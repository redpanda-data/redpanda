rp_test(
  FIXTURE_TEST
  BINARY_NAME audit_log
  SOURCES audit_log_test.cc
  LIBRARIES v::security_audit v::seastar_testing_main v::application
  ARGS "-- -c 1"
  LABELS security_audit
)
