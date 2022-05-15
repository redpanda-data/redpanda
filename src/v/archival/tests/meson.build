# ARGS "-- -c 1"
t = executable('test_archival',
  'service_fixture.cc',
  'ntp_archiver_test.cc',
  'service_test.cc',
  dependencies: [seastar_testing_main, archival, storage_test_utils,
    redpanda_app, model_test_utils])
test('archival', t)
