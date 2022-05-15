foreach t : [
    'ntp_archiver_test.cc',
    'segment_reupload_test.cc',
    'ntp_archiver_reupload_test.cc',
    'retention_strategy_test.cc',
    'archival_metadata_stm_test.cc',
    'upload_housekeeping_service_test.cc']
  exe = executable('test_' + t.substring(0, -3) + '_rpfixture',
  'service_fixture.cc',
  t,
  dependencies: [seastar_testing_main, archival, storage_test_utils,
    redpanda_app, model_test_utils, http_test_utils])
  test('archival', exe, timeout: 60, args: '-- -c1', is_parallel: false)
endforeach
