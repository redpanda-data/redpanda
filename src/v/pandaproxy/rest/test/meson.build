exe = executable('test_pp_rest_rpfixture',
  'fetch.cc',
  'list_topics.cc',
  'produce.cc',
  'consumer_group.cc',
  dependencies: [seastar_testing_main, redpanda_app, http])
test('pp rest', exe, is_parallel: false)
