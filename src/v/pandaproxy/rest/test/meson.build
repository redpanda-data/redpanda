t = executable('test_pp_rest',
  'fetch.cc',
  'list_topics.cc',
  'produce.cc',
  'consumer_group.cc',
  dependencies: [seastar_testing_main, redpanda_app, http])
test('pp rest', t)
