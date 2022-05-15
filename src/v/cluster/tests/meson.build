t = executable('bench_partition_allocator',
  'allocation_bench.cc',
  dependencies: [cluster, seastar_perf_testing])
benchmark('partition allocator', t)

t = executable('test_metadata_dissemination_utils',
  'metadata_dissemination_utils_test.cc',
  dependencies: [cluster, boost])
test('metadata dissemination', t)

t = executable('test_cluster',
    'partition_allocator_tests.cc',
    'simple_batch_builder_test.cc',
    'serialization_rt_test.cc',
    'cluster_utils_tests.cc',
    'cluster_tests.cc',
    'metadata_dissemination_test.cc',
    'notification_latch_test.cc',
    'autocreate_test.cc',
    'controller_state_test.cc',
    'commands_serialization_test.cc',
    'topic_table_test.cc',
    'topic_updates_dispatcher_test.cc',
    'controller_backend_test.cc',
    'configuration_change_test.cc',
    'idempotency_tests.cc',
    'feature_barrier_test.cc',
    'tm_stm_tests.cc',
    'rm_stm_tests.cc',
    'controller_api_tests.cc',
    'decommissioning_tests.cc',
    'replicas_rebalancing_tests.cc',
    'create_partitions_test.cc',
    'id_allocator_stm_test.cc',
    'data_policy_controller_test.cc',
    'health_monitor_test.cc',
    'topic_configuration_compat_test.cc',
    'local_monitor_test.cc',
    dependencies: [cluster, seastar_testing_main, model_test_utils, redpanda_app])
test('cluster', t)

t = executable('test_partition_movement',
  'partition_moving_test.cc',
  dependencies: [cluster, seastar_testing_main, model_test_utils,
    redpanda_app])
test('partition movement', t)

t = executable('test_leader_balancer',
  'leader_balancer_test.cc',
  dependencies: [boost, cluster])
test('leader balancer', t)

t = executable('bench_leader_balancer',
  'leader_balancer_bench.cc',
  dependencies: [seastar_perf_testing, cluster])
benchmark('leader balancer', t)

t = executable('test_metrics_reporter',
  'metrics_reporter_test.cc',
  dependencies: [boost, cluster])
test('metrics reporter', t)
