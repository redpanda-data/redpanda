foreach t : [
    'partition_allocator_tests.cc',
    'partition_balancer_planner_test.cc',
    'simple_batch_builder_test.cc',
    'serialization_rt_test.cc',
    'cluster_utils_tests.cc',
    'notification_latch_test.cc',
    'commands_serialization_test.cc',
    'topic_table_test.cc',
    'topic_updates_dispatcher_test.cc',
    'topic_table_partition_generator_test.cc',
    'controller_backend_test.cc',
    'idempotency_tests.cc',
    'feature_barrier_test.cc',
    'tm_stm_tests.cc',
    'rm_stm_tests.cc',
    'id_allocator_stm_test.cc',
    'topic_configuration_compat_test.cc',
    'local_monitor_test.cc',
    'tx_compaction_tests.cc']
  exe = executable('test_' + t.substring(0, -3),
    t,
    dependencies: [cluster, seastar_testing_main, model_test_utils, redpanda_app])
  test('cluster', exe, timeout: 60)
endforeach

foreach t : ['cluster_tests.cc',
    'configuration_change_test.cc',
    'autocreate_test.cc',
    'controller_api_tests.cc',
    'controller_state_test.cc',
    'create_partitions_test.cc',
    'decommissioning_tests.cc',
    'ephemeral_credential_test.cc',
    'health_monitor_test.cc',
    'metadata_dissemination_test.cc',
    'replicas_rebalancing_tests.cc']
  exe = executable('test_' + t.substring(0, -3) + '_rpfixture',
    t,
    dependencies: [cluster, seastar_testing_main, model_test_utils, redpanda_app])
  test('cluster', exe, timeout: 60, is_parallel: false)
endforeach

exe = executable('test_partition_movement',
  'partition_moving_test.cc',
  dependencies: [cluster, seastar_testing_main, model_test_utils,
    redpanda_app])
test('cluster', exe, suite: 'disable_on_ci', is_parallel: false)

exe = executable('test_leader_balancer',
  'leader_balancer_test.cc',
  dependencies: [boost, cluster])
test('cluster', exe)

exe = executable('test_metrics_reporter',
  'metrics_reporter_test.cc',
  dependencies: [boost, cluster])
test('cluster', exe)

exe = executable('bench_leader_balancer',
  'leader_balancer_bench.cc',
  dependencies: [seastar_perf_testing, cluster])
benchmark('cluster', exe)

exe = executable('bench_partition_allocator',
  'allocation_bench.cc',
  dependencies: [seastar_perf_testing, cluster])
benchmark('cluster', exe)
