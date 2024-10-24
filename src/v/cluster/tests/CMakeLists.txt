rp_test(
  BENCHMARK_TEST
  BINARY_NAME partition_allocator_b
  SOURCES allocation_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::cluster
  LABELS cluster
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME health_report
  SOURCES health_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::cluster
  LABELS cluster
)

set(srcs
    partition_allocator_tests.cc
    partition_balancer_planner_test.cc
    partition_balancer_simulator_test.cc
    simple_batch_builder_test.cc
    serialization_rt_test.cc
    cluster_utils_tests.cc
    notification_latch_test.cc
    commands_serialization_test.cc
    topic_table_test.cc
    topic_updates_dispatcher_test.cc
    topic_table_partition_generator_test.cc
    idempotency_tests.cc
    feature_barrier_test.cc
    tm_coordinator_mapper_tests.cc
    rm_stm_tests.cc
    rm_stm_compatibility_test.cc
    local_monitor_test.cc
    tx_compaction_tests.cc
    producer_state_tests.cc
    client_quota_store_test.cc
    )

foreach(cluster_test_src ${srcs})
get_filename_component(test_name ${cluster_test_src} NAME_WE)
rp_test(
  UNIT_TEST
  BINARY_NAME ${test_name}
  SOURCES ${cluster_test_src}
  LIBRARIES v::seastar_testing_main v::application v::storage_test_utils v::cluster v::http_test_utils
  LABELS cluster
)
endforeach()

set(srcs
    distributed_kv_stm_tests.cc
    id_allocator_stm_test.cc
    tm_stm_tests.cc
    data_migration_table_test.cc
    )

foreach(cluster_test_src ${srcs})
get_filename_component(test_name ${cluster_test_src} NAME_WE)
rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME ${test_name}
  SOURCES ${cluster_test_src}
  LIBRARIES
    v::gtest_main
    v::storage_test_utils
    v::cluster
    v::http_test_utils
    v::raft
    v::raft_fixture
  LABELS cluster
)
endforeach()

set(srcs
        manual_log_deletion_test.cc
        cluster_tests.cc
        configuration_change_test.cc
        autocreate_test.cc
        controller_api_tests.cc
        eviction_stm_test.cc
        controller_state_test.cc
        create_partitions_test.cc
        decommissioning_tests.cc
        ephemeral_credential_test.cc
        health_monitor_test.cc
        metadata_dissemination_test.cc
        replicas_rebalancing_tests.cc
        security_frontend_test.cc)

foreach(cluster_test_src ${srcs})
    get_filename_component(test_name ${cluster_test_src} NAME_WE)
    rp_test(
            FIXTURE_TEST
            BINARY_NAME cluster_${test_name}
            SOURCES ${cluster_test_src}
            LIBRARIES v::seastar_testing_main v::application v::storage_test_utils v::cluster
            LABELS cluster
    )
endforeach()

# These 2 files have a `using namespace cluster;` and removing that
# would require modifying the FIXTURE_TEST and PERF_TEST_F macros.
set_source_files_properties(
    partition_allocator_tests.cc
    allocation_bench.cc
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION 1)

rp_test(
  UNIT_TEST
  BINARY_NAME leader_balancer_test
  SOURCES leader_balancer_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::cluster
  LABELS cluster
)

rp_test(
  UNIT_TEST
  BINARY_NAME leader_balancer_constraints_test
  SOURCES leader_balancer_constraints_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::cluster
  LABELS cluster
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME leader_balancer_bench
  SOURCES leader_balancer_bench.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Seastar::seastar_perf_testing v::cluster
  LABELS cluster
)

rp_test(
  UNIT_TEST
  BINARY_NAME metrics_reporter_test
  SOURCES metrics_reporter_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::cluster
  LABELS cluster
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME partition_balancer
  SOURCES partition_balancer_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::seastar_testing_main v::cluster
  LABELS cluster
)

rp_test(
  FIXTURE_TEST
  BINARY_NAME tx_coordinator
  SOURCES
    tx_topic_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::application
  ARGS "-- -c 1"
  LABELS cluster
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME plugin_test
  SOURCES 
    plugin_table_test.cc
    plugin_frontend_validation_test.cc
  LIBRARIES 
    v::gtest_main 
    v::cluster
  ARGS "-- -c 1"
  LABELS cluster
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME namespaced_cache_test
  SOURCES 
    namespaced_cache_test.cc
  LIBRARIES 
    v::gtest_main 
    v::serde
    v::config
  ARGS "-- -c 1"
  LABELS cluster
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME cluster_recovery_test
  SOURCES
    cluster_recovery_table_test.cc
  LIBRARIES
    v::gtest_main
    v::cluster
  ARGS "-- -c 1"
  LABELS cluster
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME partition_leaders_table_test
  SOURCES
    partition_leaders_table_test.cc
  LIBRARIES
    v::gtest_main
    v::cluster
  ARGS "-- -c 1"
  LABELS cluster
)
rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME partition_properties_stm_test
  SOURCES
     partition_properties_stm_test.cc
  LIBRARIES
    v::gtest_main
    v::cluster
    v::raft_fixture
    v::model_test_utils
  ARGS "-- -c 1"
  LABELS cluster
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME health_monitor
  SOURCES health_monitor_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::cluster
  LABELS cluster
)

# Needs multiple cores
rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME shard_placement_table_test
  SOURCES
    shard_placement_table_test.cc
  LIBRARIES
    v::gtest_main
    v::cluster
  ARGS "-- -c 4"
  LABELS cluster
)
