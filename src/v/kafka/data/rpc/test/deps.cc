#include "kafka/data/rpc/test/deps.h"

#include "rpc/rpc_server.h"

namespace kafka::data::rpc::test {

void kafka_data_test_fixture::wire_up_and_start() {
    _remote_fpmp = std::make_unique<fake_partition_manager_proxy>();
    _remote_services
      .start_single(
        ss::sharded_parameter([this]() {
            auto ftmc = std::make_unique<fake_topic_metadata_cache>();
            _remote_ftmc = ftmc.get();
            return ftmc;
        }),
        ss::sharded_parameter([this]() {
            auto fpm = std::make_unique<fake_partition_manager>(
              remote_partition_manager_proxy());
            _remote_fpm = fpm.get();
            return fpm;
        }),
        ss::sharded_parameter(
          []() { return std::make_unique<fake_shadow_link_registry>(); }))
      .get();

    _local_fpmp = std::make_unique<fake_partition_manager_proxy>();
    _local_services
      .start_single(
        ss::sharded_parameter([this]() {
            auto ftmc = std::make_unique<fake_topic_metadata_cache>();
            _local_ftmc = ftmc.get();
            return ftmc;
        }),
        ss::sharded_parameter([this]() {
            auto fpm = std::make_unique<fake_partition_manager>(
              local_partition_manager_proxy());
            _local_fpm = fpm.get();
            return fpm;
        }),
        ss::sharded_parameter(
          []() { return std::make_unique<fake_shadow_link_registry>(); }))
      .get();

    auto fplc = std::make_unique<fake_partition_leader_cache>();
    _fplc = fplc.get();
    auto ftpc = std::make_unique<fake_topic_creator>(
      [this](const cluster::topic_configuration& tp_cfg) {
          remote_metadata_cache()->set_topic_cfg(tp_cfg);
          local_metadata_cache()->set_topic_cfg(tp_cfg);
      },
      [this](const cluster::topic_properties_update& update) {
          remote_metadata_cache()->update_topic_cfg(update);
          local_metadata_cache()->update_topic_cfg(update);
      },
      [this](const model::ntp& ntp, model::node_id leader) {
          elect_leader(ntp, leader);
      },
      [this](
        model::topic_namespace_view tp_ns,
        int32_t partition_count,
        model::node_id leader) {
          return update_partition_count(tp_ns, partition_count, leader);
      },
      _default_topic_replication.bind());
    _ftpc = ftpc.get();

    _client
      .start_single(
        self_node,
        ss::sharded_parameter([&fplc]() { return std::move(fplc); }),
        ss::sharded_parameter([&ftpc]() { return std::move(ftpc); }),
        ss::sharded_parameter([this]() {
            return std::make_unique<delegating_fake_topic_metadata_cache>(
              _local_ftmc);
        }),
        _conn_cache,
        &_local_services)
      .get();
}

void kafka_data_test_fixture::register_services(
  std::vector<std::unique_ptr<::rpc::service>>& services,
  ::rpc::rpc_server* server) {
    services.push_back(
      std::make_unique<kafka::data::rpc::network_service>(
        ss::default_scheduling_group(),
        ss::default_smp_service_group(),
        &_remote_services,
        kafka::data::rpc::network_service::memory_config{
          .memory = server ? &server->memory() : nullptr,
          .total = server ? server->cfg.max_service_memory_per_core : 0,
        }));
}

void kafka_data_test_fixture::elect_leader(
  const model::ntp& ntp, model::node_id node_id) {
    partition_leader_cache()->set_leader_node(ntp, node_id);
    if (node_id == self_node) {
        local_partition_manager()->set_shard_owner(ntp, ss::this_shard_id());
        remote_partition_manager()->remove_shard_owner(ntp);
    } else if (node_id == other_node) {
        remote_partition_manager()->set_shard_owner(ntp, ss::this_shard_id());
        local_partition_manager()->remove_shard_owner(ntp);
    } else {
        throw std::runtime_error(ss::format("unknown node_id {}", node_id));
    }
}

cluster::errc kafka_data_test_fixture::update_partition_count(
  model::topic_namespace_view tp_ns,
  int32_t new_partition_count,
  model::node_id node_id) {
    auto partition_count = partition_leader_cache()->partition_count(tp_ns);
    if (partition_count.has_value()) {
        for (int32_t i = partition_count.value(); i < new_partition_count;
             ++i) {
            auto ntp = model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id(i));
            elect_leader(ntp, node_id);
        }
    }

    return cluster::errc::success;
}

void kafka_data_test_fixture::reset() {
    _client.stop().get();
    _local_ftmc = nullptr;
    _local_fpm = nullptr;
    _local_fpmp.reset();
    _remote_ftmc = nullptr;
    _remote_fpm = nullptr;
    _remote_fpmp.reset();
    _fplc = nullptr;
    _ftpc = nullptr;
    _local_services.stop().get();
    _remote_services.stop().get();
}

} // namespace kafka::data::rpc::test
