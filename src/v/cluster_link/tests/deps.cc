/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster_link/tests/deps.h"

#include "cluster/cluster_link/tests/utils.h"
#include "cluster/types.h"
#include "cluster_link/types.h"

using namespace std::chrono_literals;

using kafka::data::rpc::test::fake_topic_creator;

namespace cluster_link::tests {

cluster_link_manager_test_fixture::cluster_link_manager_test_fixture(
  ::model::node_id self)
  : _self(self) {}

ss::future<> cluster_link_manager_test_fixture::wire_up_and_start(
  std::unique_ptr<link_factory> lf) {
    setup_cluster_mock();
    co_await _table.start_single();
    _cluster_factory = std::make_unique<cluster_mock_factory>(&_cluster_mock);

    auto tmc = std::make_unique<fake_topic_metadata_cache>();
    _tmc = tmc.get();

    _fpmp = std::make_unique<fake_partition_manager_proxy>();
    auto fplc = std::make_unique<fake_partition_leader_cache_impl>();
    _fplci = fplc.get();

    auto ftpc = std::make_unique<fake_topic_creator>(
      [this](const cluster::topic_configuration& tp_cfg) {
          _tmc->set_topic_config(tp_cfg);
      },
      [this](const cluster::topic_properties_update& update) {
          _tmc->update_topic_config(update);
      },
      [this](const ::model::ntp& ntp, ::model::node_id leader) {
          elect_leader(ntp, leader, std::nullopt);
      },
      [this](
        ::model::topic_namespace_view tp_ns,
        int32_t partition_count,
        ::model::node_id leader) {
          return update_partition_count(tp_ns, partition_count, leader);
      },
      _default_topic_replication.bind());
    _ftpc = ftpc.get();

    _lf = lf.get();
    co_await _manager.start_single(
      _self,
      ss::sharded_parameter([&fplc]() { return std::move(fplc); }),
      ss::sharded_parameter([this]() {
          auto fpm = std::make_unique<fake_partition_manager>(
            partition_manager_proxy());
          _fpm = fpm.get();
          return fpm;
      }),
      ss::sharded_parameter([&tmc]() { return std::move(tmc); }),
      ss::sharded_parameter([&ftpc]() { return std::move(ftpc); }),
      ss::sharded_parameter([this]() {
          auto fss = std::make_unique<fake_security_service>();
          _fss = fss.get();
          return fss;
      }),
      ss::sharded_parameter([this]() {
          return std::make_unique<test_link_registry>(&_table.local());
      }),
      ss::sharded_parameter([&lf]() { return std::move(lf); }),
      ss::sharded_parameter([this]() {
          return std::make_unique<cluster_mock_factory>(&_cluster_mock);
      }),
      ss::sharded_parameter([this]() {
          auto router = std::make_unique<test_consumer_group_router>();
          _consumer_group_router = router.get();
          return router;
      }),
      ss::sharded_parameter([this]() {
          auto provider = std::make_unique<test_partition_metadata_provider>();
          _partition_metadata_provider = provider.get();
          return provider;
      }),
      ss::sharded_parameter([this]() {
          auto rpc = std::make_unique<test_kafka_rpc_client_service>(_tmc);
          _tkrcs = rpc.get();
          return rpc;
      }),
      ss::sharded_parameter([this]() {
          auto fmtp = std::make_unique<fake_members_table_provider>();
          _fmtp = fmtp.get();
          return fmtp;
      }),
      1s,
      _default_topic_replication.bind(),
      ss::default_scheduling_group());

    auto notif_id = _table.local().register_for_updates(
      [this](model::id_t id, ::model::revision_id revision) {
          _manager.local().on_link_change(id, revision);
      });
    _notification_cleanups.emplace_back(
      [this, notif_id] { _table.local().unregister_for_updates(notif_id); });

    co_await _manager.invoke_on_all([](manager& m) { return m.start(); });
}

ss::future<> cluster_link_manager_test_fixture::reset() {
    _notification_cleanups.clear();
    _lf = nullptr;
    co_await _manager.stop();
    _fss = nullptr;
    _tmc = nullptr;
    _fpm = nullptr;
    _fplci = nullptr;
    _fpmp.reset();
    co_await _table.stop();
}

void cluster_link_manager_test_fixture::elect_leader(
  const ::model::ntp& ntp,
  ::model::node_id node_id,
  std::optional<ss::shard_id> shard_id) {
    partition_leader_cache()->set_leader_node(ntp, node_id);
    if (node_id == _self) {
        auto shard = shard_id.value_or(ss::this_shard_id());
        partition_manager()->set_shard_owner(ntp, shard);
        _manager.local().handle_partition_state_change(
          ntp,
          shard == ss::this_shard_id() ? ntp_leader::yes : ntp_leader::no,
          ::model::term_id(_term_counter++));
    } else {
        partition_manager()->remove_shard_owner(ntp);
        _manager.local().handle_partition_state_change(
          ntp, ntp_leader::no, std::nullopt);
    }
}

cluster::errc cluster_link_manager_test_fixture::update_partition_count(
  ::model::topic_namespace_view tp_ns,
  int32_t new_partition_count,
  ::model::node_id node_id) {
    auto partition_count = partition_leader_cache()->partition_count(tp_ns);
    if (partition_count.has_value()) {
        for (int32_t i = partition_count.value(); i < new_partition_count;
             ++i) {
            auto ntp = ::model::ntp(
              tp_ns.ns, tp_ns.tp, ::model::partition_id(i));
            elect_leader(ntp, node_id, std::nullopt);
        }
    }

    return cluster::errc::success;
}

ss::future<>
cluster_link_manager_test_fixture::upsert_link(model::metadata metadata) {
    auto id = model::id_t(_next_link_id++);
    return ss::do_with(
      id,
      std::move(metadata),
      [this](model::id_t& id, model::metadata& metadata) {
          return _table.invoke_on_all(
            [id, &metadata](cluster::cluster_link::table& table) {
                return metadata.copy().then([id, &table](model::metadata md) {
                    return table
                      .apply_update(
                        cluster::cluster_link::testing::create_upsert_command(
                          ::model::offset{id()}, std::move(md)))
                      .then([](std::error_code ec) {
                          vassert(
                            ec.value() == 0,
                            "Failed to upsert link: {}",
                            ec.message());
                      });
                });
            });
      });
}

ss::future<> cluster_link_manager_test_fixture::update_link(
  model::id_t id, model::metadata metadata) {
    return ss::do_with(
      id,
      std::move(metadata),
      [this](model::id_t& id, model::metadata& metadata) {
          return _table.invoke_on_all(
            [id, &metadata](cluster::cluster_link::table& table) {
                return table
                  .apply_update(
                    cluster::cluster_link::testing::
                      create_update_cluster_link_configuration_command(
                        id,
                        ::cluster_link::model::
                          update_cluster_link_configuration_cmd{
                            .connection = metadata.connection,
                            .link_config = metadata.configuration.copy(),
                          }))
                  .then([](std::error_code ec) {
                      vassert(
                        ec.value() == 0,
                        "Failed to update link: {}",
                        ec.message());
                  });
            });
      });
}

model::metadata_ptr
cluster_link_manager_test_fixture::find_link_by_id(model::id_t id) {
    return _table.local().find_link_by_id(id);
}

model::metadata_ptr cluster_link_manager_test_fixture::find_link_by_name(
  const model::name_t& name) {
    return _table.local().find_link_by_name(name);
}

std::optional<model::id_t>
cluster_link_manager_test_fixture::find_link_id_by_name(
  const model::name_t& name) {
    return _table.local().find_id_by_name(name);
}

ss::future<std::optional<model::cluster_link_task_status_report>>
cluster_link_manager_test_fixture::await_status_report(
  ss::lowres_clock::duration timeout,
  ss::lowres_clock::duration backoff,
  std::function<bool(const model::cluster_link_task_status_report&)> predicate,
  std::optional<ss::abort_source> as) {
    auto timeout_time = ss::lowres_clock::now() + timeout;
    while (ss::lowres_clock::now() < timeout_time) {
        auto report = _manager.local().get_task_status_report();
        if (predicate(report)) {
            co_return report;
        }
        if (as.has_value()) {
            co_await ss::sleep_abortable(backoff, as.value());
        } else {
            co_await ss::sleep(backoff);
        }
    }

    co_return std::nullopt;
}

ss::future<bool> cluster_link_manager_test_fixture::wait_for_report_to_match(
  ss::lowres_clock::duration timeout,
  ss::lowres_clock::duration backoff,
  std::function<bool(const model::cluster_link_task_status_report&)>
    predicate) {
    return await_status_report(timeout, backoff, std::move(predicate))
      .then([](std::optional<model::cluster_link_task_status_report> report) {
          return report.has_value();
      });
}

void cluster_link_manager_test_fixture::set_topic_config(
  cluster::topic_configuration cfg) {
    _tmc->set_topic_config(std::move(cfg));
}

void cluster_link_manager_test_fixture::set_partition_hwm(
  ::model::topic_partition_view tp, kafka::offset hwm) {
    auto cur_offsets = _tmc->get_partition_offsets(
      ::model::ntp(::model::kafka_namespace, tp.topic, tp.partition));
    if (!cur_offsets.has_value()) {
        throw std::runtime_error("unknown ntp");
    }
    cur_offsets->high_watermark = kafka::offset_cast(hwm);
    _tmc->set_partition_offsets(
      ::model::ntp(::model::kafka_namespace, tp.topic, tp.partition),
      cur_offsets.value());
    _partition_metadata_provider->hwms[::model::topic_partition(tp)] = hwm;
}

void cluster_link_manager_test_fixture::setup_cluster_mock() {
    _cluster_mock.register_default_handlers();
    _cluster_mock.add_broker(
      ::model::node_id(0), net::unresolved_address{"localhost", 9092});
    _cluster_mock.add_broker(
      ::model::node_id(1), net::unresolved_address{"localhost", 9093});
    _cluster_mock.add_broker(
      ::model::node_id(2), net::unresolved_address{"localhost", 9094});
}

std::optional<::model::partition_id>
test_consumer_group_router::partition_for(const kafka::group_id& group) const {
    auto hash = std::hash<kafka::group_id>{}(group);
    return ::model::partition_id(
      static_cast<::model::partition_id::type>(hash % partition_count));
}

ss::future<kafka::offset_commit_response>
test_consumer_group_router::offset_commit(kafka::offset_commit_request req) {
    auto& g_state = groups[req.data.group_id];
    for (auto& tp : req.data.topics) {
        auto& topic = g_state.offsets[tp.name];
        for (auto& p : tp.partitions) {
            topic[p.partition_index] = ::model::offset_cast(p.committed_offset);
        }
    }
    kafka::offset_commit_response resp;

    co_return resp;
}

ss::future<std::optional<kafka::offset>>
test_partition_metadata_provider::get_partition_high_watermark(
  ::model::topic_partition_view tp) {
    auto it = hwms.find(::model::topic_partition(tp));
    if (it != hwms.end()) {
        co_return it->second;
    }
    co_return std::nullopt;
};

ss::future<result<kafka::data::rpc::partition_offsets_map, cluster::errc>>
test_kafka_rpc_client_service::get_partition_offsets(
  chunked_vector<kafka::data::rpc::topic_partitions> tps) {
    if (inserted_get_partition_offsets_error.has_value()) {
        auto err = *inserted_get_partition_offsets_error;
        inserted_get_partition_offsets_error.reset();
        co_return err;
    }
    if (inserted_get_partition_offsets_response.has_value()) {
        auto val = std::move(inserted_get_partition_offsets_response).value();
        inserted_get_partition_offsets_response.reset();
        co_return val;
    }

    kafka::data::rpc::partition_offsets_map results;
    results.reserve(tps.size());

    for (const auto& tp : tps) {
        auto& topic_results = results[tp.topic];
        topic_results.reserve(tp.partitions.size());
        for (const auto& pid : tp.partitions) {
            auto offsets = _ftmc->get_partition_offsets(
              ::model::ntp(::model::kafka_namespace, tp.topic, pid));
            if (!offsets.has_value()) {
                topic_results[pid].err = cluster::errc::partition_not_exists;
                continue;
            }
            topic_results[pid].err = cluster::errc::success;
            topic_results[pid].offsets = kafka::data::rpc::partition_offsets{
              .high_watermark = ::model::offset_cast(offsets->high_watermark),
              .last_stable_offset = kafka::offset{-1},
            };
        }
    }

    co_return results;
}

} // namespace cluster_link::tests
