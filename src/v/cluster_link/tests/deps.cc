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
#include "cluster_link/types.h"

using namespace std::chrono_literals;

namespace cluster_link::tests {

cluster_link_manager_test_fixture::cluster_link_manager_test_fixture(
  ::model::node_id self)
  : _self(self) {}

ss::future<> cluster_link_manager_test_fixture::wire_up_and_start(
  std::unique_ptr<link_factory> lf) {
    setup_cluster_mock();
    co_await _table.start_single();
    _cluster_factory = std::make_unique<cluster_mock_factory>(&_cluster_mock);

    _fpmp = std::make_unique<fake_partition_manager_proxy>();
    auto fplc = std::make_unique<fake_partition_leader_cache_impl>();
    _fplci = fplc.get();

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
      ss::sharded_parameter([this]() {
          auto tmc = std::make_unique<fake_topic_metadata_cache>();
          _tmc = tmc.get();
          return tmc;
      }),
      ss::sharded_parameter([this]() {
          return std::make_unique<test_link_registry>(&_table.local());
      }),
      ss::sharded_parameter([&lf]() { return std::move(lf); }),
      ss::sharded_parameter([this]() {
          return std::make_unique<cluster_mock_factory>(&_cluster_mock);
      }),
      1s);

    auto notif_id = _table.local().register_for_updates(
      [this](model::id_t id) { _manager.local().on_link_change(id); });
    _notification_cleanups.emplace_back(
      [this, notif_id] { _table.local().unregister_for_updates(notif_id); });

    co_await _manager.invoke_on_all([](manager& m) { return m.start(); });
}

ss::future<> cluster_link_manager_test_fixture::reset() {
    _notification_cleanups.clear();
    _lf = nullptr;
    co_await _manager.stop();
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
          ntp, shard == ss::this_shard_id() ? ntp_leader::yes : ntp_leader::no);
    } else {
        partition_manager()->remove_shard_owner(ntp);
        _manager.local().handle_partition_state_change(ntp, ntp_leader::no);
    }
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
                return table
                  .apply_update(
                    cluster::cluster_link::testing::create_upsert_command(
                      ::model::offset{id()}, metadata.copy()))
                  .then([](std::error_code ec) {
                      vassert(
                        ec.value() == 0,
                        "Failed to upsert link: {}",
                        ec.message());
                  });
            });
      });
}

std::optional<std::reference_wrapper<const model::metadata>>
cluster_link_manager_test_fixture::find_link_by_id(model::id_t id) {
    return _table.local().find_link_by_id(id);
}

std::optional<std::reference_wrapper<const model::metadata>>
cluster_link_manager_test_fixture::find_link_by_name(
  const model::name_t& name) {
    return _table.local().find_link_by_name(name);
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

void cluster_link_manager_test_fixture::set_topic_config(
  cluster::topic_configuration cfg) {
    _tmc->set_topic_config(std::move(cfg));
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
} // namespace cluster_link::tests
