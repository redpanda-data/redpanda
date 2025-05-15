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

#include "panda_link/api.h"

#include "cluster/panda_link_frontend.h"
#include "cluster/partition_manager.h"
#include "panda_link/logger.h"
#include "panda_link/manager.h"
#include "raft/group_manager.h"

namespace panda_link {
class link_registry_adapter : public link_registry {
public:
    explicit link_registry_adapter(cluster::panda_link_frontend* pl_frontend)
      : _pl_frontend(pl_frontend) {}

    std::optional<model::panda_link_metadata>
    lookup_by_id(model::panda_link_id id) const override {
        return _pl_frontend->lookup_panda_link(id);
    }

private:
    cluster::panda_link_frontend* _pl_frontend;
};

class link_factory_adapter : public link_factory {};

service::service(
  model::node_id self,
  ss::sharded<cluster::panda_link_frontend>* pl_frontend,
  ss::sharded<cluster::partition_manager>* partition_manager,
  ss::sharded<raft::group_manager>* group_manager)
  : _self(self)
  , _pl_frontend(pl_frontend)
  , _partition_manager(partition_manager)
  , _group_manager(group_manager)
  , _queue([](const std::exception_ptr& ex) {
      vlog(pllog.warn, "unexpected panda link service error: {}", ex);
  }) {}

service::~service() = default;

ss::future<> service::start() {
    vlog(pllog.info, "Starting panda link service");
    if (ss::this_shard_id() == manager_shard) {
        // Manager only operates on a single shard in the broker
        _manager = std::make_unique<manager>(
          _self,
          std::make_unique<link_registry_adapter>(&_pl_frontend->local()),
          std::make_unique<link_factory_adapter>());

        co_await _manager->start();
    }

    register_notifications();
}

ss::future<> service::stop() {
    vlog(pllog.info, "Stopping panda link service");
    unregister_notifications();
    co_await _queue.shutdown();
    co_await _gate.close();
    if (_manager) {
        co_await _manager->stop();
    }
    vlog(pllog.info, "Panda link service stopped");
}

void service::register_notifications() {
    if (ss::this_shard_id() == manager_shard) {
        auto pl_notif_id = _pl_frontend->local().register_for_updates(
          [this](model::panda_link_id id) { _manager->on_link_change(id); });
        _notification_cleanups.emplace_back([this, pl_notif_id] {
            _pl_frontend->local().unregister_for_updates(pl_notif_id);
        });
    }

    auto leadership_notif_id
      = _group_manager->local().register_leadership_notification(
        [this](
          raft::group_id group_id,
          model::term_id term,
          std::optional<model::node_id> leader) {
            on_leadership_notification(group_id, term, leader);
        });
    _notification_cleanups.emplace_back([this, leadership_notif_id] {
        _group_manager->local().unregister_leadership_notification(
          leadership_notif_id);
    });

    auto unmanage_notification_id
      = _partition_manager->local().register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp) {
            on_unmanage_notification(tp);
        });
    _notification_cleanups.emplace_back([this, unmanage_notification_id] {
        _partition_manager->local().unregister_unmanage_notification(
          unmanage_notification_id);
    });

    auto manager_notifications_id
      = _partition_manager->local().register_manage_notification(
        model::kafka_namespace,
        [this](const ss::lw_shared_ptr<cluster::partition>& p) {
            on_manage_notification(p);
        });
    _notification_cleanups.emplace_back([this, manager_notifications_id] {
        _partition_manager->local().unregister_manage_notification(
          manager_notifications_id);
    });
}

void service::unregister_notifications() { _notification_cleanups.clear(); }

void service::on_leadership_notification(
  raft::group_id group_id,
  model::term_id term,
  std::optional<model::node_id> leader) {
    vlog(
      pllog.trace,
      "on_leadership_notification: group_id: {}, term: {}, leader: {}",
      group_id,
      term,
      leader);
    auto partition = _partition_manager->local().partition_for(group_id);
    if (!partition) {
        vlog(
          pllog.debug,
          "got leadership notification for unknown partition: {}",
          group_id);
        return;
    }
    _queue.submit([this, partition, leader] {
        return handle_on_leadership_notification(partition, leader);
    });
}

ss::future<> service::handle_on_leadership_notification(
  ss::foreign_ptr<ss::lw_shared_ptr<cluster::partition>> partition,
  std::optional<model::node_id> leader) {
    vlog(
      pllog.trace,
      "handling leadership notification: {}, leader: {}",
      partition->ntp(),
      leader);
    if (ss::this_shard_id() != manager_shard) {
        co_return co_await container().invoke_on(
          manager_shard, [p = std::move(partition), leader](auto& s) mutable {
              return s.handle_on_leadership_notification(std::move(p), leader);
          });
    }
    bool node_is_leader = leader.has_value() && *leader == _self;
    if (!node_is_leader) {
        _manager->on_leadership_change(partition->ntp(), ntp_leader::no);
        co_return;
    }
    ntp_leader is_leader = partition && partition->is_elected_leader()
                             ? ntp_leader::yes
                             : ntp_leader::no;
    _manager->on_leadership_change(partition->ntp(), is_leader);
}

void service::on_unmanage_notification(model::topic_partition_view tp) {
    vlog(
      pllog.trace, "on_unmanage_notification: {}/{}", tp.topic, tp.partition);
    model::ntp ntp(model::kafka_namespace, tp.topic, tp.partition);
    _queue.submit([this, ntp = std::move(ntp)] mutable {
        return handle_on_unmanage_notification(std::move(ntp));
    });
}
ss::future<> service::handle_on_unmanage_notification(model::ntp ntp) {
    vlog(pllog.trace, "handling unmanage notification: {}", ntp);
    if (ss::this_shard_id() != manager_shard) {
        co_return co_await container().invoke_on(
          manager_shard, [ntp = std::move(ntp)](auto& s) mutable {
              return s.handle_on_unmanage_notification(std::move(ntp));
          });
    }
    _manager->on_leadership_change(ntp, ntp_leader::no);
}

void service::on_manage_notification(
  const ss::lw_shared_ptr<cluster::partition>& partition) {
    vlog(pllog.trace, "on_manage_notification: {}", partition->ntp());
    _queue.submit(
      [this, partition] { return handle_on_manage_notification(partition); });
}

ss::future<> service::handle_on_manage_notification(
  ss::foreign_ptr<ss::lw_shared_ptr<cluster::partition>> partition) {
    vlog(pllog.trace, "handling manage notification: {}", partition->ntp());
    if (ss::this_shard_id() != manager_shard) {
        co_return co_await container().invoke_on(
          manager_shard, [partition = std::move(partition)](auto& s) mutable {
              return s.handle_on_manage_notification(std::move(partition));
          });
    }
    _manager->on_leadership_change(
      partition->ntp(),
      partition->is_elected_leader() ? ntp_leader::yes : ntp_leader::no);
}
} // namespace panda_link
