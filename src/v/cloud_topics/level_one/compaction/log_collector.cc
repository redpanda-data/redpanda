/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_collector.h"

#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_configuration.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

ss::future<> log_collector::start() { co_await start_collecting_logs(); }

ss::future<> log_collector::stop() { co_await stop_collecting_logs(); }

partition_leader_log_collector::partition_leader_log_collector(
  manage_cb_t manage_cb,
  unmanage_cb_t unmanage_cb,
  is_managed_cb_t is_managed_cb,
  model::node_id self,
  ss::sharded<cluster::partition_leaders_table>* leaders,
  ss::sharded<cluster::topic_table>* topic_table)
  : _manage_cb(std::move(manage_cb))
  , _unmanage_cb(std::move(unmanage_cb))
  , _is_managed_cb(std::move(is_managed_cb))
  , _self(self)
  , _leaders(leaders)
  , _topic_table(topic_table) {}

ss::future<> partition_leader_log_collector::start_collecting_logs() {
    _ntp_notify_handle = _topic_table->local().register_ntp_delta_notification(
      [this](cluster::topic_table::ntp_delta_range_t deltas) {
          for (const auto& delta : deltas) {
              on_ntp_change(delta);
          }
      });
    _leader_notify_handle
      = _leaders->local().register_leadership_change_notification(
        [this](const model::ntp& ntp, model::term_id, model::node_id leader) {
            on_leadership_change(ntp, leader);
        });
    co_return;
}

ss::future<> partition_leader_log_collector::stop_collecting_logs() {
    auto close_fut = _gate.close();
    _topic_table->local().unregister_ntp_delta_notification(_ntp_notify_handle);
    _leaders->local().unregister_leadership_change_notification(
      _leader_notify_handle);
    co_await std::move(close_fut);
    co_return;
}

void partition_leader_log_collector::on_ntp_change(
  const cluster::topic_table::ntp_delta& delta) {
    const auto& ntp = delta.ntp;
    auto is_managed = _is_managed_cb(ntp);

    using delta_type = cluster::topic_table_ntp_delta_type;
    switch (delta.type) {
    case delta_type::removed: {
        // Partition/possibly topic was removed. Unmanage it if necessary.
        if (is_managed) {
            _unmanage_cb(ntp, "Partition removed");
        }
        return;
    }
    case delta_type::properties_updated: {
        auto topic_cfg_opt = _topic_table->local().get_topic_cfg(
          model::topic_namespace_view{ntp});
        if (!topic_cfg_opt.has_value()) {
            return;
        }

        auto& topic_cfg = topic_cfg_opt.value();

        auto is_compacted_cloud_topic = topic_cfg.is_compacted()
                                        && topic_cfg.is_cloud_topic();
        auto is_leader_for = _leaders->local().get_leader(ntp) == _self;
        if (is_compacted_cloud_topic && is_leader_for && !is_managed) {
            // This is likely an existing cloud topic which is now `compact`
            // enabled. We should manage it iff this broker already hosts
            // the partition leader.
            auto tp_id = topic_cfg.tp_id;
            vassert(tp_id.has_value(), "Expected tp_id to have value.");
            auto tidp = model::topic_id_partition(
              tp_id.value(), ntp.tp.partition);

            _manage_cb(ntp, tidp, "Enabled compaction");
        }

        if (!is_compacted_cloud_topic && is_managed) {
            // This is likely an existing cloud topic which is no longer
            // `compact` enabled.
            _unmanage_cb(ntp, "Disabled compaction");
        }
        return;
    }
    case delta_type::added:
        [[fallthrough]];
    case delta_type::replicas_updated:
        [[fallthrough]];
    case delta_type::disabled_flag_updated:
        return;
    }
}

void partition_leader_log_collector::on_leadership_change(
  const model::ntp& ntp, model::node_id leader) {
    auto topic_cfg_opt = _topic_table->local().get_topic_cfg(
      model::topic_namespace_view{ntp});
    if (!topic_cfg_opt.has_value()) {
        return;
    }

    auto& topic_cfg = topic_cfg_opt.value();

    auto is_compacted_cloud_topic = topic_cfg.is_compacted()
                                    && topic_cfg.is_cloud_topic();

    if (!is_compacted_cloud_topic) {
        return;
    }

    auto is_managed = _is_managed_cb(ntp);
    auto is_leader = leader == _self;

    if (is_leader && !is_managed) {
        auto tp_id = topic_cfg.tp_id;
        vassert(tp_id.has_value(), "Expected tp_id to have value.");
        auto tidp = model::topic_id_partition(tp_id.value(), ntp.tp.partition);

        _manage_cb(ntp, tidp, "Became the leader");
    }

    if (!is_leader && is_managed) {
        _unmanage_cb(ntp, "Stepped down as leader");
    }
}

std::unique_ptr<log_collector> make_default_log_collector(
  partition_leader_log_collector::manage_cb_t manage_cb,
  partition_leader_log_collector::unmanage_cb_t unmanage_cb,
  partition_leader_log_collector::is_managed_cb_t is_managed_cb,
  compaction_cluster_state state) {
    return std::make_unique<partition_leader_log_collector>(
      std::move(manage_cb),
      std::move(unmanage_cb),
      std::move(is_managed_cb),
      state.self,
      state.leaders_table,
      state.topic_table);
}

} // namespace cloud_topics::l1
