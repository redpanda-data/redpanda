/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/manager/manager.h"

#include "base/vlog.h"
#include "cloud_topics/logger.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_configuration.h"
#include "cluster/topic_table.h"
#include "cluster/utils/partition_change_notifier_impl.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace cloud_topics {

cloud_topics_manager::cloud_topics_manager(
  ss::sharded<cluster::partition_manager>* pm,
  ss::sharded<raft::group_manager>* gm,
  ss::sharded<cluster::topic_table>* tt)
  : partition_manager_(pm)
  , topic_table_(tt)
  , notifier_(
      cluster::partition_change_notifier_impl::make_default(*gm, *pm, *tt)) {}

void cloud_topics_manager::on_ctp_partition_leader(
  notification_cb_t cb) noexcept {
    ctp_callbacks_.push_back(std::move(cb));
}

void cloud_topics_manager::on_l1_domain_leader(notification_cb_t cb) noexcept {
    l1_callbacks_.push_back(std::move(cb));
}

void cloud_topics_manager::on_ctp_leader_properties_change(
  notification_cb_t cb) noexcept {
    ctp_prop_change_callbacks_.push_back(std::move(cb));
}

ss::future<> cloud_topics_manager::start() {
    using notify_current_state
      = cluster::partition_change_notifier::notify_current_state;
    using notif_type = cluster::partition_change_notifier::notification_type;
    using partition_state = cluster::partition_change_notifier::partition_state;
    notification_ = notifier_->register_partition_notifications(
      [this](
        notif_type t,
        const model::ntp& ntp,
        std::optional<partition_state> state) noexcept {
          auto is_leader = state
                             .transform([](const partition_state& state) {
                                 return state.is_leader;
                             })
                             .value_or(false);
          std::optional<cluster::topic_configuration> config
            = state
                .and_then([](partition_state state) {
                    return std::move(state.topic_cfg);
                })
                .or_else([this, &ntp] {
                    return topic_table_->local().get_topic_cfg(
                      {ntp.ns, ntp.tp.topic});
                });
          if (!config || !config->tp_id) {
              auto it = topic_id_mapping_.find(ntp);
              if (it == topic_id_mapping_.end()) {
                  // This can happen if a topic is deleted and it wasn't a cloud
                  // topic, so keep logging below INFO.
                  vlog(cd_log.debug, "unable to find topic ID for {}", ntp);
                  return;
              }
              // Always emit these even if they are not cloud topics (we can't
              // know), because it means the topic was deleted.
              model::topic_id_partition tidp{it->second, ntp.tp.partition};
              topic_id_mapping_.erase(it);
              on_leadership_change(ntp, tidp, /*is_leader=*/false);
              return;
          }
          if (
            !config->is_cloud_topic()
            && model::topic_namespace_view(ntp) != model::l1_metastore_nt) {
              return;
          }
          if (config->is_read_replica()) {
              // Read replicas are handled explicitly separately.
              return;
          }
          if (is_leader) {
              // Always ensure that if there is a leadership notification
              // emitted, that we also emit a no leader notification, even if
              // the topic is deleted and we no longer have the topic ID.
              topic_id_mapping_.try_emplace(ntp, config->tp_id.value());
          } else {
              topic_id_mapping_.erase(ntp);
          }
          model::topic_id_partition tidp{
            config->tp_id.value(), ntp.tp.partition};
          switch (t) {
          case notif_type::leadership_change:
          case notif_type::partition_replica_assigned:
          case notif_type::partition_replica_unassigned:
              on_leadership_change(ntp, tidp, is_leader);
              [[fallthrough]];
          case notif_type::partition_properties_change:
              on_leadership_or_properties_change(ntp, tidp, is_leader);
              break;
          }
      },
      notify_current_state::yes);
    co_return;
}

ss::future<> cloud_topics_manager::stop() {
    if (notification_) {
        auto id = std::exchange(notification_, std::nullopt).value();
        notifier_->unregister_partition_notifications(id);
    }
    co_return;
}

void cloud_topics_manager::on_leadership_change(
  const model::ntp& ntp,
  const model::topic_id_partition& tidp,
  bool is_leader) noexcept {
    ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition;
    if (is_leader) {
        partition = partition_manager_->local().get(ntp);
    }
    if (model::l1_metastore_nt == model::topic_namespace_view(ntp)) {
        for (const auto& cb : l1_callbacks_) {
            cb(ntp, tidp, partition);
        }
    } else {
        for (const auto& cb : ctp_callbacks_) {
            cb(ntp, tidp, partition);
        }
    }
}

void cloud_topics_manager::on_leadership_or_properties_change(
  const model::ntp& ntp,
  const model::topic_id_partition& tidp,
  bool is_leader) noexcept {
    ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition;
    if (is_leader) {
        partition = partition_manager_->local().get(ntp);
    }
    if (model::l1_metastore_nt != model::topic_namespace_view(ntp)) {
        for (const auto& cb : ctp_prop_change_callbacks_) {
            cb(ntp, tidp, partition);
        }
    }
}

} // namespace cloud_topics
