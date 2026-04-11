/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/read_replica/metadata_manager.h"

#include "cloud_topics/logger.h"
#include "cloud_topics/read_replica/snapshot_manager.h"
#include "cloud_topics/read_replica/state_refresh_loop.h"
#include "cloud_topics/read_replica/stm.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "ssx/work_queue.h"

namespace cloud_topics::read_replica {

metadata_manager::metadata_manager(
  std::unique_ptr<cluster::partition_change_notifier> notifier,
  ss::sharded<cluster::partition_manager>& partition_mgr,
  snapshot_manager* snapshot_mgr,
  ss::sharded<cloud_io::remote>& remote,
  ss::sharded<cluster::topic_table>& topics)
  : notifier_(std::move(notifier))
  , partition_mgr_(partition_mgr)
  , topic_table_(topics)
  , queue_([](const std::exception_ptr& ex) {
      vlog(cd_log.warn, "Metadata manager work queue error: {}", ex);
  })
  , snapshot_mgr_(snapshot_mgr)
  , remote_(remote) {}

metadata_manager::~metadata_manager() = default;

void metadata_manager::start() {
    using notif_type = cluster::partition_change_notifier::notification_type;
    using partition_state = cluster::partition_change_notifier::partition_state;
    using notify_current
      = cluster::partition_change_notifier::notify_current_state;

    notification_ = notifier_->register_partition_notifications(
      [this](
        notif_type type,
        const model::ntp& ntp,
        std::optional<partition_state> state) {
          on_partition_notification(type, ntp, std::move(state));
      },
      notify_current::yes);
}

ss::future<> metadata_manager::stop() {
    vlog(cd_log.info, "Metadata manager stopping");

    // Unregister notifications first so we don't get more calls to start new
    // refresh loop.
    if (notification_) {
        vlog(cd_log.info, "Unregistering notifications");
        auto id = std::exchange(notification_, std::nullopt).value();
        notifier_->unregister_partition_notifications(id);
        vlog(cd_log.info, "Unregistered notifications");
    }

    // Stop the queue to prevent new refresh_loops from being added.
    vlog(cd_log.info, "Shutting down queue");
    co_await queue_.shutdown();
    for (auto& [ntp, loop] : refresh_loops_) {
        if (loop) {
            vlog(cd_log.debug, "Stopping refresh loop for {}", ntp);
            co_await loop->stop_and_wait();
            vlog(cd_log.debug, "Stopped refresh loop for {}", ntp);
        }
    }
    vlog(cd_log.info, "Shut down queue");
    refresh_loops_.clear();
    metadata_.clear();
    vlog(cd_log.info, "Metadata manager stopped");
}

void metadata_manager::on_partition_notification(
  cluster::partition_change_notifier::notification_type type,
  const model::ntp& ntp,
  std::optional<cluster::partition_change_notifier::partition_state> state) {
    using notif_type = cluster::partition_change_notifier::notification_type;

    std::optional<cluster::topic_configuration> config
      = state.and_then([](auto state) { return std::move(state.topic_cfg); })
          .or_else([this, &ntp] {
              return topic_table_.local().get_topic_cfg({ntp.ns, ntp.tp.topic});
          });
    if (!config) {
        // The topic was likely deleted.
        metadata_.erase(ntp);
        enqueue_refresh_loop_stop(ntp);
        return;
    }
    if (!config->is_cloud_topic() || !config->is_read_replica()) {
        return;
    }
    auto remote_label = config->properties.remote_label;
    if (!remote_label) {
        vlog(cd_log.error, "Missing remote label: {}", ntp);
        return;
    }
    if (!config->tp_id) {
        vlog(cd_log.error, "Missing topic ID: {}", ntp);
        return;
    }
    std::optional<model::term_id> term_opt;
    if (state && state->is_leader) {
        term_opt = state->term;
    }
    const auto& tp_id = *config->tp_id;
    switch (type) {
    case notif_type::partition_replica_assigned:
        on_partition_change(ntp, tp_id, /*has_partition=*/true);
        on_leadership_change(ntp, tp_id, *remote_label, term_opt);
        break;
    case notif_type::partition_replica_unassigned:
        on_partition_change(ntp, tp_id, /*has_partition=*/false);
        on_leadership_change(ntp, tp_id, *remote_label, std::nullopt);
        break;
    case notif_type::leadership_change:
        on_leadership_change(ntp, tp_id, *remote_label, term_opt);
        break;
    case notif_type::partition_properties_change:
        break;
    }
}

void metadata_manager::on_leadership_change(
  const model::ntp& ntp,
  const model::topic_id& topic_id,
  const cloud_storage::remote_label& remote_label,
  std::optional<model::term_id> term) {
    if (term.has_value()) {
        auto partition = partition_mgr_.local().get(ntp);
        if (partition) {
            enqueue_refresh_loop(
              std::move(partition), term.value(), topic_id, remote_label);
        } else {
            vlog(
              cd_log.error,
              "Missing managed partition on becoming leader: {}",
              ntp);
        }

        auto p_meta = metadata_.find(ntp);
        if (p_meta != metadata_.end()) {
            p_meta->second.last_leadership_time = ss::lowres_clock::now();
        } else {
            vlog(
              cd_log.error,
              "Missing partition metadata on becoming leader: {}",
              ntp);
        }
    } else {
        enqueue_refresh_loop_stop(ntp);
    }
}

void metadata_manager::on_partition_change(
  const model::ntp& ntp, const model::topic_id& topic_id, bool has_partition) {
    if (has_partition) {
        auto partition = partition_mgr_.local().get(ntp);
        if (!partition) {
            vlog(
              cd_log.error,
              "Missing partition on registering partition: {}",
              ntp);
            return;
        }
        auto bucket = partition->get_read_replica_bucket();
        model::topic_id_partition tidp{topic_id, ntp.tp.partition};
        partition_metadata metadata{
          .creation_time = ss::lowres_clock::now(),
          .tidp = tidp,
          .bucket = bucket,
        };
        metadata_.insert_or_assign(ntp, std::move(metadata));
    } else {
        metadata_.erase(ntp);
    }
}

void metadata_manager::enqueue_refresh_loop(
  ss::lw_shared_ptr<cluster::partition> partition,
  model::term_id term,
  model::topic_id topic_id,
  cloud_storage::remote_label remote_label) {
    queue_.submit(
      [this, partition, topic_id, term, remote_label]() mutable
        -> ss::future<> {
          start_refresh_loop(
            std::move(partition), term, topic_id, remote_label);
          return ss::make_ready_future();
      });
}

void metadata_manager::enqueue_refresh_loop_stop(const model::ntp& ntp) {
    queue_.submit(
      [this, ntp]() -> ss::future<> { return stop_refresh_loop(ntp); });
}

std::optional<partition_metadata>
metadata_manager::get_metadata(const model::ntp& ntp) const {
    auto it = metadata_.find(ntp);
    if (it == metadata_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void metadata_manager::start_refresh_loop(
  ss::lw_shared_ptr<cluster::partition> partition,
  model::term_id term,
  model::topic_id topic_id,
  cloud_storage::remote_label remote_label) {
    model::topic_id_partition tidp{topic_id, partition->ntp().tp.partition};
    auto& stm_manager = partition->raft()->stm_manager();
    auto stm_ptr = stm_manager->get<stm>();
    auto loop = std::make_unique<state_refresh_loop>(
      term,
      tidp,
      std::move(stm_ptr),
      snapshot_mgr_,
      partition->get_read_replica_bucket(),
      remote_label,
      remote_.local());

    loop->start();
    refresh_loops_.emplace(partition->ntp(), std::move(loop));
}

ss::future<> metadata_manager::stop_refresh_loop(model::ntp ntp) {
    auto it = refresh_loops_.find(ntp);
    if (it == refresh_loops_.end()) {
        co_return;
    }
    auto loop = std::move(it->second);
    refresh_loops_.erase(it);
    co_await loop->stop_and_wait();
}

} // namespace cloud_topics::read_replica
