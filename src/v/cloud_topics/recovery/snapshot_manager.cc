// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/recovery/snapshot_manager.h"

#include "cloud_topics/dl_snapshot.h"
#include "cloud_topics/dl_stm/dl_stm.h"
#include "cloud_topics/dl_stm/dl_stm_api.h"
#include "cloud_topics/recovery/logger.h"
#include "cloud_topics/recovery/managed_partition.h"
#include "cloud_topics/recovery/snapshot_io.h"
#include "cluster/partition_manager.h"
#include "raft/group_manager.h"
#include "serde/rw/rw.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>

#include <memory>

namespace experimental::cloud_topics::recovery {

namespace {
static constexpr auto snapshot_interval = 5 * 60s;
static constexpr auto max_concurrent_snapshots = 10;
}; // namespace

snapshot_manager::snapshot_manager(
  cluster::partition_manager* pm,
  raft::group_manager* gm,
  cloud_io::remote* cloud_io,
  cloud_storage_clients::bucket_name bucket)
  : _partition_manager(pm)
  , _raft_manager(gm)
  , _snapshot_io(std::make_unique<snapshot_io>(cloud_io, std::move(bucket))) {}

snapshot_manager::~snapshot_manager() = default;

ss::future<> snapshot_manager::start() {
    vlog(lg.info, "Starting snapshot manager");

    _manage_notify_handle = _partition_manager->register_manage_notification(
      model::kafka_namespace, [this](ss::lw_shared_ptr<cluster::partition> p) {
          attach_partition(std::move(p));
      });

    _unmanage_notify_handle
      = _partition_manager->register_unmanage_notification(
        model::kafka_namespace, [this](model::topic_partition_view tp_p) {
            detach_partition(
              model::ntp(model::kafka_namespace, tp_p.topic, tp_p.partition));
        });

    _raft_manager_notify_handle
      = _raft_manager->register_leadership_notification(
        [this](
          raft::group_id group, model::term_id, std::optional<model::node_id>) {
            auto partition_ptr = _partition_manager->partition_for(group);
            // We were just notified about leadership change synchronously. The
            // partition must exist.
            vassert(partition_ptr, "No partition for group {}", group);
            // Calling the next method synchronously allows us to inspect the
            // partition leadership state directly.
            handle_partition_leader_change(partition_ptr->ntp());
        });

    ssx::spawn_with_gate(_gate, [this] { return scheduler_loop(); });

    co_return;
}

ss::future<> snapshot_manager::stop() {
    vlog(lg.debug, "Stopping snapshot manager");

    _raft_manager->unregister_leadership_notification(
      _raft_manager_notify_handle);
    _partition_manager->unregister_manage_notification(_manage_notify_handle);
    _partition_manager->unregister_unmanage_notification(
      _unmanage_notify_handle);

    _as.request_abort();

    // If there are still attached partitions, req
    for (auto& [ntp, f] : _snapshots_in_progress) {
        vlog(lg.debug, "Aborting snapshot of partition {}", ntp);
        _attached_partitions.at(ntp)->as.request_abort();
    }

    co_await _gate.close();

    // The gate is closed, so there are no more snapshots in progress.
    vassert(
      _snapshots_in_progress.empty(),
      "There are still snapshots in progress when stopping snapshot manager");

    vlog(lg.debug, "Snapshot manager stopped");
}

void snapshot_manager::attach_partition(
  ss::lw_shared_ptr<cluster::partition> p) {
    if (!p->get_ntp_config().cloud_topic_enabled()) {
        return;
    }

    auto ntp = p->ntp();
    auto initial_rev = p->get_ntp_config().get_initial_revision();
    vlog(
      lg.info,
      "Attaching partition {} with initial revision {}",
      ntp,
      initial_rev);

    auto partition_info = ss::make_lw_shared<managed_partition>(
      managed_partition{initial_rev, std::move(p)});

    auto res = _attached_partitions.try_emplace(ntp, partition_info);

    vassert(res.second, "Double registration of ntp {}", ntp);
}

void snapshot_manager::detach_partition(const model::ntp& ntp) {
    if (auto it = _attached_partitions.find(ntp);
        it != _attached_partitions.end()) {
        vlog(lg.info, "Detaching partition {}", ntp);

        // Request abort of the per-partition operations.
        it->second->as.request_abort();

        // Forget the partition. Note that there might still be async
        // operations in-progress for this partition. We are not waiting for
        // them to complete but also not erasing the partition from the
        // in-progress list. If we become leader again, we will wait for the
        // previous operations to complete before starting new ones. The waiting
        // is achieved by keying the in-progress list by the ntp and allowing
        // only one operation per ntp.
        _attached_partitions.erase(it);
    }
}

void snapshot_manager::handle_partition_leader_change(const model::ntp& ntp) {
    auto it = _attached_partitions.find(ntp);
    if (it == _attached_partitions.end()) {
        // We don't manage this partition.
        return;
    }
    auto info = it->second;
    if (info->partition->is_leader()) {
        auto should_recover = info->partition->get_ntp_config().has_overrides()
                              && info->partition->get_ntp_config()
                                   .get_overrides()
                                   .cloud_recovery_enabled;
        if (should_recover) {
            vlog(
              lg.info,
              "Partition {} acquired leadership. Starting recovery in term {}.",
              ntp,
              info->partition->term());
            info->current_state = managed_partition::state::recovering;
        } else {
            vlog(
              lg.info,
              "Partition {} acquired leadership. Activating in term {}.",
              ntp,
              info->partition->term());
            info->switch_to_active(info->partition->term());
        }
    } else {
        vlog(
          lg.info,
          "Partition {} lost leadership. Stopping snapshot operations",
          ntp);
        auto p = info->partition;

        // This will abort any in-progress operations.
        detach_partition(ntp);
        attach_partition(std::move(p));
    }
}

ss::future<> snapshot_manager::scheduler_loop() {
    while (!_as.abort_requested()) {
        try {
            co_await scheduler_loop_once();
        } catch (...) {
            vlog(
              lg.error,
              "Error in snapshot manager scheduler loop: {}",
              std::current_exception());
        }

        co_await ss::sleep_abortable(5s, _as);
    }
}

ss::future<> snapshot_manager::scheduler_loop_once() {
    // TODO(nv): There are better ways to do this. We can use a priority
    // queue to schedule the next snapshot operation and a timer for
    // waking up the loop.
    for (auto& [ntp, partition] : _attached_partitions) {
        // If we have less than max_concurrent_snapshots in progress, we
        // can start a new snapshot operation.
        if (_snapshots_in_progress.size() >= max_concurrent_snapshots) {
            break;
        }

        bool past_deadline = partition->next_schedule_deadline
                             <= ss::lowres_clock::now();
        // Note: There might be a snapshot in progress from a previous
        // term. We will always wait for the previous snapshot to
        // complete.
        if (past_deadline && !_snapshots_in_progress.contains(ntp)) {
            if (partition->current_state == managed_partition::state::active) {
                schedule_snapshot(partition);
            } else if (
              partition->current_state
              == managed_partition::state::recovering) {
                schedule_recovery(partition);
            }
        }

        co_await ss::maybe_yield();
    }
}

void snapshot_manager::schedule_snapshot(
  ss::lw_shared_ptr<managed_partition> partition_info) {
    vlog(
      lg.trace,
      "Scheduling snapshot of partition {}",
      partition_info->partition->ntp());

    _snapshots_in_progress.emplace(
      partition_info->partition->ntp(),
      ss::with_gate(_gate, [this, partition_info = std::move(partition_info)] {
          return take_snapshot(partition_info)
            .handle_exception([partition_info](std::exception_ptr e) {
                vlog(
                  lg.error,
                  "Error taking snapshot of partition {}: {}",
                  partition_info->partition->ntp(),
                  e);
            })
            .finally([this, partition_info] {
                // Remove the snapshot from the in-progress list.
                _snapshots_in_progress.erase(partition_info->partition->ntp());

                // And schedule the next snapshot. No problem if we don't
                // manage the partition anymore. The object will be
                // destroyed and the scheduler won't pick it up.
                // TODO(nv): We should take into account how many sequential
                // errors this operation has had and back off.
                partition_info->next_schedule_deadline = ss::lowres_clock::now()
                                                         + snapshot_interval;
            });
      }));
}

void snapshot_manager::schedule_recovery(
  ss::lw_shared_ptr<managed_partition> partition_info) {
    vlog(
      lg.trace,
      "Scheduling recovery of partition {}",
      partition_info->partition->ntp());

    _snapshots_in_progress.emplace(
      partition_info->partition->ntp(),
      ss::with_gate(_gate, [this, partition_info = std::move(partition_info)] {
          return recover_snapshot(partition_info)
            .handle_exception([partition_info](std::exception_ptr e) {
                vlog(
                  lg.error,
                  "Error installing snapshot of partition {}: {}",
                  partition_info->partition->ntp(),
                  e);
            })
            .finally([this, partition_info] {
                // Remove the snapshot from the in-progress list.
                _snapshots_in_progress.erase(partition_info->partition->ntp());
            });
      }));
}

ss::future<>
snapshot_manager::take_snapshot(ss::lw_shared_ptr<managed_partition> info) {
    auto rtc = retry_chain_node{info->as, 300s, 100ms};

    vlog(lg.info, "Taking snapshot of partition {}", info->partition->ntp());

    // TODO(nv): Make it easier to access the api. Consider embedding it into
    // the stm itself.
    auto stm = info->partition->raft()->stm_manager()->get<dl_stm>();
    auto stm_api = dl_stm_api(lg, &*stm);

    auto start_snapshot_result = co_await stm_api.start_snapshot();
    if (start_snapshot_result.has_error()) {
        vlog(
          lg.error,
          "Failed to start snapshot of partition {}: {}",
          info->partition->ntp(),
          start_snapshot_result.error());
        throw std::runtime_error(fmt::format(
          "Failed to start snapshot: {}", start_snapshot_result.error()));
    }

    auto dl_snapshot_id = start_snapshot_result.value();

    // TODO(nv): Send dl_snapshot request to the partition which will return a
    // snapshot_id.
    // TODO(nv): Request the snapshot from the partition.
    // TODO(nv): Upload the snapshot to cloud storage.
    // TODO(nv): Discard previous snapshots.
    // TODO(nv): Delete old snapshots from cloud storage.

    // TODO(nv): Dummy.

    auto id = ntp_snapshot_id(
      info->partition->ntp().ns,
      info->partition->ntp().tp.topic,
      info->partition->ntp().tp.partition,
      info->initial_rev,
      dl_snapshot_id.version);

    auto payload = stm_api.read_snapshot(dl_snapshot_id);
    if (!payload.has_value()) {
        vlog(
          lg.error,
          "Failed to read snapshot of partition {}",
          info->partition->ntp());
        throw std::runtime_error(
          fmt::format("Failed to read snapshot {}", dl_snapshot_id.version));
    }

    auto buf = serde::to_iobuf(std::move(payload).value());

    co_await _snapshot_io->upload_snapshot(id, std::move(buf), rtc);

    co_await ss::sleep_abortable(6s, info->as);

    vlog(lg.info, "Snapshot of partition {} completed", info->partition->ntp());

    vlog(
      lg.info,
      "Deleting old snapshots of partition {}. Last to keep: {}",
      info->partition->ntp(),
      id.object_key());

    // TODO(nv): Ask STM what is the oldest snapshot we should keep.
    co_await _snapshot_io->delete_snapshots_before(id, rtc);
}

ss::future<>
snapshot_manager::recover_snapshot(ss::lw_shared_ptr<managed_partition> info) {
    auto rtc = retry_chain_node{info->as, 3600s, 100ms};

    /*
     * TODO(nv): How this will work with incremental snapshots? To install
     * something we need to create the partition first. But when do we set
     * correctly the raft/kafka delta?
     *
     * See `raft::details::bootstrap_pre_existing_partition`. However, maybe
     * we'll do it differently here. We'll start the raft partition as if it was
     * empty but won't expose it to the clients just yet. Then, we'll install
     * the snapshot, replay logs if needed, and then expose the partition to the
     * clients.
     */

    auto prefix = ntp_snapshot_prefix(
      info->partition->ntp().ns,
      info->partition->ntp().tp.topic,
      info->partition->ntp().tp.partition,
      info->initial_rev);

    auto latest_snapshot_id = co_await _snapshot_io->find_latest_snapshot(
      prefix, rtc);
    if (!latest_snapshot_id) {
        vlog(
          lg.warn,
          "No snapshot found for partition {}. Starting from scratch",
          info->partition->ntp());

        // TODO(nv): Install empty snapshot.
    } else {
        auto snapshot_buf = co_await _snapshot_io->download_snapshot(
          *latest_snapshot_id, rtc);

        vlog(
          lg.info,
          "Recovering partition {} from snapshot {}",
          info->partition->ntp(),
          latest_snapshot_id->object_key());

        // TODO(nv): Install the snapshot to the partition. IDEMPOTENT!
    }

    // TODO(nv): Recovery is done, we can expose the partition to the clients.
    //   We also need to make sure that no new attempts to recover the partition
    //   will be made. To do this we need to change the partition config.
    //   Note that partition overrides are set based on the topic overrides so
    //   we must not change the topic overrides until all partitions are
    //   recovered...

    // Finally, let new snapshots be taken.
    vlog(
      lg.info,
      "Recovery of partition {} completed. Activating in term {}",
      info->partition->ntp(),
      info->partition->term());

    info->switch_to_active(info->partition->term());
}

} // namespace experimental::cloud_topics::recovery
