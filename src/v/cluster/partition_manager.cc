// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_manager.h"

#include "base/vlog.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_label.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/remote_path_provider.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/types.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/partition.h"
#include "cluster/partition_recovery_manager.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/fundamental.h"
#include "raft/group_configuration.h"
#include "raft/rpc_client_protocol.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/async-clear.h"
#include "storage/segment_utils.h"
#include "storage/snapshot.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

#include <algorithm>
#include <exception>
#include <iterator>
#include <utility>

namespace cluster {

partition_manager::partition_manager(
  ss::sharded<storage::api>& storage,
  ss::sharded<raft::group_manager>& raft,
  ss::sharded<cloud_storage::partition_recovery_manager>& recovery_mgr,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<cloud_storage::cache>& cloud_storage_cache,
  ss::lw_shared_ptr<const archival::configuration> archival_conf,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<archival::upload_housekeeping_service>& upload_hks,
  config::binding<std::chrono::milliseconds> partition_shutdown_timeout)
  : _storage(storage.local())
  , _raft_manager(raft)
  , _partition_recovery_mgr(recovery_mgr)
  , _cloud_storage_api(cloud_storage_api)
  , _cloud_storage_cache(cloud_storage_cache)
  , _archival_conf(std::move(archival_conf))
  , _feature_table(feature_table)
  , _upload_hks(upload_hks)
  , _partition_shutdown_timeout(std::move(partition_shutdown_timeout)) {
    _leader_notify_handle
      = _raft_manager.local().register_leadership_notification(
        [this](
          raft::group_id group,
          model::term_id term [[maybe_unused]],
          std::optional<model::node_id> leader_id) {
            auto p = partition_for(group);
            if (p) {
                auto a = p->archiver();
                if (a) {
                    a.value().get().notify_leadership(leader_id);
                }
            }
        });
    _shutdown_watchdog.set_callback(
      [this] { check_partitions_shutdown_state(); });
}

partition_manager::~partition_manager() {
    if (_leader_notify_handle) {
        _raft_manager.local().unregister_leadership_notification(
          *_leader_notify_handle);
    }
}

partition_manager::ntp_table_container
partition_manager::get_topic_partition_table(
  model::topic_namespace_view tn) const {
    ntp_table_container rs;
    for (const auto& p : _ntp_table) {
        if (p.second->ntp().ns == tn.ns && p.second->ntp().tp.topic == tn.tp) {
            rs.emplace(p.first, p.second);
        }
    }
    return rs;
}

ss::future<> partition_manager::start() {
    maybe_arm_shutdown_watchdog();
    co_return;
}

ss::future<consensus_ptr> partition_manager::manage(
  storage::ntp_config ntp_cfg,
  raft::group_id group,
  std::vector<model::broker> initial_nodes,
  raft::with_learner_recovery_throttle enable_learner_recovery_throttle,
  raft::keep_snapshotted_log keep_snapshotted_log,
  std::optional<xshard_transfer_state> xst_state,
  std::optional<remote_topic_properties> rtp,
  std::optional<cloud_storage_clients::bucket_name> read_replica_bucket,
  std::optional<cloud_storage::remote_label> remote_label,
  std::optional<model::topic_namespace> topic_namespace_override) {
    vlog(
      clusterlog.trace,
      "Creating partition with configuration: {}, raft group_id: {}, "
      "initial_nodes: {}, remote topic properties: {}, remote label: {}, "
      "topic_namespace_override: {}",
      ntp_cfg,
      group,
      initial_nodes,
      rtp,
      remote_label,
      topic_namespace_override);

    auto guard = _gate.hold();
    // topic_namespace_override is used in case of a cluster migration.
    // The original ("source") topic name must be used in the tiered
    // storage/archival subsystems, while the alias ("destination") will be used
    // for local storage on the new cluster.
    if (topic_namespace_override.has_value()) {
        vlog(
          clusterlog.info,
          "Topic namespace override present for ntp {}: topic namespace {} "
          "used for remote path providing",
          ntp_cfg.ntp(),
          topic_namespace_override.value());
    }

    // NOTE: while the source cluster UUIDs of the path providers will
    // ultimately be the same, this is a different path provider than what will
    // be used at runtime by the partition. The latter is owned by the archival
    // metadata STM and its lifecycle is therefore tied to the partition, which
    // hasn't been constructed yet.
    cloud_storage::remote_path_provider path_provider(
      remote_label, topic_namespace_override);
    auto dl_result = co_await maybe_download_log(ntp_cfg, rtp, path_provider);

    auto& [logs_recovered, clean_download, min_offset, max_offset, manifest, ot_state]
      = dl_result;
    if (logs_recovered) {
        vlog(
          clusterlog.info,
          "Log download complete, ntp: {}, rev: {}, "
          "min_offset: {}, max_offset: {}",
          ntp_cfg.ntp(),
          ntp_cfg.get_revision(),
          min_offset,
          max_offset);
        if (!clean_download) {
            vlog(
              clusterlog.error,
              "{} partition recovery is not clean, try to delete the topic "
              "and retry recovery",
              ntp_cfg.ntp());
            manifest.disable_permanently();
        }

        if (min_offset == max_offset && min_offset == model::offset{0}) {
            // Here two cases are possible:
            // - Recover failed and we didn't download anything.
            //   In this case we need to create empty partition and disable
            //   updates to archival STM to preserve data.
            // - The manifest was empty. We don't need to do anything, just
            //   start from an empty slate.
            vlog(
              clusterlog.info,
              "{} no data in the downloaded segments, empty partition will be "
              "created",
              ntp_cfg.ntp());

            if (!clean_download) {
                // No data was downloaded because of error, in this case it's
                // not safe to upload data to the cloud. The snapshot should be
                // created to disable uploads.
                co_await archival_metadata_stm::make_snapshot(
                  ntp_cfg, manifest, max_offset);
            }
        } else {
            // Manifest is not empty since we were able to recovery
            // some data.
            auto last_segment = manifest.last_segment();
            vassert(last_segment.has_value(), "Manifest is empty");
            auto last_included_term = last_segment->archiver_term;

            vlog(
              clusterlog.info,
              "Bootstrap on-disk state for pre existing partition {}. "
              "Group: {}, "
              "Min offset: {}, "
              "Max offset: {}, "
              "Last included term: {}, ",
              ntp_cfg.ntp(),
              group,
              min_offset,
              max_offset,
              last_included_term);

            if (min_offset > max_offset) {
                // No data was downloaded. In this case we're doing shallow
                // recovery. The dl_result is not populated with data yet so we
                // have to provide initial delta value.
                vassert(
                  manifest.last_segment().has_value(), "Manifest is empty");

                min_offset = model::next_offset(manifest.get_last_offset());
                max_offset = min_offset;
                co_await seastar::recursive_touch_directory(
                  ntp_cfg.work_directory());
            }

            dl_result.ot_state->add_absolute_delta(
              model::next_offset(manifest.get_last_offset()),
              manifest.last_segment()->delta_offset_end);

            co_await raft::details::bootstrap_pre_existing_partition(
              _storage,
              ntp_cfg,
              group,
              min_offset,
              max_offset,
              last_included_term,
              initial_nodes,
              dl_result.ot_state);

            // Initialize archival snapshot
            co_await archival_metadata_stm::make_snapshot(
              ntp_cfg, manifest, max_offset);
        }
    }
    auto translator_batch_types = raft::offset_translator_batch_types(
      ntp_cfg.ntp());
    auto log = co_await _storage.log_mgr().manage(
      std::move(ntp_cfg), group, std::move(translator_batch_types));
    vlog(
      clusterlog.debug,
      "Log created manage completed, ntp: {}, rev: {}, {} "
      "segments, {} bytes",
      log->config().ntp(),
      log->config().get_revision(),
      log->segment_count(),
      log->size_bytes());

    ss::lw_shared_ptr<raft::consensus> c
      = co_await _raft_manager.local().create_group(
        group,
        std::move(initial_nodes),
        log,
        enable_learner_recovery_throttle,
        keep_snapshotted_log);

    auto p = ss::make_lw_shared<partition>(
      c,
      _cloud_storage_api,
      _cloud_storage_cache,
      _archival_conf,
      _feature_table,
      _upload_hks,
      read_replica_bucket);

    _ntp_table.emplace(log->config().ntp(), p);
    _raft_table.emplace(group, p);

    /*
     * part of the node leadership draining infrastructure. when a node is
     * in a drianing state new groups might be created since the controller
     * will still be active as a follower. however, if draining is almost
     * complete then new groups may not be noticed. marking as blocked
     * should be done atomically with adding the partition to the ntp_table
     * index above for proper synchronization with the drianing manager.
     */
    if (_block_new_leadership) {
        p->block_new_leadership();
    }

    co_await p->start(_stm_registry, xst_state);

    _manage_watchers.notify(p->ntp(), p);

    co_return c;
}

ss::future<cloud_storage::log_recovery_result>
partition_manager::maybe_download_log(
  storage::ntp_config& ntp_cfg,
  std::optional<remote_topic_properties> rtp,
  cloud_storage::remote_path_provider& path_provider) {
    if (rtp.has_value() && _partition_recovery_mgr.local_is_initialized()) {
        auto res = co_await _partition_recovery_mgr.local().download_log(
          ntp_cfg,
          rtp->remote_revision,
          rtp->remote_partition_count,
          path_provider);
        co_return res;
    }
    vlog(
      clusterlog.debug,
      "Logs can't be downloaded because cloud storage is not configured. "
      "Continue creating {} without downloading the logs.",
      ntp_cfg);
    co_return cloud_storage::log_recovery_result{};
}

ss::future<> partition_manager::stop_partitions() {
    _as.request_abort();

    _raft_manager.local().unregister_leadership_notification(
      *_leader_notify_handle);
    _leader_notify_handle.reset();

    co_await _gate.close();
    // prevent partitions from being accessed
    auto partitions = std::exchange(_ntp_table, {});

    co_await ssx::async_clear(_raft_table)();

    // shutdown all partitions
    co_await ss::max_concurrent_for_each(partitions, 1024, [this](auto& e) {
        return do_shutdown(e.second).discard_result();
    });

    co_await ssx::async_clear(partitions)();
}

ss::future<xshard_transfer_state>
partition_manager::do_shutdown(ss::lw_shared_ptr<partition> partition) {
    partition_shutdown_state shutdown_state(partition);
    _partitions_shutting_down.push_back(shutdown_state);

    xshard_transfer_state xst_state;
    try {
        auto ntp = partition->ntp();
        shutdown_state.update(partition_shutdown_stage::stopping_raft);
        xst_state.raft = co_await _raft_manager.local().shutdown(
          partition->raft());
        _unmanage_watchers.notify(ntp, model::topic_partition_view(ntp.tp));
        shutdown_state.update(partition_shutdown_stage::stopping_partition);
        co_await partition->stop();
        shutdown_state.update(partition_shutdown_stage::stopping_storage);
        co_await _storage.log_mgr().shutdown(partition->ntp());
    } catch (...) {
        vassert(
          false,
          "error shutting down partition {},  "
          "partition manager state: {}, error: {} - terminating redpanda",
          partition->ntp(),
          *this,
          std::current_exception());
    }

    co_return xst_state;
}

ss::future<>
partition_manager::remove(const model::ntp& ntp, partition_removal_mode mode) {
    auto guard = _gate.hold();

    auto partition = get(ntp);

    if (!partition) {
        throw std::invalid_argument(fmt::format(
          "Can not remove partition. NTP {} is not present in partition "
          "manager",
          ntp));
    }
    vlog(clusterlog.debug, "removing partition {}", ntp);
    partition_shutdown_state shutdown_state(partition);
    _partitions_shutting_down.push_back(shutdown_state);
    auto group_id = partition->group();

    // remove partition from ntp & raft tables
    _ntp_table.erase(ntp);
    _raft_table.erase(group_id);
    shutdown_state.update(partition_shutdown_stage::stopping_raft);
    co_await _raft_manager.local().remove(partition->raft());
    _unmanage_watchers.notify(
      ntp, model::topic_partition_view(partition->ntp().tp));
    shutdown_state.update(partition_shutdown_stage::stopping_partition);
    co_await partition->stop();
    shutdown_state.update(partition_shutdown_stage::removing_persistent_state);
    co_await partition->remove_persistent_state();
    shutdown_state.update(partition_shutdown_stage::removing_storage);
    co_await _storage.log_mgr().remove(partition->ntp());

    if (mode == partition_removal_mode::global) {
        shutdown_state.update(
          partition_shutdown_stage::finalizing_remote_storage);
        co_await partition->finalize_remote_partition(_as);
    }
}

ss::future<xshard_transfer_state>
partition_manager::shutdown(const model::ntp& ntp) {
    auto guard = _gate.hold();

    auto partition = get(ntp);
    if (!partition) {
        return ss::make_exception_future<xshard_transfer_state>(
          std::invalid_argument(fmt::format(
            "Can not shutdown partition. NTP {} is not present in "
            "partition "
            "manager",
            ntp)));
    }
    // remove partition from ntp & raft tables
    _ntp_table.erase(ntp);
    _raft_table.erase(partition->group());

    return do_shutdown(partition);
}

partition_manager::partition_shutdown_state::partition_shutdown_state(
  ss::lw_shared_ptr<cluster::partition> p)
  : partition(std::move(p))
  , stage(partition_manager::partition_shutdown_stage::shutdown_requested)
  , last_update_timestamp(ss::lowres_clock::now()) {}

void partition_manager::partition_shutdown_state::update(
  partition_shutdown_stage s) {
    stage = s;
    last_update_timestamp = ss::lowres_clock::now();
}

void partition_manager::check_partitions_shutdown_state() {
    const auto now = ss::lowres_clock::now();
    for (auto& state : _partitions_shutting_down) {
        if (state.last_update_timestamp < now - _partition_shutdown_timeout()) {
            vlog(
              clusterlog.error,
              "partition {} shutdown takes longer than expected, current "
              "shutdown stage: {} time since last update: {} seconds",
              state.partition->ntp(),
              state.stage,
              (now - state.last_update_timestamp) / 1s);
        }
    }
    maybe_arm_shutdown_watchdog();
}

void partition_manager::maybe_arm_shutdown_watchdog() {
    if (!_as.abort_requested()) {
        _shutdown_watchdog.arm(_partition_shutdown_timeout() / 5);
    }
}

uint64_t partition_manager::upload_backlog_size() const {
    uint64_t size = 0;
    for (const auto& [_, partition] : _ntp_table) {
        size += partition->upload_backlog_size();
    }
    return size;
}

std::ostream& operator<<(std::ostream& o, const partition_manager& pm) {
    return o << "{shard:" << ss::this_shard_id() << ", mngr:{}"
             << pm._storage.log_mgr()
             << ", ntp_table.size:" << pm._ntp_table.size()
             << ", raft_table.size:" << pm._raft_table.size() << "}";
}

ss::future<size_t> partition_manager::non_log_disk_size_bytes() const {
    co_return co_await container().map_reduce0(
      [](const partition_manager& pm) {
          const auto size = std::accumulate(
            pm._raft_table.cbegin(),
            pm._raft_table.cend(),
            size_t{0},
            [](size_t acc, const auto& elem) {
                return acc + elem.second->non_log_disk_size_bytes();
            });
          return size;
      },
      size_t{0},
      [](size_t acc, size_t update) { return acc + update; });
}

ss::future<cloud_storage::cache_usage_target>
partition_manager::get_cloud_cache_disk_usage_target() const {
    co_return co_await container().map_reduce0(
      [](const partition_manager& pm) {
          const auto size = std::accumulate(
            pm._raft_table.cbegin(),
            pm._raft_table.cend(),
            cloud_storage::cache_usage_target{},
            [](auto acc, const auto& p) {
                if (p.second->remote_partition()) {
                    acc = acc
                          + p.second->remote_partition()
                              ->get_cache_usage_target();
                }
                return acc;
            });
          return size;
      },
      cloud_storage::cache_usage_target{},
      [](auto acc, auto update) { return acc + update; });
}

std::ostream& operator<<(
  std::ostream& o, const partition_manager::partition_shutdown_stage& stage) {
    switch (stage) {
    case partition_manager::partition_shutdown_stage::shutdown_requested:
        return o << "shutdown_requested";
    case partition_manager::partition_shutdown_stage::stopping_raft:
        return o << "stopping_raft";
    case partition_manager::partition_shutdown_stage::removing_raft:
        return o << "removing_raft";
    case partition_manager::partition_shutdown_stage::stopping_partition:
        return o << "stopping_partition";
    case partition_manager::partition_shutdown_stage::removing_persistent_state:
        return o << "removing_persistent_state";
    case partition_manager::partition_shutdown_stage::stopping_storage:
        return o << "stopping_storage";
    case partition_manager::partition_shutdown_stage::removing_storage:
        return o << "removing_storage";
    case partition_manager::partition_shutdown_stage::finalizing_remote_storage:
        return o << "finalizing_remote_storage";
    }
}

} // namespace cluster
