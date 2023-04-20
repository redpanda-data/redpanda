// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_manager.h"

#include "archival/ntp_archiver_service.h"
#include "archival/types.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cluster/archival_metadata_stm.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/partition_recovery_manager.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "raft/offset_translator.h"
#include "raft/rpc_client_protocol.h"
#include "raft/types.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/async-clear.h"
#include "storage/offset_translator_state.h"
#include "storage/segment_utils.h"
#include "storage/snapshot.h"
#include "utils/retry_chain_node.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

#include <algorithm>
#include <exception>
#include <iterator>

namespace cluster {

partition_manager::partition_manager(
  ss::sharded<storage::api>& storage,
  ss::sharded<raft::group_manager>& raft,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<cloud_storage::partition_recovery_manager>& recovery_mgr,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<cloud_storage::cache>& cloud_storage_cache,
  ss::lw_shared_ptr<const archival::configuration> archival_conf,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cluster::tm_stm_cache_manager>& tm_stm_cache_manager,
  ss::sharded<archival::upload_housekeeping_service>& upload_hks,
  config::binding<uint64_t> max_concurrent_producer_ids)
  : _storage(storage.local())
  , _raft_manager(raft)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _partition_recovery_mgr(recovery_mgr)
  , _cloud_storage_api(cloud_storage_api)
  , _cloud_storage_cache(cloud_storage_cache)
  , _archival_conf(std::move(archival_conf))
  , _feature_table(feature_table)
  , _tm_stm_cache_manager(tm_stm_cache_manager)
  , _upload_hks(upload_hks)
  , _max_concurrent_producer_ids(max_concurrent_producer_ids) {
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
}

partition_manager::~partition_manager() {
    if (_leader_notify_handle) {
        _raft_manager.local().unregister_leadership_notification(
          *_leader_notify_handle);
    }
}

partition_manager::ntp_table_container
partition_manager::get_topic_partition_table(
  const model::topic_namespace& tn) const {
    ntp_table_container rs;
    for (const auto& p : _ntp_table) {
        if (p.second->ntp().ns == tn.ns && p.second->ntp().tp.topic == tn.tp) {
            rs.emplace(p.first, p.second);
        }
    }
    return rs;
}

ss::future<consensus_ptr> partition_manager::manage(
  storage::ntp_config ntp_cfg,
  raft::group_id group,
  std::vector<model::broker> initial_nodes,
  std::optional<remote_topic_properties> rtp,
  std::optional<cloud_storage_clients::bucket_name> read_replica_bucket,
  raft::with_learner_recovery_throttle enable_learner_recovery_throttle,
  raft::keep_snapshotted_log keep_snapshotted_log) {
    gate_guard guard(_gate);
    auto dl_result = co_await maybe_download_log(ntp_cfg, rtp);
    auto
      [logs_recovered,
       clean_download,
       min_offset,
       max_offset,
       manifest,
       ot_state]
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
    storage::log log = co_await _storage.log_mgr().manage(std::move(ntp_cfg));
    vlog(
      clusterlog.debug,
      "Log created manage completed, ntp: {}, rev: {}, {} "
      "segments, {} bytes",
      log.config().ntp(),
      log.config().get_revision(),
      log.segment_count(),
      log.size_bytes());

    ss::lw_shared_ptr<raft::consensus> c
      = co_await _raft_manager.local().create_group(
        group,
        std::move(initial_nodes),
        log,
        enable_learner_recovery_throttle,
        keep_snapshotted_log);

    auto p = ss::make_lw_shared<partition>(
      c,
      _tx_gateway_frontend,
      _cloud_storage_api,
      _cloud_storage_cache,
      _archival_conf,
      _feature_table,
      _tm_stm_cache_manager,
      _upload_hks,
      _max_concurrent_producer_ids,
      read_replica_bucket);

    _ntp_table.emplace(log.config().ntp(), p);
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

    _manage_watchers.notify(p->ntp(), p);

    co_await p->start();

    co_return c;
}

ss::future<cloud_storage::log_recovery_result>
partition_manager::maybe_download_log(
  storage::ntp_config& ntp_cfg, std::optional<remote_topic_properties> rtp) {
    if (rtp.has_value() && _partition_recovery_mgr.local_is_initialized()) {
        auto res = co_await _partition_recovery_mgr.local().download_log(
          ntp_cfg, rtp->remote_revision, rtp->remote_partition_count);
        co_return res;
    }
    vlog(
      clusterlog.debug,
      "Logs can't be downloaded because cloud storage is not configured. "
      "Continue creating {} witout downloading the logs.",
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
    co_await ss::max_concurrent_for_each(
      partitions, 1024, [this](auto& e) { return do_shutdown(e.second); });

    co_await ssx::async_clear(partitions)();
}

ss::future<>
partition_manager::do_shutdown(ss::lw_shared_ptr<partition> partition) {
    try {
        auto ntp = partition->ntp();
        co_await _raft_manager.local().shutdown(partition->raft());
        _unmanage_watchers.notify(ntp, ntp.tp.partition);
        co_await partition->stop();
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
}

ss::future<>
partition_manager::remove(const model::ntp& ntp, partition_removal_mode mode) {
    auto partition = get(ntp);

    if (!partition) {
        return ss::make_exception_future<>(std::invalid_argument(fmt::format(
          "Can not remove partition. NTP {} is not present in partition "
          "manager",
          ntp)));
    }
    auto group_id = partition->group();

    // remove partition from ntp & raft tables
    _ntp_table.erase(ntp);
    _raft_table.erase(group_id);

    return _raft_manager.local()
      .remove(partition->raft())
      .then([this, ntp] { _unmanage_watchers.notify(ntp, ntp.tp.partition); })
      .then([partition] { return partition->stop(); })
      .then([partition] { return partition->remove_persistent_state(); })
      .then([this, ntp] { return _storage.log_mgr().remove(ntp); })
      .then([this, partition, mode] {
          if (mode == partition_removal_mode::global) {
              return partition->remove_remote_persistent_state(_as);
          } else {
              return ss::now();
          }
      })
      .finally([partition] {}); // in the end remove partition
}

ss::future<> partition_manager::shutdown(const model::ntp& ntp) {
    auto partition = get(ntp);
    if (!partition) {
        return ss::make_exception_future<>(std::invalid_argument(fmt::format(
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
} // namespace cluster
