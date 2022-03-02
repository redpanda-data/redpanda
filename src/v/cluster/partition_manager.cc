// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_manager.h"

#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/rpc_client_protocol.h"
#include "raft/types.h"
#include "resource_mgmt/io_priority.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
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
  ss::sharded<cloud_storage::cache>& cloud_storage_cache)
  : _storage(storage.local())
  , _raft_manager(raft)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _partition_recovery_mgr(recovery_mgr)
  , _cloud_storage_api(cloud_storage_api)
  , _cloud_storage_cache(cloud_storage_cache) {}

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
  std::vector<model::broker> initial_nodes) {
    gate_guard guard(_gate);
    bool logs_recovered = co_await maybe_download_log(ntp_cfg);
    if (logs_recovered) {
        vlog(
          clusterlog.info,
          "Log download complete, ntp: {}, rev: {}",
          ntp_cfg.ntp(),
          ntp_cfg.get_revision());
    }
    storage::log log = co_await _storage.log_mgr().manage(std::move(ntp_cfg));
    vlog(
      clusterlog.info,
      "Log created manage completed, ntp: {}, rev: {}, {} "
      "segments, {} bytes",
      log.config().ntp(),
      log.config().get_revision(),
      log.segment_count(),
      log.size_bytes());

    ss::lw_shared_ptr<raft::consensus> c
      = co_await _raft_manager.local().create_group(
        group, std::move(initial_nodes), log);

    auto p = ss::make_lw_shared<partition>(
      c, _tx_gateway_frontend, _cloud_storage_api, _cloud_storage_cache);

    _ntp_table.emplace(log.config().ntp(), p);
    _raft_table.emplace(group, p);

    /*
     * part of the node leadership draining infrastructure. when a node is in a
     * drianing state new groups might be created since the controller will
     * still be active as a follower. however, if draining is almost complete
     * then new groups may not be noticed. marking as blocked should be done
     * atomically with adding the partition to the ntp_table index above for
     * proper synchronization with the drianing manager.
     */
    if (_block_new_leadership) {
        p->block_new_leadership();
    }

    _manage_watchers.notify(p->ntp(), p);

    co_await p->start();
    co_return c;
}

ss::future<bool>
partition_manager::maybe_download_log(storage::ntp_config& ntp_cfg) {
    if (_partition_recovery_mgr.local_is_initialized()) {
        co_return co_await _partition_recovery_mgr.local().download_log(
          ntp_cfg);
    }
    vlog(
      clusterlog.debug,
      "Logs can't be downloaded because cloud storage is not configured. "
      "Continue creating {} witout downloading the logs.",
      ntp_cfg);
    co_return false;
}

ss::future<> partition_manager::stop_partitions() {
    co_await _gate.close();
    // prevent partitions from being accessed
    auto partitions = std::exchange(_ntp_table, {});
    _raft_table.clear();
    // shutdown all partitions
    co_await ss::parallel_for_each(
      partitions, [this](auto& e) { return do_shutdown(e.second); });
}

ss::future<>
partition_manager::do_shutdown(ss::lw_shared_ptr<partition> partition) {
    try {
        auto ntp = partition->ntp();
        co_await _raft_manager.local().shutdown(partition->raft());
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

ss::future<> partition_manager::remove(const model::ntp& ntp) {
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
      .finally([partition] {}); // in the end remove partition
}

ss::future<> partition_manager::shutdown(const model::ntp& ntp) {
    auto partition = get(ntp);

    if (!partition) {
        return ss::make_exception_future<>(std::invalid_argument(fmt::format(
          "Can not shutdown partition. NTP {} is not present in partition "
          "manager",
          ntp)));
    }

    // remove partition from ntp & raft tables
    _ntp_table.erase(ntp);
    _raft_table.erase(partition->group());

    return do_shutdown(partition);
}

std::ostream& operator<<(std::ostream& o, const partition_manager& pm) {
    return o << "{shard:" << ss::this_shard_id() << ", mngr:{}"
             << pm._storage.log_mgr()
             << ", ntp_table.size:" << pm._ntp_table.size()
             << ", raft_table.size:" << pm._raft_table.size() << "}";
}
} // namespace cluster
