// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_manager.h"

#include "cluster/logger.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/log_eviction_stm.h"
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
  ss::sharded<cloud_storage::partition_recovery_manager>& recovery_mgr)
  : _storage(storage.local())
  , _raft_manager(raft)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _partition_recovery_mgr(recovery_mgr) {}

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
    return _storage.log_mgr()
      .manage(std::move(ntp_cfg))
      .then([this, group, nodes = std::move(initial_nodes)](
              storage::log&& log) mutable {
          return _raft_manager.local()
            .create_group(group, std::move(nodes), log)
            .then([this, log, group](ss::lw_shared_ptr<raft::consensus> c) {
                auto p = ss::make_lw_shared<partition>(c, _tx_gateway_frontend);
                _ntp_table.emplace(log.config().ntp(), p);
                _raft_table.emplace(group, p);
                _manage_watchers.notify(p->ntp(), p);
                return p->start().then([c] { return c; });
            });
      });
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

ss::future<> partition_manager::stop() {
    return ss::parallel_for_each(
      _ntp_table, [](auto& p) { return p.second->stop(); });
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
    auto group_id = partition->group();

    // remove partition from ntp & raft tables
    _ntp_table.erase(ntp);
    _raft_table.erase(group_id);

    return _raft_manager.local()
      .shutdown(partition->raft())
      .then([partition] { return partition->stop(); })
      .then([this, ntp] { return _storage.log_mgr().shutdown(ntp); })
      .handle_exception([this, ntp, group_id](const std::exception_ptr& e) {
          vassert(
            false,
            "error shutting down partition {{ ntp: {}, raft_group: {}}},  "
            "partition manager state: {}, error: {} - terminating redpanda",
            ntp,
            group_id,
            *this,
            e);
      })
      .finally([partition] {}); // in the end remove partition
}

std::ostream& operator<<(std::ostream& o, const partition_manager& pm) {
    return o << "{shard:" << ss::this_shard_id() << ", mngr:{}"
             << pm._storage.log_mgr()
             << ", ntp_table.size:" << pm._ntp_table.size()
             << ", raft_table.size:" << pm._raft_table.size() << "}";
}
} // namespace cluster
