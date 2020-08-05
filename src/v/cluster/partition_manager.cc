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

#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

namespace cluster {

partition_manager::partition_manager(
  ss::sharded<storage::api>& storage, ss::sharded<raft::group_manager>& raft)
  : _storage(storage.local())
  , _raft_manager(raft) {}

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
                if (log.config().is_collectable()) {
                    auto p = ss::make_lw_shared<partition>(
                      c, raft::log_eviction_stm(c.get(), clusterlog));
                }
                auto p = ss::make_lw_shared<partition>(c);
                _ntp_table.emplace(log.config().ntp(), p);
                _raft_table.emplace(group, p);
                _manage_watchers.notify(p->ntp(), p);
                return p->start().then([c] { return c; });
            });
      });
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
      .stop_group(partition->raft())
      .then([this, ntp] { return _storage.log_mgr().remove(ntp); })
      .finally([partition] {}); // in the end remove partition
}

std::ostream& operator<<(std::ostream& o, const partition_manager& pm) {
    return o << "{shard:" << ss::this_shard_id() << ", mngr:{}"
             << pm._storage.log_mgr()
             << ", ntp_table.size:" << pm._ntp_table.size()
             << ", raft_table.size:" << pm._raft_table.size() << "}";
}
} // namespace cluster
