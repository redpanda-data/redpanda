#include "cluster/partition_manager.h"

#include "cluster/logger.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/rpc_client_protocol.h"
#include "raft/types.h"
#include "resource_mgmt/io_priority.h"
#include "vlog.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

namespace cluster {

storage::log_config manager_config_from_global_config() {
    return storage::log_config(
      storage::log_config::storage_type::disk,
      config::shard_local_cfg().data_directory().as_sstring(),
      config::shard_local_cfg().log_segment_size(),
      storage::log_config::debug_sanitize_files::no,
      config::shard_local_cfg().retention_bytes(),
      config::shard_local_cfg().log_compaction_interval(),
      config::shard_local_cfg().delete_retention_ms(),
      storage::log_config::with_cache(
        config::shard_local_cfg().disable_batch_cache()),
      storage::batch_cache::reclaim_options{
        .growth_window = config::shard_local_cfg().reclaim_growth_window(),
        .stable_window = config::shard_local_cfg().reclaim_stable_window(),
        .min_size = config::shard_local_cfg().reclaim_min_size(),
        .max_size = config::shard_local_cfg().reclaim_max_size(),
      });
}

partition_manager::partition_manager(
  model::timeout_clock::duration disk_timeout,
  ss::sharded<rpc::connection_cache>& clients)
  : _self(config::shard_local_cfg().node_id())
  , _disk_timeout(disk_timeout)
  , _mngr(manager_config_from_global_config())
  , _client(raft::make_rpc_client_protocol(clients))
  , _hbeats(config::shard_local_cfg().raft_heartbeat_interval(), _client) {}

ss::future<> partition_manager::start() { return _hbeats.start(); }

ss::future<> partition_manager::stop() {
    using pair_t = typename decltype(_raft_table)::value_type;
    return _bg.close()
      .then([this] { return _hbeats.stop(); })
      .then([this] {
          return parallel_for_each(_raft_table, [](pair_t& pair) {
              clusterlog.info("Shutting down raft group: {}", pair.first);
              return pair.second->stop();
          });
      })
      .then([this] { return _mngr.stop(); });
}

void partition_manager::trigger_leadership_notification(
  raft::leadership_status st) {
    auto ptr = _raft_table.find(st.group)->second;
    for (auto& cb : _notifications) {
        cb.second(ptr, st.term, st.current_leader);
    }
}

ss::future<consensus_ptr> partition_manager::manage(
  storage::ntp_config ntp_cfg,
  raft::group_id group,
  std::vector<model::broker> initial_nodes,
  std::optional<raft::consensus::append_entries_cb_t> ap_entries_cb) {
    return _mngr.manage(std::move(ntp_cfg))
      .then(
        [this,
         group,
         nodes = std::move(initial_nodes),
         ap_entries_cb = std::move(ap_entries_cb)](storage::log&& log) mutable {
            auto c = make_consensus(
              group, std::move(nodes), log, std::move(ap_entries_cb));
            auto p = ss::make_lw_shared<partition>(c);
            _ntp_table.emplace(log.config().ntp, p);
            _raft_table.emplace(group, p);
            _manage_watchers.notify(p->ntp(), p);
            if (_bg.is_closed()) {
                return ss::make_exception_future<consensus_ptr>(
                  ss::gate_closed_exception());
            }
            return with_gate(_bg, [this, p, c, group] {
                vlog(clusterlog.debug, "Recovering raft group: {}", group);
                return p->start().then([this, c]() mutable {
                    return _hbeats.register_group(c).then([c] { return c; });
                });
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

    // stop partiton raft & remove log
    return partition->stop()
      .then([this, group_id] { return _hbeats.deregister_group(group_id); })
      .then([this, ntp] { return _mngr.remove(ntp); })
      .finally([partition] {}); // in the end remove partition
}

ss::lw_shared_ptr<raft::consensus> partition_manager::make_consensus(
  raft::group_id gr,
  std::vector<model::broker> nodes,
  storage::log log,
  std::optional<raft::consensus::append_entries_cb_t> append_entries_cb) {
    return ss::make_lw_shared<raft::consensus>(
      _self,
      gr,
      raft::group_configuration{.nodes = std::move(nodes)},
      raft::timeout_jitter(
        config::shard_local_cfg().raft_election_timeout_ms()),
      log,
      raft_priority(),
      _disk_timeout,
      _client,
      [this](raft::leadership_status st) {
          trigger_leadership_notification(std::move(st));
      },
      std::move(append_entries_cb));
}

std::ostream& operator<<(std::ostream& o, const partition_manager& pm) {
    return o << "{shard:" << ss::this_shard_id() << ", mngr:{}" << pm._mngr
             << ", notification_sequence:" << pm._notification_id
             << ", notifications.size:" << pm._notifications.size()
             << ", ntp_table.size:" << pm._ntp_table.size()
             << ", raft_table.size:" << pm._raft_table.size() << "}";
}
} // namespace cluster
