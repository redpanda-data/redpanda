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

partition_manager::partition_manager(
  model::timeout_clock::duration disk_timeout,
  ss::sharded<cluster::shard_table>& nlc,
  ss::sharded<rpc::connection_cache>& clients)
  : _self(config::shard_local_cfg().node_id())
  , _disk_timeout(disk_timeout)
  , _mngr(storage::log_config{
      .base_dir = config::shard_local_cfg().data_directory().as_sstring(),
      .max_segment_size = config::shard_local_cfg().log_segment_size(),
      .should_sanitize = storage::log_config::sanitize_files::no,
      .retention_bytes = config::shard_local_cfg().retention_bytes(),
      .compaction_interval
      = config::shard_local_cfg().log_compaction_interval(),
      .delete_retention = config::shard_local_cfg().delete_retention_ms(),
      .disable_cache = storage::log_config::disable_batch_cache(
        config::shard_local_cfg().disable_batch_cache()),
    })
  , _client(raft::make_rpc_client_protocol(clients))
  , _hbeats(config::shard_local_cfg().raft_heartbeat_interval(), _client)
  , _shard_table(nlc) {}

ss::future<> partition_manager::start() { return _hbeats.start(); }

ss::future<> partition_manager::stop() {
    using pair_t = typename decltype(_raft_table)::value_type;
    return _bg.close()
      .then([this] { return _hbeats.stop(); })
      .then([this] {
          return parallel_for_each(_raft_table, [this](pair_t& pair) {
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
  model::ntp ntp,
  raft::group_id group,
  std::vector<model::broker> initial_nodes,
  std::optional<raft::consensus::append_entries_cb_t> ap_entries_cb) {
    return _mngr.manage(std::move(ntp))
      .then(
        [this,
         group,
         nodes = std::move(initial_nodes),
         ap_entries_cb = std::move(ap_entries_cb)](storage::log log) mutable {
            auto c = make_consensus(
              group, std::move(nodes), log, std::move(ap_entries_cb));
            auto p = ss::make_lw_shared<partition>(c);
            _ntp_table.emplace(log.ntp(), p);
            _raft_table.emplace(group, p);
            _manage_watchers.notify(p->ntp(), p);
            if (_bg.is_closed()) {
                return ss::make_exception_future<consensus_ptr>(
                  ss::gate_closed_exception());
            }
            return with_gate(_bg, [this, p, c, group] {
                vlog(clusterlog.debug, "Recovering raft group: {}", group);
                return p->start().then([this, c]() mutable {
                    _hbeats.register_group(c);
                    return c;
                });
            });
        });
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
