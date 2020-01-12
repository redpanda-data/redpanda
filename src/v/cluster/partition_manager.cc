#include "cluster/partition_manager.h"

#include "cluster/logger.h"
#include "config/configuration.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/reactor.hh>

namespace cluster {

partition_manager::partition_manager(
  storage::log_append_config::fsync should_fsync,
  model::timeout_clock::duration disk_timeout,
  ss::sharded<cluster::shard_table>& nlc,
  ss::sharded<rpc::connection_cache>& clients)
  : _self(config::shard_local_cfg().node_id())
  , _should_fsync(should_fsync)
  , _disk_timeout(disk_timeout)
  , _mngr(storage::log_config{
      .base_dir = config::shard_local_cfg().data_directory().as_sstring(),
      .max_segment_size = config::shard_local_cfg().log_segment_size(),
      .should_sanitize = storage::log_config::sanitize_files::no})
  , _hbeats(config::shard_local_cfg().raft_timeout(), clients)
  , _shard_table(nlc)
  , _clients(clients) {}

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

void partition_manager::trigger_leadership_notification(raft::group_id group) {
    auto ptr = _raft_table.find(group)->second;
    for (auto& cb : _notifications) {
        cb(ptr);
    }
}

ss::future<consensus_ptr> partition_manager::manage(
  model::ntp ntp,
  raft::group_id group,
  std::vector<model::broker> initial_nodes) {
    return _mngr.manage(std::move(ntp))
      .then(
        [this, group, nodes = std::move(initial_nodes)](storage::log log) {
            auto c = ss::make_lw_shared<raft::consensus>(
              _self,
              group,
              raft::group_configuration{.nodes = std::move(nodes)},
              raft::timeout_jitter(_hbeats.election_duration()),
              log,
              _should_fsync,
              raft_priority(),
              _disk_timeout,
              _clients,
              [this](raft::group_id g) { trigger_leadership_notification(g); });
            auto p = ss::make_lw_shared<partition>(c);
            _ntp_table.emplace(log.ntp(), p);
            _raft_table.emplace(group, p);
            if (_bg.is_closed()) {
                return ss::make_exception_future<consensus_ptr>(
                  ss::gate_closed_exception());
            }
            return with_gate(_bg, [this, p, c, group] {
                clusterlog.debug("Recovering raft group: {}", group);
                return p->start().then([this, c]() mutable {
                    _hbeats.register_group(c);
                    return c;
                });
            });
        });
}

} // namespace cluster
