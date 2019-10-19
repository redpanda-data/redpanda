#include "cluster/partition_manager.h"

#include "resource_mgmt/io_priority.h"

namespace cluster {

partition_manager::partition_manager(
  model::node_id::type nid,
  std::chrono::milliseconds raft_timeout,
  sstring base_dir,
  size_t max_segment_size,
  storage::log_append_config::fsync should_fsync,
  model::timeout_clock::duration disk_timeout,
  sharded<cluster::shard_table>& nlc,
  sharded<raft::client_cache>& clients)
  : _self(std::move(nid))
  , _should_fsync(should_fsync)
  , _disk_timeout(disk_timeout)
  , _mngr(storage::log_config{
      .base_dir = base_dir,
      .max_segment_size = max_segment_size,
      .should_sanitize = storage::log_config::sanitize_files::no})
  , _hbeats(raft_timeout, clients)
  , _shard_table(nlc)
  , _clients(clients) {
}

future<> partition_manager::stop() {
    return _mngr.stop();
}
void partition_manager::trigger_leadership_notification(raft::group_id group) {
    auto ptr = _raft_table.find(group)->second;
    for (auto& cb : _notifications) {
        cb(ptr);
    }
}

future<> partition_manager::manage(model::ntp ntp, raft::group_id group) {
    return _mngr.manage(std::move(ntp))
      .then([this, group](storage::log_ptr log) {
          auto c = make_lw_shared<raft::consensus>(
            _self,
            raft::timeout_jitter(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                _hbeats.base_duration())
                .count()),
            *log,
            _should_fsync,
            raft_priority(),
            _disk_timeout,
            _clients);
          auto p = make_lw_shared<partition>(c);
          _ntp_table.emplace(log->ntp(), p);
          _raft_table.emplace(group, p);
          return p->start().then([this, c] { _hbeats.register_group(c); });
      });
}

} // namespace cluster
