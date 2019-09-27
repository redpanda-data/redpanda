#include "cluster/controller.h"

namespace cluster {
static void verify_shard() {
    if (__builtin_expect(engine().cpu_id() != controller::shard, false)) {
        throw std::runtime_error(fmt::format(
          "Attempted to access controller on core: {}", engine().cpu_id()));
    }
}

inline model::ntp cluster_ntp() {
    return model::ntp{model::ns("redpanda"),
                      model::topic_partition{model::topic("controller"),
                                             model::partition_id(0)}};
}

controller::controller(
  model::node_id n,
  sstring basedir,
  size_t max_segment_size,
  sharded<partition_manager>& pm,
  sharded<shard_table>& st)
  : _self(std::move(n))
  , _mngr(storage::log_config{
      std::move(basedir),
      max_segment_size,
      /*this is for debug only*/ storage::log_config::sanitize_files::no})
  , _pm(pm)
  , _st(st)
  , _stgh(this) {
}

future<> controller::start() {
    verify_shard();
    return _mngr.manage(cluster_ntp()).then([this](storage::log_ptr plog) {});
}
future<> controller::stop() {
    verify_shard();
    return make_ready_future<>();
}

controller::stage_hook::stage_hook(controller* self)
  : ptr(self) {
}
void controller::stage_hook::pre_commit(
  model::offset, const std::vector<std::unique_ptr<raft::entry>>&) {
    verify_shard();
}
void controller::stage_hook::abort(model::offset begin) {
    verify_shard();
}
void controller::stage_hook::commit(
  model::offset begin, model::offset committed) {
    verify_shard();
}

} // namespace cluster
