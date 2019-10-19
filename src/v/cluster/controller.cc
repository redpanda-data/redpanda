#include "cluster/controller.h"

#include "cluster/logger.h"
#include "resource_mgmt/io_priority.h"
#include "utils/memory_data_source.h"

namespace cluster {
static void verify_shard() {
    if (__builtin_expect(engine().cpu_id() != controller::shard, false)) {
        throw std::runtime_error(fmt::format(
          "Attempted to access controller on core: {}", engine().cpu_id()));
    }
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
    clusterlog().debug("Starting cluster recovery");
    return _mngr.manage(cluster_ntp()).then([this](storage::log_ptr plog) {
        return bootstrap_from_log(plog);
    });
}
future<> controller::stop() {
    verify_shard();
    return _mngr.stop();
}

future<> controller::bootstrap_from_log(storage::log_ptr l) {
    storage::log_reader_config rcfg{
      .start_offset = model::offset(0), // from begining
      .max_bytes = std::numeric_limits<size_t>::max(),
      .min_bytes = 0, // ok to be empty
      .prio = controller_priority()};
    return do_with(
      l->make_reader(rcfg), [this](model::record_batch_reader& reader) {
          return reader.consume(batch_consumer(this), model::no_timeout);
      });
}

future<> controller::recover_batch(model::record_batch batch) {
    if (batch.type() != controller::controller_record_batch_type) {
        return make_ready_future<>();
    }
    // XXX https://github.com/vectorizedio/v/issues/188
    // we only support decompressed records
    if (batch.compressed()) {
        throw std::runtime_error(
          "We cannot process compressed record_batch'es yet, see #188");
    }
    return do_with(std::move(batch), [this](model::record_batch& batch) {
        return do_for_each(batch, [this](model::record& rec) {
            return recover_record(std::move(rec));
        });
    });
}

future<> controller::recover_record(model::record r) {
    auto in = input_stream<char>(
      data_source(std::make_unique<memory_data_source>(
        std::move(r).release_packed_value_and_headers().release())));
    auto src = rpc::source(in);
    return do_with(
             std::move(in),
             std::move(src),
             [](input_stream<char>& in, rpc::source& src) {
                 return rpc::deserialize<partition_assignment>(src);
             })
      .then([this](partition_assignment as) {
          return recover_assignment(std::move(as));
      });
}

future<> controller::recover_assignment(partition_assignment as) {
    if (as.broker.id() != _self) {
        return make_ready_future<>();
    }
    // the following ops have a dependency on the shard_table *then*
    // partition_manager order

    // 1. update shard_table: broadcast
    return _st
      .invoke_on_all([shard = as.shard, raft_group = as.group, ntp = as.ntp](
                       shard_table& s) {
          s.insert(ntp, shard);
          s.insert(raft_group, shard);
      })
      .then([this, shard = as.shard, raft_group = as.group, ntp = as.ntp] {
          // 2. update partition_manager
          return _pm.invoke_on(
            shard, [this, raft_group, ntp](partition_manager& pm) {
                return pm.manage(ntp, raft_group);
            });
      });
}

void controller::end_of_stream() {
    clusterlog().info("Finished recovering cluster state");
}

future<std::vector<topic_result>> controller::create_topics(
  model::ns ns,
  std::vector<topic_configuration> topics,
  model::timeout_clock::time_point timeout) {
    verify_shard();
    // FIXME: Replace this stub...
    return make_ready_future<std::vector<topic_result>>();
}

// ---- hooks below

controller::stage_hook::stage_hook(controller* self)
  : ptr(self) {
}
void controller::stage_hook::pre_commit(
  model::offset, const std::vector<raft::entry>&) {
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
