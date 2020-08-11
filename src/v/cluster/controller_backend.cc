
#include "cluster/controller_backend.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "raft/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

/// Class that contains the controller state, for now we will have single
/// controller backend

/// on every core, sharded
namespace cluster {

controller_backend::controller_backend(
  ss::sharded<topic_table>& tp_state,
  ss::sharded<shard_table>& st,
  ss::sharded<partition_manager>& pm,
  ss::sharded<members_table>& members,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<ss::abort_source>& as)
  : _topics(tp_state)
  , _shard_table(st)
  , _partition_manager(pm)
  , _members_table(members)
  , _partition_leaders_table(leaders)
  , _self(model::node_id(config::shard_local_cfg().node_id))
  , _data_directory(config::shard_local_cfg().data_directory().as_sstring())
  , _as(as) {}

ss::future<> controller_backend::stop() {
    _housekeeping_timer.cancel();
    return _gate.close();
}

ss::future<> controller_backend::start() {
    start_topics_reconciliation_loop();
    _housekeeping_timer.set_callback([this] { housekeeping(); });
    _housekeeping_timer.arm_periodic(std::chrono::seconds(1));
    return ss::now();
}

void controller_backend::start_topics_reconciliation_loop() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] {
              return _topics.local()
                .wait_for_changes(_as.local())
                .then([this](std::vector<topic_table::delta> deltas) {
                    return ss::with_semaphore(
                      _topics_sem,
                      1,
                      [this, deltas = std::move(deltas)]() mutable {
                          for (auto& d : deltas) {
                              _topic_deltas.emplace_back(std::move(d));
                          }
                          return reconcile_topics();
                      });
                })
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      clusterlog.error,
                      "Error while reconciling topics - {}",
                      e);
                });
          });
    });
}

void controller_backend::housekeeping() {
    if (!_topic_deltas.empty() && _topics_sem.available_units() > 0) {
        (void)ss::with_gate(_gate, [this] {
            return ss::with_semaphore(
              _topics_sem, 1, [this] { return reconcile_topics(); });
        });
    }
}

// caller must hold _topics_sem lock
ss::future<> controller_backend::reconcile_topics() {
    using meta_t = task_meta<topic_table::delta>;
    // _topic_deltas are chronologically ordered, we cannot reorder applying
    // them, hence we have to use `ss::do_for_each`
    return ss::do_for_each(
             std::begin(_topic_deltas),
             std::end(_topic_deltas),
             [this](meta_t& task) {
                 return do_reconcile_topic(task).then(
                   [&task]() { task.finished = true; });
             })
      .then([this] {
          // remove finished tasks
          auto it = std::stable_partition(
            std::begin(_topic_deltas),
            std::end(_topic_deltas),
            [](meta_t& task) { return task.finished; });
          _topic_deltas.erase(_topic_deltas.begin(), it);
      });
}

bool has_local_replicas(
  model::node_id self, const std::vector<model::broker_shard>& replicas) {
    return std::find_if(
             std::cbegin(replicas),
             std::cend(replicas),
             [self](const model::broker_shard& bs) {
                 return bs.node_id == self && bs.shard == ss::this_shard_id();
             })
           != replicas.cend();
}

std::vector<model::broker> create_brokers_set(
  const std::vector<model::broker_shard>& replicas,
  cluster::members_table& members) {
    std::vector<model::broker> brokers;
    brokers.reserve(replicas.size());
    std::transform(
      std::cbegin(replicas),
      std::cend(replicas),
      std::back_inserter(brokers),
      [&members](const model::broker_shard& bs) {
          auto br = members.get_broker(bs.node_id);
          if (!br) {
              throw std::logic_error(
                fmt::format("Replica node {} is not available", bs.node_id));
          }
          return *br->get();
      });
    return brokers;
}

ss::future<>
controller_backend::do_reconcile_topic(task_meta<topic_table::delta>& task) {
    // new partitions
    auto f = ss::parallel_for_each(
      task.delta.partitions.additions.begin(),
      task.delta.partitions.additions.end(),
      [this](topic_table::delta::partition& p) {
          // only create partitions for this backend
          // partitions created on current shard at this node
          if (!has_local_replicas(_self, p.second.replicas)) {
              return ss::now();
          }
          model::ntp ntp(p.first.ns, p.first.tp, p.second.id);
          return create_partition(
            ntp,
            p.second.group,
            create_brokers_set(p.second.replicas, _members_table.local()));
      });

    // delete partitions
    f = f.then([this, &task] {
        return ss::parallel_for_each(
          std::cbegin(task.delta.partitions.deletions),
          std::cend(task.delta.partitions.deletions),
          [this](const topic_table::delta::partition& p) {
              return delete_partition(
                model::ntp(p.first.ns, p.first.tp, p.second.id));
          });
    });

    return f.then([&task] { task.finished = true; });
}

ss::future<> controller_backend::add_to_shard_table(
  model::ntp ntp, raft::group_id raft_group, uint32_t shard) {
    // update shard_table: broadcast
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, shard](shard_table& s) mutable {
          s.insert(std::move(ntp), shard);
          s.insert(raft_group, shard);
      });
}

ss::future<> controller_backend::create_partition(
  model::ntp ntp, raft::group_id group_id, std::vector<model::broker> members) {
    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));

    if (!cfg) {
        // partition was already removed, do nothing
        return ss::now();
    }

    auto f = ss::now();
    // handle partially created topic
    if (likely(_partition_manager.local().get(ntp).get() == nullptr)) {
        f = _partition_manager.local()
              .manage(
                cfg->make_ntp_config(_data_directory, ntp.tp.partition),
                group_id,
                std::move(members))
              .discard_result();
    }

    return f.then([this, ntp = std::move(ntp), group_id]() mutable {
        // we create only partitions that belongs to current shard
        return add_to_shard_table(
          std::move(ntp), group_id, ss::this_shard_id());
    });
}

ss::future<> controller_backend::delete_partition(model::ntp ntp) {
    auto part = _partition_manager.local().get(ntp);
    if (unlikely(part.get() == nullptr)) {
        return ss::now();
    }
    auto group_id = part->group();

    return _shard_table
      .invoke_on_all(
        [ntp, group_id](shard_table& st) mutable { st.erase(ntp, group_id); })
      .then([this, ntp] {
          return _partition_leaders_table.invoke_on_all(
            [ntp](partition_leaders_table& leaders) {
                leaders.remove_leader(ntp);
            });
      })
      .then([this, ntp = std::move(ntp)] {
          // remove partition
          return _partition_manager.local().remove(ntp);
      });
}

} // namespace cluster
