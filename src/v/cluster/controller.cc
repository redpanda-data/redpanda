#include "cluster/controller.h"

#include "cluster/cluster_utils.h"
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/namespace.h"
#include "cluster/notification_latch.h"
#include "cluster/partition_manager.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "resource_mgmt/io_priority.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "storage/shard_assignment.h"
#include "utils/retry.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/net/inet_address.hh>

namespace cluster {
static void verify_shard() {
    if (unlikely(ss::this_shard_id() != controller::shard)) {
        throw std::runtime_error(fmt::format(
          "Attempted to access controller on core: {}", ss::this_shard_id()));
    }
}

controller::controller(
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& st,
  ss::sharded<metadata_cache>& md_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<metadata_dissemination_service>& md_dissemination)
  : _self(config::make_self_broker(config::shard_local_cfg()))
  , _seed_servers(config::shard_local_cfg().seed_servers())
  , _data_directory(config::shard_local_cfg().data_directory().as_sstring())
  , _pm(pm)
  , _st(st)
  , _md_cache(md_cache)
  , _connection_cache(connection_cache)
  , _md_dissemination_service(md_dissemination)
  , _raft0(nullptr)
  , _highest_group_id(0) {}

model::record_batch create_checkpoint_batch() {
    const static log_record_key checkpoint_key{
      .record_type = log_record_key::type::checkpoint};

    storage::record_batch_builder builder(
      controller_record_batch_type, model::offset(0));
    iobuf key_buf;
    reflection::adl<log_record_key>{}.to(key_buf, checkpoint_key);
    builder.add_raw_kv(std::move(key_buf), iobuf{});
    return std::move(builder).build();
}

template<typename Func>
auto controller::dispatch_rpc_to_leader(Func&& f) {
    using inner_t = typename std::result_of_t<Func(controller_client_protocol)>;
    using fut_t = ss::futurize<inner_t>;
    using ret_t = result<
      typename std::tuple_element<0, typename fut_t::value_type>::type>;

    std::optional<model::node_id> leader_id = get_leader_id();
    if (!leader_id) {
        return ss::make_ready_future<ret_t>(errc::no_leader_controller);
    }

    auto leader = _raft0->config().find_in_nodes(*leader_id);

    if (leader == _raft0->config().nodes.end()) {
        return ss::make_ready_future<ret_t>(errc::no_leader_controller);
    }

    return with_client<controller_client_protocol, Func>(
      _connection_cache,
      *leader_id,
      leader->rpc_address(),
      std::forward<Func>(f));
}

/**
 * The controller is a sharded service. However, all of the primary state
 * and computation occurs exclusively on the single controller::shard core.
 * The service is sharded in order to safely coordinate communication
 * between cores. Unless otherwise specificed, access to any controller::*
 * is only valid on the controller::shard core.
 *
 * Shutdown
 * ========
 *
 * On controller shutdown the controller::_bg gate on each core is closed.
 *
 * Leadership notification
 * =======================
 *
 * Leadership notifications occur on the node where a raft group is
 * assigned, and in general this is not the controller::shard core. In order
 * control sequencing of operations on controller state, each leadership
 * notification is forwarded to the controller::shard core for handling.
 *
 * Locking for leadership notifications is:
 *
 *          core-0                    core-i
 *             │                         │
 *             │                         ▼
 *             │                 leadership notify
 *             │                         │
 *             │                         ▼
 *             │                 with_gate(core-i)
 *             ▼                         │
 *     with_gate(core-0)  ◀──────────────┘
 *             │
 *             ▼
 *     with_sem(core-0)
 *             │
 *              ─ ─ ─ ─ ─ ─ ─ ─ ─▶ metadata update
 *               invoke_on_all
 *
 * It's important to grab the gate on both the source and destination cores
 * of the x-core forwarding of the notification. Acquiring the gate on the
 * source core keeps the service from being fully shutdown and destroyed.
 * The continuation run on the destination core may encounter a closed gate
 * when racing with shutdown, but successfully acquiring the gate will
 * guarantee that the service instance on that core won't be fully shutdown
 * while the continuation is running.
 */
ss::future<> controller::start() {
    if (ss::this_shard_id() != controller::shard) {
        return ss::make_ready_future<>();
    }
    return _pm
      .invoke_on_all([this](partition_manager& pm) {
          _leader_notify_handle = pm.register_leadership_notification(
            [this](
              ss::lw_shared_ptr<partition> p,
              model::term_id term,
              std::optional<model::node_id> leader_id) {
                handle_leadership_notification(
                  p->ntp(), term, std::move(leader_id));
            });
      })
      .then([this] {
          return _pm.invoke_on_all([](partition_manager& pm) {
              // start the partition managers first...
              return pm.start();
          });
      })
      .then([this] {
          // add raft0 to shard table
          return assign_group_to_shard(
            controller_ntp, controller::group, controller::shard);
      })
      .then([this] {
          // add current node to brokers cache
          return update_brokers_cache({_self});
      })
      .then([this] {
          vlog(clusterlog.debug, "Starting cluster recovery");
          return start_raft0()
            .then([this](consensus_ptr c) { _raft0 = c.get(); })
            .then([this] {
                _raft0_cfg_offset = model::offset(
                  _raft0->meta().prev_log_index);
                return _state_lock.with(
                  [this] { return apply_raft0_cfg_update(_raft0->config()); });
            })
            .then([this] {
                if (!is_already_member()) {
                    return join_raft0();
                };
            });
      })
      .then([this] {
          // done in background fibre
          _md_dissemination_service.local().initialize_leadership_metadata();
      });
}

ss::future<consensus_ptr> controller::start_raft0() {
    std::vector<model::broker> brokers;
    // When seed server property is empty it means that current node is cluster
    // root, otherwise it will use one of the seed servers to join the cluster
    if (_seed_servers.empty()) {
        vlog(clusterlog.info, "Current node is cluster root");
        brokers.push_back(_self);
    }
    return _pm.local().manage(
      make_raft0_ntp_config(),
      controller::group,
      std::move(brokers),
      [this](model::record_batch_reader&& reader) {
          verify_shard();
          on_raft0_entries_comitted(std::move(reader));
      });
}

ss::future<> controller::stop() {
    _pm.local().unregister_leadership_notification(_leader_notify_handle);
    if (ss::this_shard_id() == controller::shard && _raft0) {
        _raft0->remove_append_entries_callback();
    }
    if (ss::this_shard_id() == controller::shard) {
        // in a multi-threaded app this would look like a deadlock waiting
        // to happen, but it works in seastar: broadcast here only makes the
        // waiters runnable. they won't actually have a chance to run until
        // after the closed flag within the gate is set, ensuring that when
        // they wake up they'll leave the gate after observing that it is
        // closed.
        _leadership_cond.broadcast();
    }
    _as.request_abort();
    return _state_lock.with([this] {
        // we have to stop latch first as there may be some futures waiting
        // inside the gate for notification
        _notification_latch.stop();
        return _bg.close();
    });
}

ss::future<> controller::process_raft0_batch(model::record_batch batch) {
    if (unlikely(batch.header().type == raft::data_batch_type)) {
        // we are not intrested in data batches
        return ss::make_ready_future<>();
    }
    // XXX https://github.com/vectorizedio/v/issues/188
    // we only support decompressed records
    if (batch.compressed()) {
        return ss::make_exception_future<>(std::runtime_error(
          "We cannot process compressed record_batch'es yet, see #188"));
    }
    auto last_offset = batch.last_offset();
    auto f = ss::make_ready_future<>();
    if (batch.header().type == raft::configuration_batch_type) {
        auto base_offset = batch.header().base_offset;
        f = model::consume_records(
          std::move(batch), [this, base_offset](model::record rec) mutable {
              auto rec_offset = model::offset(
                base_offset() + rec.offset_delta());
              return process_raft0_cfg_update(std::move(rec), rec_offset);
          });
    } else {
        auto records_to_recover = batch.record_count();
        f = model::consume_records(
              std::move(batch),
              [this](model::record rec) mutable {
                  return recover_record(std::move(rec));
              })
              .then([this, records_to_recover] {
                  return _recovery_semaphore.wait(records_to_recover);
              });
    }

    return f.then(
      [this, last_offset] { return _notification_latch.notify(last_offset); });
}

ss::future<>
controller::apply_raft0_cfg_update(const raft::group_configuration& cfg) {
    auto old_list = _md_cache.local().all_brokers();

    std::vector<broker_ptr> new_list;
    cfg.for_each([&new_list](const model::broker& br) {
        new_list.push_back(ss::make_lw_shared<model::broker>(br));
    });

    if (is_leader()) {
        update_partition_allocator(new_list);
    }

    return update_clients_cache(new_list, std::move(old_list))
      .then([this, nodes = cfg.nodes]() mutable {
          return update_brokers_cache(std::move(nodes));
      });
}

ss::future<>
controller::process_raft0_cfg_update(model::record r, model::offset o) {
    if (o <= _raft0_cfg_offset) {
        return seastar::make_ready_future<>();
    }
    vlog(clusterlog.debug, "Processing new raft-0 configuration");
    _raft0_cfg_offset = o;
    auto cfg = reflection::adl<raft::group_configuration>().from(
      r.value().copy());
    return apply_raft0_cfg_update(std::move(cfg));
}

ss::future<> controller::recover_record(model::record r) {
    auto log_record = reflection::adl<log_record_key>{}.from(r.key().copy());
    return dispatch_record_recovery(std::move(log_record), r.value().copy());
}

ss::future<>
controller::dispatch_record_recovery(log_record_key key, iobuf&& v_buf) {
    switch (key.record_type) {
    case log_record_key::type::partition_assignment:
        recover_assignment_in_background(
          reflection::from_iobuf<partition_assignment>(std::move(v_buf)));
        return ss::make_ready_future<>();
    case log_record_key::type::topic_configuration:
        return recover_topic_configuration(
                 reflection::from_iobuf<topic_configuration>(std::move(v_buf)))
          .then([this] { _recovery_semaphore.signal(); });
    case log_record_key::type::topic_deletion:
        return process_topic_deletion(
                 reflection::from_iobuf<model::topic_namespace>(
                   std::move(v_buf)))
          .then([this] { _recovery_semaphore.signal(); });
    case log_record_key::type::checkpoint:
        return ss::make_ready_future<>().then(
          [this] { _recovery_semaphore.signal(); });
    default:
        _recovery_semaphore.signal();
        return ss::make_exception_future<>(
          std::runtime_error("Not supported record type in controller batch"));
    }
}

ss::future<> controller::recover_assignment(partition_assignment as) {
    _highest_group_id = std::max(_highest_group_id, as.group);
    return ss::do_with(std::move(as), [this](partition_assignment& as) {
        return update_cache_with_partitions_assignment(as).then([this, &as] {
            auto it = std::find_if(
              std::cbegin(as.replicas),
              std::cend(as.replicas),
              [this](const model::broker_shard& bs) {
                  return bs.node_id == _self.id();
              });

            if (it == std::cend(as.replicas)) {
                // This partition in not replicated on current broker
                return ss::make_ready_future<>();
            }
            // Recover replica as it is replicated on current broker
            return recover_replica(as.ntp, as.group, it->shard, as.replicas);
        });
    });
}

void controller::recover_assignment_in_background(partition_assignment as) {
    // We can recover partitition assignments in background as the state updates
    // comming from single assignment are independent so we can parallelize
    // updates
    _highest_group_id = std::max(_highest_group_id, as.group);
    (void)ss::with_gate(_bg, [this, as = std::move(as)]() mutable {
        return recover_assignment(std::move(as)).then([this] {
            _recovery_semaphore.signal();
        });
    });
}

ss::future<> controller::recover_replica(
  model::ntp ntp,
  raft::group_id raft_group,
  uint32_t shard,
  std::vector<model::broker_shard> replicas) {
    // the following ops have a dependency on the shard_table
    // *then* partition_manager order

    // FIXME: Pass topic configuration to partitions manager

    // (compression, compation, etc)

    return assign_group_to_shard(ntp, raft_group, shard)
      .then([this, shard, raft_group, ntp, replicas = std::move(replicas)] {
          // 2. update partition_manager
          return dispatch_manage_partition(
            std::move(ntp), raft_group, shard, std::move(replicas));
      });
}

ss::future<> controller::dispatch_manage_partition(
  model::ntp ntp,
  raft::group_id raft_group,
  uint32_t shard,
  std::vector<model::broker_shard> replicas) {
    vassert(
      shard <= ss::smp::count,
      "Inconsistency between restarts detected. Cannot start machine with "
      "lower core count than a previous run. Core count can only increase. "
      "Old shard-assignment: {}. Active core-count:{}",
      shard,
      ss::smp::count);
    auto ntp_config = _md_cache.local()
                        .get_topic_cfg(model::topic_namespace_view(ntp))
                        ->make_ntp_config(_data_directory, ntp.tp.partition);
    return _pm.invoke_on(
      shard,
      [this,
       raft_group,
       ntp_config = std::move(ntp_config),
       replicas = std::move(replicas)](partition_manager& pm) mutable {
          return manage_partition(
            pm, std::move(ntp_config), raft_group, std::move(replicas));
      });
}

ss::future<> controller::manage_partition(
  partition_manager& pm,
  storage::ntp_config ntp_cfg,
  raft::group_id raft_group,
  std::vector<model::broker_shard> replicas) {
    auto path = ntp_cfg.ntp.path();
    return pm
      .manage(
        std::move(ntp_cfg),
        raft_group,
        get_replica_set_brokers(_md_cache.local(), std::move(replicas)),
        std::nullopt)
      .then([path = std::move(path), raft_group](consensus_ptr) {
          vlog(
            clusterlog.info,
            "recovered: {}, raft group_id: {}",
            path,
            raft_group);
      });
}

ss::future<> controller::assign_group_to_shard(
  model::ntp ntp, raft::group_id raft_group, uint32_t shard) {
    // 1. update shard_table: broadcast
    return _st.invoke_on_all(
      [ntp = std::move(ntp), raft_group, shard](shard_table& s) mutable {
          s.insert(std::move(ntp), shard);
          s.insert(raft_group, shard);
      });
}

void controller::end_of_stream() {}

ss::future<> controller::update_cache_with_partitions_assignment(
  const partition_assignment& p_as) {
    return _md_cache.invoke_on_all(
      [p_as](metadata_cache& md_c) { md_c.update_partition_assignment(p_as); });
}

ss::future<>
controller::recover_topic_configuration(topic_configuration t_cfg) {
    // broadcast to all caches
    return _md_cache.invoke_on_all(
      [t_cfg = std::move(t_cfg)](metadata_cache& md_c) mutable {
          md_c.add_topic(std::move(t_cfg));
      });
}

model::ntp
make_ntp(model::topic_namespace_view tp_ns, model::partition_id pid) {
    return model::ntp{.ns = tp_ns.ns,
                      .tp = {.topic = tp_ns.tp, .partition = pid}};
}

ss::future<> controller::process_topic_deletion(model::topic_namespace tp_ns) {
    auto tp_md = _md_cache.local().get_topic_metadata(tp_ns);

    // first remove topic from metadata cache
    return _md_cache
      .invoke_on_all(
        [tp_ns](metadata_cache& cache) { cache.remove_topic(tp_ns); })
      .then([this, partitions = std::move(tp_md->partitions), tp_ns]() mutable {
          // remove all partitions
          vlog(
            clusterlog.info,
            "Removing topic {}, partitions {}",
            tp_ns,
            partitions);
          return ss::parallel_for_each(
            partitions.begin(),
            partitions.end(),
            [this, tp_ns = std::move(tp_ns)](
              model::partition_metadata& p_md) mutable {
                return delete_partitition(tp_ns, std::move(p_md));
            });
      });
}

bool is_in_replica_set(
  const std::vector<model::broker_shard> replicas, model::node_id node_id) {
    auto it = std::find_if(
      std::cbegin(replicas),
      std::cend(replicas),
      [node_id](const model::broker_shard& bs) {
          return bs.node_id == node_id;
      });
    return it != replicas.end();
}

ss::future<> controller::delete_partitition(
  model::topic_namespace tp_ns, model::partition_metadata p_md) {
    // This partition is not replicated locally
    if (!is_in_replica_set(p_md.replicas, _self.id())) {
        return ss::make_ready_future<>();
    }
    auto ntp = make_ntp(tp_ns, p_md.id);
    // find partition shard
    auto shard = _st.local().shard_for(ntp);
    if (!shard) {
        vlog(
          clusterlog.error, "Unable to remove NTP {} - shard not found", ntp);
        return ss::make_ready_future<>();
    }

    return _pm
      .invoke_on(
        *shard,
        [ntp](cluster::partition_manager& pm) {
            // get group
            return pm.get(ntp)->group();
        })
      .then([this, ntp](raft::group_id group_id) mutable {
          // update shard table
          return _st.invoke_on_all(
            [ntp = std::move(ntp), group_id](shard_table& st) mutable {
                st.erase(ntp, group_id);
            });
      })
      .then([this, shard, ntp = std::move(ntp)] {
          return _pm.invoke_on(*shard, [ntp](cluster::partition_manager& pm) {
              // remove partition
              return pm.remove(ntp);
          });
      })
      .then([this, replicas = std::move(p_md.replicas)] {
          if (is_leader() && _allocator) {
              // update allocator if required
              for (auto r : replicas) {
                  _allocator->deallocate(r);
              }
          }
      });
}

/// Creates the topics, this method forwards the request to leader controller
/// if current node is not a leader.
///
/// After this method returns caller has following guaranetees for successfully
/// created topics:
///
/// - metadata for those topics are updated on each core
/// - Configuration is successfully replicated to majority of cluster members
///
/// In order to give the caller guarantee that topics metadata are up to date
/// the leader controller response contains created topics metadata. Requesting
/// controller use these to update its metadata cache. Otherwise it would have
/// to wait for raft-0 append entries notification
///
/// Following sequence is realized by this method, for the sequence on leader
/// controller, see create_topics method documentation.
///
///            +------------+       +-------------+    +------------+
///            | Controller |       |  Metadata   |    |   Leader   |
///            |            |       |   Cache     |    | Controller |
///            +------+-----+       +------+------+    +-----+------+
///                   |                    |                 |
///                   |                    |                 |
///  autocreate topics|                    |                 |
///+----------------->+ [RPC]create topics |                 |
///                   +------------------------------------->+
///                   |                    |                 |
///                   |                    |                 |
///                   |                    |                 |
///                   |  [RPC] response    |                 |
///                   +<-------------------------------------+
///                   |                    |                 |
///                   |                    |                 |
///                   |                    |                 |
///                   |   update cache     |                 |
///                   +------------------->+                 |
///    results        |                    |                 |
/// <-----------------+                    |                 |
///                   |                    |                 |
///                   |                    |                 |
///                   |                    |                 |
///                   +                    +                 +
///
///

ss::future<std::vector<topic_result>> controller::autocreate_topics(
  std::vector<topic_configuration> topics,
  model::timeout_clock::duration timeout) {
    using ret_t = std::vector<topic_result>;
    vlog(clusterlog.trace, "Autocreating topics {}", topics);
    if (is_leader()) {
        // create topics locally
        return create_topics(
          std::move(topics), timeout + model::timeout_clock::now());
    }

    return _operation_lock.with([this,
                                 topics = std::move(topics),
                                 timeout]() mutable {
        return dispatch_rpc_to_leader([topics, timeout](
                                        controller_client_protocol c) mutable {
                   vlog(
                     clusterlog.trace,
                     "Dispatching autocreate {} request to leader ",
                     topics);
                   return c
                     .create_topics(
                       create_topics_request{std::move(topics), timeout},
                       rpc::client_opts(timeout + model::timeout_clock::now()))
                     .then([](rpc::client_context<create_topics_reply> ctx) {
                         return std::move(ctx.data);
                     });
               })
          .then([this, topics](result<create_topics_reply> r) mutable {
              return process_autocreate_response(
                std::move(topics), std::move(r));
          })
          .handle_exception(
            [topics = std::move(topics)](const std::exception_ptr& e) mutable {
                // wasn't able to create topics
                vlog(
                  clusterlog.warn,
                  "Unknown error while autocreating topics {}",
                  e);
                return ss::make_ready_future<ret_t>(create_topic_results(
                  topics, errc::auto_create_topics_exception));
            });
    });
}

ss::future<std::vector<topic_result>> controller::process_autocreate_response(
  std::vector<topic_configuration> topics, result<create_topics_reply> r) {
    using ret_t = std::vector<topic_result>;
    if (!r) {
        return ss::make_ready_future<ret_t>(
          create_topic_results(topics, static_cast<errc>(r.error().value())));
    }
    return _state_lock.with(
      [this, topics = std::move(topics), r = std::move(r)]() mutable {
          return _md_cache
            .invoke_on_all(
              [cfg = std::move(r.value().configs),
               md = std::move(r.value().metadata)](metadata_cache& c) mutable {
                  vassert(
                    cfg.size() == md.size(),
                    "Error processing auto create topics response. List of "
                    "configurations have to have the same size as list of "
                    "metadata. "
                    "Topic configurations: {}, metadata: {}",
                    cfg,
                    md);
                  for (size_t i = 0; i < cfg.size(); ++i) {
                      c.insert_topic(std::move(md[i]), std::move(cfg[i]));
                  }
              })
            .then([res = std::move(r.value().results)]() mutable {
                return std::move(res);
            });
      });
}

/// Create topics API for Kafka API handler to call.
/// After this method returns caller has following guaranetees for successfully
/// created topics:
///
/// - metadata for those topics are updated on each core
/// - Configuration is successfully replicated to majority of cluster members
/// - Topic partitons abstractions are created and ready to use
///
/// Following sequence is realized by this method
///
///      +-------------+    +------------+    +-------------+    +------------+
///      |             |    |            |    |             |    |  Metadata  |
///      |  Kafka API  |    | Controller |    |   Raft 0    |    |   Cache    |
///      |             |    |            |    |             |    |            |
///      +------+------+    +------+-----+    +------+------+    +-----+------+
///             |                  |                 |                 |
/// create topics|                  |                 |                 |
///------------>+ create topics    |                 |                 |
///             +----------------->+  replicate      |                 |
///             |                  +---------------->+                 |
///             |                  |                 |                 |
///             |                  |                 |                 |
///             |                  |                 |                 |
///             |                  | append upcall   |                 |
///             |                  +<----------------+                 |
///             |                  |                 |                 |
///             |                  |                 |                 |
///             |                  |                 |                 |
///             |                  | update cache    |                 |
///             |                  +---------------------------------->+
///             |   results        |                 |                 |
///             +<-----------------+                 |                 |
///  <----------+                  |                 |                 |
///             |                  |                 |                 |
///             |                  |                 |                 |
///             +                  +                 +                 +
///

ss::future<std::vector<topic_result>> controller::create_topics(
  std::vector<topic_configuration> topics,
  model::timeout_clock::time_point timeout) {
    verify_shard();

    auto f = _operation_lock.with([this, topics, timeout]() mutable {
        return _state_lock.get_units().then(
          [this, topics = std::move(topics), timeout](
            ss::semaphore_units<> u) mutable {
              return do_create_topics(std::move(u), std::move(topics), timeout);
          });
    });

    return ss::with_timeout(timeout, std::move(f))
      .handle_exception_type(
        [topics = std::move(topics)](const ss::timed_out_error&) {
            return ss::make_ready_future<std::vector<topic_result>>(
              create_topic_results(topics, errc::timeout));
        });
}

///  Topic deletion works analogical to the creation. The deletion request is
///  replicated via raft-0 and then applied to controller state through raft
///  apply notifications upcall. When deletion record is processed by the
///  controller logic it executes the following steps:
///
///  1) Removes topic from metedata cache
///  2) For each topic partition:
///     a) removes partition via partition_manager API
///     b) removes partition mappings (NTP & group_id) from shard table
///     c) if current controller is leader updates partition allocator
///
///  After those actions are done it returns vector of results

ss::future<std::vector<topic_result>> controller::delete_topics(
  std::vector<model::topic_namespace> topics,
  model::timeout_clock::time_point timeout) {
    verify_shard();

    auto f = _operation_lock.with([this, topics, timeout]() mutable {
        return _state_lock.get_units().then(
          [this, topics, timeout](ss::semaphore_units<> u) mutable {
              return do_delete_topics(std::move(u), std::move(topics), timeout);
          });
    });

    return ss::with_timeout(timeout, std::move(f))
      .handle_exception_type(
        [topics = std::move(topics)](const ss::timed_out_error&) {
            return ss::make_ready_future<std::vector<topic_result>>(
              create_topic_results(topics, errc::timeout));
        });
}

static ss::future<std::vector<topic_result>> process_replicate_topic_op_result(
  std::vector<model::topic_namespace> valid_topics,
  notification_latch& latch,
  model::timeout_clock::time_point timeout,
  ss::future<result<raft::replicate_result>> f) {
    using ret_t = std::vector<topic_result>;
    try {
        auto result = f.get0();
        if (result.has_value()) {
            // success case
            // wait for notification
            return latch.wait_for(result.value().last_offset, timeout)
              .then([valid_topics = std::move(valid_topics)](errc ec) {
                  return create_topic_results(valid_topics, ec);
              });
        }
    } catch (...) {
        clusterlog.error(
          "An error occurred while appending topic operation entries: {}",
          std::current_exception());
    }
    return ss::make_ready_future<ret_t>(
      create_topic_results(valid_topics, errc::replication_error));
}

ss::future<std::vector<topic_result>> controller::do_delete_topics(
  ss::semaphore_units<> units,
  std::vector<model::topic_namespace> topics,
  model::timeout_clock::time_point timeout) {
    verify_shard();
    using ret_t = std::vector<topic_result>;
    clusterlog.trace("Delete topics {}", topics);
    // Controller is not a leader fail all requests
    if (!is_leader()) {
        return ss::make_ready_future<ret_t>(
          create_topic_results(topics, errc::not_leader_controller));
    }
    ret_t errors;
    errors.reserve(topics.size());
    auto valid_range_end = std::end(topics);
    // Check if exists
    valid_range_end = std::partition(
      std::begin(topics),
      valid_range_end,
      [this](const model::topic_namespace& tp_ns) {
          return _md_cache.local().get_topic_metadata(tp_ns).has_value();
      });

    // generate errors
    std::transform(
      valid_range_end,
      std::end(topics),
      std::back_inserter(errors),
      [](const model::topic_namespace& tp_ns) {
          return topic_result(tp_ns, errc::topic_not_exists);
      });

    topics.erase(valid_range_end, std::cend(topics));
    // no topics to erase
    if (topics.empty()) {
        return ss::make_ready_future<std::vector<topic_result>>(errors);
    }

    auto rdr = make_deletion_batches(topics);
    return _raft0
      ->replicate(
        std::move(rdr), raft::replicate_options(default_consistency_level))
      .then_wrapped(
        [this, units = std::move(units), timeout, topics = std::move(topics)](
          ss::future<result<raft::replicate_result>> f) mutable {
            return process_replicate_topic_op_result(
              topics, _notification_latch, timeout, std::move(f));
        })
      .then([errors = std::move(errors)](ret_t results) {
          // merge results from both sources
          std::move(
            std::begin(errors), std::end(errors), std::back_inserter(results));
          return results;
      });
}

ss::future<std::vector<topic_result>> controller::do_create_topics(
  ss::semaphore_units<> units,
  std::vector<topic_configuration> topics,
  model::timeout_clock::time_point timeout) {
    using ret_t = std::vector<topic_result>;
    clusterlog.trace("Create topics {}", topics);
    // Controller is not a leader fail all requests
    if (!is_leader() || _allocator == nullptr) {
        return ss::make_ready_future<ret_t>(
          create_topic_results(topics, errc::not_leader_controller));
    }

    ret_t errors;
    ss::circular_buffer<model::record_batch> batches;
    std::vector<model::topic_namespace> valid_topics;

    batches.reserve(topics.size());
    valid_topics.reserve(topics.size());

    for (auto& t_cfg : topics) {
        if (_md_cache.local().get_topic_metadata(t_cfg.tp_ns)) {
            errors.emplace_back(t_cfg.tp_ns, errc::topic_already_exists);
            continue;
        }
        auto batch = create_topic_cfg_batch(t_cfg);
        if (batch) {
            batches.push_back(std::move(*batch));
            valid_topics.push_back(std::move(t_cfg.tp_ns));
        } else {
            errors.emplace_back(t_cfg.tp_ns, errc::topic_invalid_partitions);
        }
    }
    // Do not need to replicate all configurations are invalid
    if (batches.empty()) {
        return ss::make_ready_future<ret_t>(std::move(errors));
    }

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    return _raft0
      ->replicate(
        std::move(rdr), raft::replicate_options(default_consistency_level))
      .then_wrapped(
        [this,
         valid_topics = std::move(valid_topics),
         units = std::move(units),
         timeout](ss::future<result<raft::replicate_result>> f) mutable {
            return process_replicate_topic_op_result(
              std::move(valid_topics),
              _notification_latch,
              timeout,
              std::move(f));
        })
      .then([errors = std::move(errors)](ret_t results) {
          // merge results from both sources
          clusterlog.error(
            "Topic create errors: {}, results: {}", errors, results);
          std::move(
            std::begin(errors), std::end(errors), std::back_inserter(results));
          return results;
      });
}

std::optional<model::record_batch>
controller::create_topic_cfg_batch(const topic_configuration& cfg) {
    simple_batch_builder builder(
      controller_record_batch_type, model::offset(_raft0->meta().commit_index));
    builder.add_kv(
      log_record_key{.record_type = log_record_key::type::topic_configuration},
      cfg);

    auto assignments = _allocator->allocate(cfg);
    if (!assignments) {
        vlog(clusterlog.error, "Unable to allocate partitions for {}", cfg);
        return std::nullopt;
    }
    log_record_key assignment_key = {
      .record_type = log_record_key::type::partition_assignment};
    for (auto const p_as : *assignments) {
        builder.add_kv(assignment_key, p_as);
    }
    return std::move(builder).build();
}

ss::future<> controller::do_leadership_notification(
  model::ntp ntp, model::term_id term, std::optional<model::node_id> lid) {
    verify_shard();
    // gate is reentrant making it ok if leadership notification originated
    // on the the controller::shard core.
    return with_gate(_bg, [this, ntp = std::move(ntp), lid, term]() mutable {
        auto f = _state_lock.get_units().then(
          [this, ntp = std::move(ntp), lid, term](
            ss::semaphore_units<> u) mutable {
              if (ntp == controller_ntp) {
                  return handle_controller_leadership_notification(
                    std::move(u), term, lid);
              } else {
                  auto f = _md_cache.invoke_on_all(
                    [ntp, lid, term](metadata_cache& md) {
                        md.update_partition_leader(ntp, term, lid);
                    });
                  if (lid == _self.id()) {
                      // only disseminate from current leader
                      f = f.then([this,
                                  ntp = std::move(ntp),
                                  term,
                                  lid,
                                  u = std::move(u)]() mutable {
                          return _md_dissemination_service.local()
                            .disseminate_leadership(std::move(ntp), term, lid);
                      });
                  }
                  return f;
              }
          });
    });
}

ss::future<> controller::handle_controller_leadership_notification(
  ss::semaphore_units<> u, model::term_id, std::optional<model::node_id> lid) {
    if (lid != _self.id()) {
        _is_leader = false;
        return ss::make_ready_future<>();
    }

    return _raft0
      ->replicate(
        model::make_memory_record_batch_reader({create_checkpoint_batch()}),
        raft::replicate_options(default_consistency_level))
      .then([this, u = std::move(u)](result<raft::replicate_result> res) {
          return _notification_latch
            .wait_for(res.value().last_offset, model::no_timeout)
            .then([this](errc ec) {
                if (ec != errc::success) {
                    vlog(
                      clusterlog.warn, "Unable to replicate data as a leader");
                    return;
                }
                vlog(clusterlog.info, "Local controller became a leader");
                create_partition_allocator();
                _is_leader = true;
                _leadership_cond.broadcast();
            });
      });
}

void controller::handle_leadership_notification(
  model::ntp ntp, model::term_id term, std::optional<model::node_id> lid) {
    // gate for this core's controller instance
    (void)with_gate(_bg, [this, ntp = std::move(ntp), lid, term]() mutable {
        // forward notification to controller's home core
        return container().invoke_on(
          controller::shard,
          [ntp = std::move(ntp), lid, term](controller& c) mutable {
              return c.do_leadership_notification(std::move(ntp), term, lid);
          });
    }).handle_exception([](std::exception_ptr e) {
        clusterlog.warn(
          "Exception thrown while processing leadership notification - {}", e);
    });
}

void controller::create_partition_allocator() {
    _allocator = std::make_unique<partition_allocator>(_highest_group_id);
    // _md_cache contains a mirror copy of metadata at each core
    // so it is sufficient to access core-local copy
    for (auto b : _md_cache.local().all_brokers()) {
        _allocator->register_node(std::make_unique<allocation_node>(
          allocation_node(b->id(), b->properties().cores, {})));
    }
    _allocator->update_allocation_state(
      _md_cache.local().all_topics_metadata());
}

void controller::update_partition_allocator(
  const std::vector<broker_ptr>& brokers) {
    for (const auto& b : brokers) {
        // FIXME: handle removing brokers
        if (!_allocator->contains_node(b->id())) {
            _allocator->register_node(std::make_unique<allocation_node>(
              allocation_node(b->id(), b->properties().cores, {})));
        }
    }
}

ss::future<>
controller::update_brokers_cache(std::vector<model::broker> nodes) {
    // broadcast update to all caches
    return _md_cache.invoke_on_all(
      [nodes = std::move(nodes)](metadata_cache& c) mutable {
          c.update_brokers_cache(std::move(nodes));
      });
}

void controller::on_raft0_entries_comitted(
  model::record_batch_reader&& reader) {
    (void)with_gate(_bg, [this, reader = std::move(reader)]() mutable {
        return _state_lock.with([this, reader = std::move(reader)]() mutable {
            if (_bg.is_closed()) {
                return ss::make_ready_future<>();
            }
            return std::move(reader).consume(
              batch_consumer(this), model::no_timeout);
        });
    }).handle_exception_type([](const ss::gate_closed_exception&) {
        vlog(
          clusterlog.info,
          "On shutdown... ignoring append_entries notification");
    });
}

ss::future<> controller::update_clients_cache(
  std::vector<broker_ptr> new_list, std::vector<broker_ptr> old_list) {
    return ss::do_with(
      calculate_changed_brokers(std::move(new_list), std::move(old_list)),
      [this](brokers_diff& diff) {
          return ss::do_for_each(
                   diff.removed,
                   [this](broker_ptr removed) {
                       return remove_broker_client(
                         _connection_cache, removed->id());
                   })
            .then([this, &diff] {
                return ss::do_for_each(diff.updated, [this](broker_ptr b) {
                    if (b->id() == _self.id()) {
                        // Do not create client to local broker
                        return ss::make_ready_future<>();
                    }
                    return update_broker_client(
                      _connection_cache, b->id(), b->rpc_address());
                });
            });
      });
}

ss::future<> controller::wait_for_leadership() {
    verify_shard();
    return with_gate(_bg, [this]() {
        return _leadership_cond.wait(
          [this] { return is_leader() || _bg.is_closed(); });
    });
}

ss::future<result<join_reply>> controller::dispatch_join_to_remote(
  const config::seed_server& target, model::broker joining_node) {
    vlog(
      clusterlog.info,
      "Sending join request to {} @ {}",
      target.id,
      target.addr);

    return with_client<controller_client_protocol>(
      _connection_cache,
      target.id,
      target.addr,
      [joining_node = std::move(joining_node)](
        controller_client_protocol c) mutable {
          return c
            .join(
              join_request(std::move(joining_node)),
              rpc::client_opts(rpc::clock_type::now() + join_timeout))
            .then([](rpc::client_context<join_reply> ctx) {
                return std::move(ctx.data);
            });
      });
}

static inline ss::future<> wait_for_next_join_retry(ss::abort_source& as) {
    using namespace std::chrono_literals; // NOLINT
    vlog(clusterlog.info, "Next cluster join attempt in 5 seconds");
    return ss::sleep_abortable(5s, as).handle_exception_type(
      [](const ss::sleep_aborted&) {
          vlog(clusterlog.debug, "Aborting join sequence");
      });
}

void controller::join_raft0() {
    (void)ss::with_gate(_bg, [this] {
        vlog(clusterlog.debug, "Trying to join the cluster");
        return ss::repeat([this] {
            return dispatch_join_to_seed_server(std::cbegin(_seed_servers))
              .then([this](result<join_reply> r) {
                  bool success = r && r.value().success;
                  // stop on success or closed gate
                  if (success || _bg.is_closed() || is_already_member()) {
                      return ss::make_ready_future<ss::stop_iteration>(
                        ss::stop_iteration::yes);
                  }

                  return wait_for_next_join_retry(_as).then(
                    [] { return ss::stop_iteration::no; });
              });
        });
    });
}

ss::future<result<join_reply>>
controller::dispatch_join_to_seed_server(seed_iterator it) {
    using ret_t = result<join_reply>;
    auto f = ss::make_ready_future<ret_t>(errc::seed_servers_exhausted);
    if (it == std::cend(_seed_servers)) {
        return f;
    }
    // Current node is a seed server, just call the method
    if (it->id == _self.id()) {
        vlog(clusterlog.debug, "Using current node as a seed server");
        f = process_join_request(_self);
    } else {
        // If seed is the other server then dispatch join requst to it
        f = dispatch_join_to_remote(*it, _self);
    }

    return f.then_wrapped([it, this](ss::future<ret_t> fut) {
        if (!fut.failed()) {
            if (auto r = fut.get0(); r.has_value()) {
                return ss::make_ready_future<ret_t>(std::move(r));
            }
        }
        vlog(
          clusterlog.info,
          "Error joining cluster using {} seed server",
          it->id);

        // Dispatch to next server
        return dispatch_join_to_seed_server(std::next(it));
    });
}

ss::future<result<join_reply>>
controller::process_join_request(model::broker broker) {
    verify_shard();
    using ret_t = result<join_reply>;
    vlog(clusterlog.info, "Processing node '{}' join request", broker.id());
    // curent node is a leader
    if (is_leader()) {
        // Just update raft0 configuration
        return _raft0->add_group_member(std::move(broker)).then([] {
            return ss::make_ready_future<ret_t>(join_reply{true});
        });
    }
    // Current node is not the leader have to send an RPC to leader
    // controller
    return dispatch_rpc_to_leader([broker = std::move(broker)](
                                    controller_client_protocol c) mutable {
               return c
                 .join(
                   join_request(std::move(broker)),
                   rpc::client_opts(rpc::clock_type::now() + join_timeout))
                 .then([](rpc::client_context<join_reply> reply) {
                     return reply.data;
                 });
           })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(
            clusterlog.warn,
            "Error while dispatching join request to leader node - {}",
            e);
          return ss::make_ready_future<ret_t>(
            errc::join_request_dispatch_error);
      });
}

storage::ntp_config controller::make_raft0_ntp_config() const {
    return storage::ntp_config(controller_ntp, _data_directory);
}

} // namespace cluster
