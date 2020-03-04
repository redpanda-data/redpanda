#include "cluster/controller.h"

#include "cluster/cluster_utils.h"
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/notification_latch.h"
#include "cluster/partition_manager.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "resource_mgmt/io_priority.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "storage/shard_assignment.h"
#include "utils/retry.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/net/inet_address.hh>

namespace cluster {
static void verify_shard() {
    if (unlikely(ss::engine().cpu_id() != controller::shard)) {
        throw std::runtime_error(fmt::format(
          "Attempted to access controller on core: {}", ss::engine().cpu_id()));
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
  , _pm(pm)
  , _st(st)
  , _md_cache(md_cache)
  , _connection_cache(connection_cache)
  , _md_dissemination_service(md_dissemination)
  , _raft0(nullptr)
  , _highest_group_id(0) {}

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
    if (ss::engine().cpu_id() != controller::shard) {
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
            controller::ntp, controller::group, controller::shard);
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
                return apply_raft0_cfg_update(_raft0->config());
            })
            .then([this] { return join_raft0(); })
            .then([this] {
                vlog(clusterlog.info, "Finished recovering cluster state");
                _recovered = true;
                if (_leadership_notification_pending) {
                    handle_leadership_notification(
                      controller::ntp, model::term_id{}, _self.id());
                    _leadership_notification_pending = false;
                }
            });
      })
      .then([this] {
          // done in background fibre
          _md_dissemination_service.local().initialize_leadership_metadata();
      });
}

ss::future<consensus_ptr> controller::start_raft0() {
    std::vector<model::broker> brokers;
    if (_seed_servers.front().id() == _self.id()) {
        vlog(clusterlog.info, "Current node is cluster root");
        brokers.push_back(_self);
    }
    return _pm.local().manage(
      controller::ntp,
      controller::group,
      std::move(brokers),
      [this](model::record_batch_reader&& reader) {
          verify_shard();
          on_raft0_entries_comitted(std::move(reader));
      });
}

ss::future<> controller::stop() {
    _pm.local().unregister_leadership_notification(_leader_notify_handle);
    if (ss::engine().cpu_id() == controller::shard && _raft0) {
        _raft0->remove_append_entries_callback();
    }
    if (ss::engine().cpu_id() == controller::shard) {
        // in a multi-threaded app this would look like a deadlock waiting
        // to happen, but it works in seastar: broadcast here only makes the
        // waiters runnable. they won't actually have a chance to run until
        // after the closed flag within the gate is set, ensuring that when
        // they wake up they'll leave the gate after observing that it is
        // closed.
        _leadership_cond.broadcast();
    }
    _as.request_abort();
    return _bg.close();
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
        f = model::consume_records(
          std::move(batch), [this](model::record rec) mutable {
              return recover_record(std::move(rec));
          });
    }

    return f.then(
      [this, last_offset] { return _notification_latch.notify(last_offset); });
}

ss::future<> controller::apply_raft0_cfg_update(raft::group_configuration cfg) {
    auto old_list = _md_cache.local().all_brokers();
    std::vector<broker_ptr> new_list;
    auto all_new_nodes = cfg.all_brokers();
    if (is_leader()) {
        update_partition_allocator(all_new_nodes);
    }
    new_list.reserve(all_new_nodes.size());
    std::transform(
      std::begin(all_new_nodes),
      std::end(all_new_nodes),
      std::back_inserter(new_list),
      [](model::broker& broker) {
          return ss::make_lw_shared<model::broker>(std::move(broker));
      });
    return update_clients_cache(std::move(new_list), std::move(old_list))
      .then([this, nodes = std::move(cfg.nodes)] {
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
      r.share_value());
    return apply_raft0_cfg_update(std::move(cfg));
}

ss::future<> controller::recover_record(model::record r) {
    auto log_record = reflection::adl<log_record_key>{}.from(r.share_key());
    return dispatch_record_recovery(std::move(log_record), r.share_value());
}

ss::future<>
controller::dispatch_record_recovery(log_record_key key, iobuf&& v_buf) {
    switch (key.record_type) {
    case log_record_key::type::partition_assignment:
        return recover_assignment(
          reflection::from_iobuf<partition_assignment>(std::move(v_buf)));
    case log_record_key::type::topic_configuration:
        return recover_topic_configuration(
          reflection::from_iobuf<topic_configuration>(std::move(v_buf)));
    default:
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
    return _pm.invoke_on(
      shard,
      [this, raft_group, ntp = std::move(ntp), replicas = std::move(replicas)](
        partition_manager& pm) {
          return manage_partition(
            pm, std::move(ntp), raft_group, std::move(replicas));
      });
}

ss::future<> controller::manage_partition(
  partition_manager& pm,
  model::ntp ntp,
  raft::group_id raft_group,
  std::vector<model::broker_shard> replicas) {
    return pm
      .manage(
        ntp,
        raft_group,
        get_replica_set_brokers(_md_cache.local(), std::move(replicas)),
        std::nullopt)
      .then([path = ntp.path(), raft_group](consensus_ptr) {
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
      [ntp = std::move(ntp), raft_group, shard](shard_table& s) {
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
      [tp = t_cfg.topic](metadata_cache& md_c) { md_c.add_topic(tp); });
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
  model::timeout_clock::time_point timeout) {
    if (is_leader()) {
        // create topics locally
        return create_topics(std::move(topics), timeout);
    }

    return dispatch_rpc_to_leader([topics = std::move(topics), timeout](
                                    controller_client_protocol& c) mutable {
               return c.create_topics(
                 create_topics_request{std::move(topics),
                                       timeout - model::timeout_clock::now()},
                 timeout);
           })
      .then([this](rpc::client_context<create_topics_reply> ctx) {
          return _md_cache
            .invoke_on_all(
              [md = std::move(ctx.data.metadata)](metadata_cache& c) mutable {
                  for (auto& m : md) {
                      c.insert_topic(std::move(m));
                  }
              })
            .then([res = std::move(ctx.data.results)]() mutable {
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

    auto f = ss::get_units(_sem, 1).then(
      [this, topics = std::move(topics), timeout](
        ss::semaphore_units<> u) mutable {
          return do_create_topics(std::move(u), std::move(topics), timeout);
      });

    return ss::with_timeout(timeout, std::move(f));
}

static ss::future<std::vector<topic_result>> process_create_topics_results(
  std::vector<model::topic> valid_topics,
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
          "An error occurred while appending create topic entries: {}",
          std::current_exception());
    }
    return ss::make_ready_future<ret_t>(
      create_topic_results(valid_topics, errc::replication_error));
}

ss::future<std::vector<topic_result>> controller::do_create_topics(
  ss::semaphore_units<> units,
  std::vector<topic_configuration> topics,
  model::timeout_clock::time_point timeout) {
    using ret_t = std::vector<topic_result>;
    // Controller is not a leader fail all requests
    if (!is_leader() || _allocator == nullptr) {
        return ss::make_ready_future<ret_t>(
          create_topic_results(topics, errc::not_leader_controller));
    }

    ret_t errors;
    ss::circular_buffer<model::record_batch> batches;
    std::vector<model::topic> valid_topics;

    batches.reserve(topics.size());
    valid_topics.reserve(topics.size());

    for (auto& t_cfg : topics) {
        if (_md_cache.local().get_topic_metadata(t_cfg.topic)) {
            errors.emplace_back(t_cfg.topic, errc::topic_already_exists);
            continue;
        }
        auto batch = create_topic_cfg_batch(t_cfg);
        if (batch) {
            batches.push_back(std::move(*batch));
            valid_topics.push_back(std::move(t_cfg.topic));
        } else {
            errors.emplace_back(t_cfg.topic, errc::topic_invalid_partitions);
        }
    }
    // Do not need to replicate all configurations are invalid
    if (batches.empty()) {
        return ss::make_ready_future<ret_t>(std::move(errors));
    }

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    return _raft0->replicate(std::move(rdr))
      .then_wrapped(
        [this,
         valid_topics = std::move(valid_topics),
         units = std::move(units),
         timeout](ss::future<result<raft::replicate_result>> f) mutable {
            return process_create_topics_results(
              std::move(valid_topics),
              _notification_latch,
              timeout,
              std::move(f));
        })
      .then([errors = std::move(errors)](ret_t results) {
          // merge results from both sources
          std::move(
            std::begin(errors), std::end(errors), std::back_inserter(results));
          return std::move(results);
      });
}

std::optional<model::record_batch>
controller::create_topic_cfg_batch(const topic_configuration& cfg) {
    simple_batch_builder builder(
      controller::controller_record_batch_type,
      model::offset(_raft0->meta().commit_index));
    builder.add_kv(
      log_record_key{.record_type = log_record_key::type::topic_configuration},
      cfg);

    auto assignments = _allocator->allocate(cfg);
    if (!assignments) {
        clusterlog.error(
          "Unable to allocate partitions for topic '{}'", cfg.topic());
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
        return with_semaphore(
          _sem, 1, [this, ntp = std::move(ntp), lid, term]() mutable {
              if (ntp == controller::ntp) {
                  if (lid != _self.id()) {
                      // for now do nothing if we are not the leader
                      return ss::make_ready_future<>();
                  }
                  if (unlikely(!_recovered)) {
                      _leadership_notification_pending = true;
                      return ss::make_ready_future<>();
                  }
                  vlog(clusterlog.info, "Local controller became a leader");
                  create_partition_allocator();
                  _leadership_cond.broadcast();
                  return ss::make_ready_future<>();
              } else {
                  auto f = _md_cache.invoke_on_all(
                    [ntp, lid, term](metadata_cache& md) {
                        md.update_partition_leader(
                          ntp.tp.topic, ntp.tp.partition, term, lid);
                    });
                  if (lid == _self.id()) {
                      // only disseminate from current leader
                      f = f.then(
                        [this, ntp = std::move(ntp), term, lid]() mutable {
                            return _md_dissemination_service.local()
                              .disseminate_leadership(
                                std::move(ntp), term, lid);
                        });
                  }
                  return f;
              }
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
  const std::vector<model::broker>& brokers) {
    for (auto& b : brokers) {
        // FIXME: handle removing brokers
        if (!_allocator->contains_node(b.id())) {
            _allocator->register_node(std::make_unique<allocation_node>(
              allocation_node(b.id(), b.properties().cores, {})));
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
        return with_semaphore(
          _sem, 1, [this, reader = std::move(reader)]() mutable {
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

ss::future<join_reply> controller::dispatch_join_to_remote(
  const config::seed_server& target, model::broker joining_node) {
    vlog(
      clusterlog.info,
      "Sending join request to {} @ {}",
      target.id,
      target.addr);
    return dispatch_rpc(
      _connection_cache,
      target.id,
      target.addr,
      [joining_node = std::move(joining_node)](
        controller_client_protocol& c) mutable {
          return c
            .join(
              join_request(std::move(joining_node)),
              rpc::clock_type::now() + join_timeout)
            .then([](rpc::client_context<join_reply> ctx) {
                return std::move(ctx.data);
            });
      });
}

[[gnu::always_inline]] static ss::future<>
wait_for_next_join_retry(ss::abort_source& as) {
    using namespace std::chrono_literals; // NOLINT
    return ss::sleep_abortable(5s, as).handle_exception_type(
      [](const ss::sleep_aborted&) {
          vlog(clusterlog.debug, "Aborting join sequence");
      });
}

void controller::join_raft0() {
    (void)ss::with_gate(_bg, [this] {
        return ss::do_until(
          [this] {
              // stop if we are already cluster member or gate is closed
              return _raft0->config().contains_broker(_self.id())
                     || _bg.is_closed();
          },
          [this] {
              vlog(clusterlog.debug, "Trying to join the cluster");
              return dispatch_join_to_seed_server(std::cbegin(_seed_servers))
                .finally([this] { return wait_for_next_join_retry(_as); });
          });
    });
}

ss::future<> controller::dispatch_join_to_seed_server(seed_iterator it) {
    if (it == std::cend(_seed_servers)) {
        return ss::make_ready_future<>();
    }
    // Current node is a seed server, just call the method
    if (it->id == _self.id()) {
        vlog(clusterlog.debug, "Using current node as a seed server");
        return process_join_request(_self).handle_exception(
          [this, it](std::exception_ptr e) {
              clusterlog.warn("Error processing join request at current node");
              return dispatch_join_to_seed_server(std::next(it));
          });
    }
    // If seed is the other server then dispatch join requst to it
    return dispatch_join_to_remote(*it, _self)
      .then_wrapped([it, this](ss::future<join_reply> f) {
          try {
              join_reply reply = f.get0();
              if (reply.success) {
                  return ss::make_ready_future<>();
              }
          } catch (...) {
              clusterlog.error(
                "Error sending join request - {}", std::current_exception());
          }
          // Dispatch to next server
          return dispatch_join_to_seed_server(std::next(it));
      });
}

ss::future<> controller::process_join_request(model::broker broker) {
    verify_shard();
    vlog(clusterlog.info, "Processing node '{}' join request", broker.id());
    // curent node is a leader
    if (is_leader()) {
        // Just update raft0 configuration
        return _raft0->add_group_member(std::move(broker));
    }
    // Current node is not the leader have to send an RPC to leader
    // controller
    return dispatch_rpc_to_leader([this, broker = std::move(broker)](
                                    controller_client_protocol& c) mutable {
               return c.join(
                 join_request(std::move(broker)),
                 rpc::clock_type::now() + join_timeout);
           })
      .discard_result();
}

template<typename Func>
ss::futurize_t<std::result_of_t<Func(controller_client_protocol&)>>
controller::dispatch_rpc_to_leader(Func&& f) {
    using ret_t
      = ss::futurize<std::result_of_t<Func(controller_client_protocol&)>>;
    auto leader_id = get_leader_id();
    if (!leader_id) {
        return ret_t::make_exception_future(
          std::runtime_error("There is no leader controller in cluster"));
    }
    auto leader = _raft0->config().find_in_nodes(*leader_id);

    if (leader == _raft0->config().nodes.end()) {
        return ret_t::make_exception_future(
          std::runtime_error("There is no leader controller in cluster"));
    }
    // Dispatch request to current leader
    return dispatch_rpc(
      _connection_cache,
      leader->id(),
      leader->rpc_address(),
      std::forward<Func>(f));
}
} // namespace cluster
