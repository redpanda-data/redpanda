#include "cluster/controller.h"

#include "cluster/logger.h"
#include "cluster/simple_batch_builder.h"
#include "config/configuration.h"
#include "model/record_batch_reader.h"
#include "resource_mgmt/io_priority.h"
#include "storage/shard_assignment.h"
namespace cluster {
static void verify_shard() {
    if (__builtin_expect(engine().cpu_id() != controller::shard, false)) {
        throw std::runtime_error(fmt::format(
          "Attempted to access controller on core: {}", engine().cpu_id()));
    }
}

controller::controller(
  sharded<partition_manager>& pm,
  sharded<shard_table>& st,
  sharded<metadata_cache>& md_cache)
  : _self(config::make_self_broker(config::shard_local_cfg()))
  , _seed_servers(config::shard_local_cfg().seed_servers())
  , _pm(pm)
  , _st(st)
  , _md_cache(md_cache)
  , _raft0(nullptr)
  , _highest_group_id(0) {}

future<> controller::start() {
    verify_shard();
    _pm.local().register_leadership_notification(
      [this](lw_shared_ptr<partition> p) {
          if (p->ntp() == controller::ntp) {
              verify_shard();
              leadership_notification();
          }
      });
    return _pm
      .invoke_on_all([](partition_manager& pm) {
          // start the partition managers first...
          return pm.start();
      })
      .then([this] {
          clusterlog.debug("Starting cluster recovery");
          return _pm.local()
            .manage(controller::ntp, controller::group)
            .then([this](consensus_ptr c) {
                auto plog = _pm.local().logs().find(controller::ntp)->second;
                _raft0 = c.get();
                return bootstrap_from_log(plog);
            })
            .then([this] {
                _raft0->register_hook(
                  [this](std::vector<raft::entry>&& entries) {
                      verify_shard();
                      on_raft0_entries_commited(std::move(entries));
                  });
            })
            .then([this] { return join_raft_group(*_raft0); })
            .then([this] {
                clusterlog.info("Finished recovering cluster state");
                _recovered = true;
                if (_leadership_notification_pending) {
                    leadership_notification();
                    _leadership_notification_pending = false;
                }
            });
      });
}

future<> controller::stop() {
    verify_shard();
    return _bg.close();
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
        return make_exception_future<>(std::runtime_error(
          "We cannot process compressed record_batch'es yet, see #188"));
    }
    return do_with(std::move(batch), [this](model::record_batch& batch) {
        return do_for_each(batch, [this](model::record& rec) {
            return recover_record(std::move(rec));
        });
    });
}

future<> controller::recover_record(model::record r) {
    return rpc::deserialize<log_record_key>(r.share_key())
      .then([this, v_buf = std::move(r.share_packed_value_and_headers())](
              log_record_key key) mutable {
          return dispatch_record_recovery(std::move(key), std::move(v_buf));
      });
}

future<>
controller::dispatch_record_recovery(log_record_key key, iobuf&& v_buf) {
    switch (key.record_type) {
    case log_record_key::type::partition_assignment:
        return rpc::deserialize<partition_assignment>(std::move(v_buf))
          .then([this](partition_assignment as) {
              return recover_assignment(std::move(as));
          });
    case log_record_key::type::topic_configuration:
        return rpc::deserialize<topic_configuration>(std::move(v_buf))
          .then([this](topic_configuration t_cfg) {
              return recover_topic_configuration(std::move(t_cfg));
          });
    default:
        return make_exception_future<>(
          std::runtime_error("Not supported record type in controller batch"));
    }
}

future<> controller::recover_assignment(partition_assignment as) {
    _highest_group_id = std::max(_highest_group_id, as.group);
    return do_with(std::move(as), [this](partition_assignment& as) {
        return update_cache_with_partitions_assignment(as).then([this, &as] {
            return do_for_each(
              as.replicas,
              [this, raft_group = as.group, ntp = std::move(as.ntp)](
                model::broker_shard& bs) {
                  return recover_replica(
                    std::move(ntp), raft_group, std::move(bs));
              });
        });
    });
}

future<> controller::recover_replica(
  model::ntp ntp, raft::group_id raft_group, model::broker_shard bs) {
    // if the assignment is not for current broker just update
    // metadata cache
    if (bs.node_id != _self.id()) {
        return make_ready_future<>();
    }
    // the following ops have a dependency on the shard_table
    // *then* partition_manager order

    // FIXME: Pass topic configuration to partitions manager

    // (compression, compation, etc)
    // 1. update shard_table: broadcast
    return _st
      .invoke_on_all([ntp, raft_group, shard = bs.shard](shard_table& s) {
          s.insert(ntp, shard);
          s.insert(raft_group, shard);
      })
      .then([this, shard = bs.shard, raft_group, ntp] {
          // 2. update partition_manager
          return dispatch_manage_partition(std::move(ntp), raft_group, shard);
      });
}

future<> controller::dispatch_manage_partition(
  model::ntp ntp, raft::group_id raft_group, uint32_t shard) {
    return _pm.invoke_on(
      shard, [this, raft_group, ntp = std::move(ntp)](partition_manager& pm) {
          return manage_partition(pm, std::move(ntp), raft_group);
      });
}

future<> controller::manage_partition(
  partition_manager& pm, model::ntp ntp, raft::group_id raft_group) {
    return pm.manage(ntp, raft_group)
      .then([this](consensus_ptr c) { return join_raft_group(*c); })
      .then([path = ntp.path(), raft_group] {
          clusterlog.info("recovered: {}, raft group_id: {}", path, raft_group);
      });
}

void controller::end_of_stream() {}

future<> controller::update_cache_with_partitions_assignment(
  const partition_assignment& p_as) {
    return _md_cache.invoke_on_all(
      [p_as](metadata_cache& md_c) { md_c.update_partition_assignment(p_as); });
}

future<> controller::recover_topic_configuration(topic_configuration t_cfg) {
    // broadcast to all caches
    return _md_cache.invoke_on_all(
      [tp = t_cfg.topic](metadata_cache& md_c) { md_c.add_topic(tp); });
}

future<std::vector<topic_result>> controller::create_topics(
  std::vector<topic_configuration> topics,
  model::timeout_clock::time_point timeout) {
    verify_shard();
    if (!is_leader() || _allocator == nullptr) {
        return make_ready_future<std::vector<topic_result>>(
          create_topic_results(
            std::move(topics), topic_error_code::not_leader_controller));
    }
    std::vector<topic_result> errors;
    std::vector<raft::entry> entries;
    entries.reserve(topics.size());
    for (const auto& t_cfg : topics) {
        auto entry = create_topic_cfg_entry(t_cfg);
        if (entry) {
            entries.push_back(std::move(*entry));
        } else {
            errors.emplace_back(
              t_cfg.topic, topic_error_code::invalid_partitions);
        }
    }

    // Do append entries to raft0 logs
    auto f = _raft0->replicate(std::move(entries))
               .then_wrapped([topics = std::move(topics)](future<> f) {
                   bool success = true;
                   try {
                       f.get();
                   } catch (...) {
                       auto e = std::current_exception();
                       clusterlog.error(
                         "An error occurred while "
                         "appending create topic entries: {}",
                         e);
                       success = false;
                   }
                   return create_topic_results(
                     std::move(topics),
                     success ? topic_error_code::no_error
                             : topic_error_code::unknown_error);
               })
               .then([errors = std::move(errors)](
                       std::vector<topic_result> results) {
                   // merge results from both sources
                   std::move(
                     std::begin(errors),
                     std::end(errors),
                     std::back_inserter(results));
                   return std::move(results);
               });

    return with_timeout(timeout, std::move(f));
} // namespace cluster

std::optional<raft::entry>
controller::create_topic_cfg_entry(const topic_configuration& cfg) {
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
    std::vector<model::record_batch> batches;
    batches.push_back(std::move(builder).build());
    return raft::entry(
      controller_record_batch_type,
      model::make_memory_record_batch_reader(std::move(batches)));
}

void controller::leadership_notification() {
    (void)with_gate(_bg, [this]() mutable {
        if (__builtin_expect(!_recovered, false)) {
            _leadership_notification_pending = true;
            return;
        }
        clusterlog.info("Local controller became a leader");
        create_partition_allocator();
    });
}

void controller::create_partition_allocator() {
    _allocator = std::make_unique<partition_allocator>(_highest_group_id);

    _allocator->register_node(
      std::make_unique<allocation_node>(local_allocation_node()));
    // _md_cache contains a mirror copy of metadata at each core
    // so it is sufficient to access core-local copy
    _allocator->update_allocation_state(
      _md_cache.local().all_topics_metadata());
}

allocation_node controller::local_allocation_node() {
    return allocation_node(_self.id(), smp::count, {});
}

future<> controller::join_raft_group(raft::consensus& c) {
    // FIXME: Replace this naive join with full fledged join
    // mechanism. This is stubbed implementation for being able to
    // test raft
    if (!c.config().contains_broker(_self.id())) {
        return c.add_group_member(_self);
    }
    return make_ready_future<>();
}

void controller::on_raft0_entries_commited(std::vector<raft::entry>&& entries) {
    if (_bg.is_closed()) {
        throw gate_closed_exception();
    }
    (void)with_gate(_bg, [this, entries = std::move(entries)]() mutable {
        return do_with(
          std::move(entries), [this](std::vector<raft::entry>& entries) {
              return do_for_each(entries, [this](raft::entry& entry) {
                  return entry.reader().consume(
                    batch_consumer(this), model::no_timeout);
              });
          });
    });
}

} // namespace cluster
