#include "kafka/server/group_metadata_migration.h"

#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/feature_manager.h"
#include "cluster/feature_table.h"
#include "cluster/fwd.h"
#include "cluster/partition.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_recovery_consumer.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"
#include "storage/utils/transforming_reader.h"

#include <seastar/core/abort_source.hh>

#include <algorithm>

namespace kafka {
static ss::logger mlog("group-metadata-migration");

group_metadata_migration::group_metadata_migration(
  cluster::controller& controller,
  ss::sharded<kafka::group_router>& group_router)
  : _controller(controller)
  , _group_router(group_router) {}

const cluster::feature_barrier_tag
  group_metadata_migration::preparing_barrier_tag{"consumer_offsets_preparing"};

const cluster::feature_barrier_tag group_metadata_migration::active_barrier_tag{
  "consumer_offsets_active"};

static constexpr std::chrono::seconds default_wait_time(3);

namespace {
ss::future<std::optional<model::record_batch>>
transform_batch(model::record_batch batch) {
    if (batch.header().type == model::record_batch_type::raft_configuration) {
        // replace raft configuration with an empty checkpoint batch, the empty
        // batch will simply be ignored but the source and target topic offsets
        // will match
        co_return std::nullopt;
    }

    if (batch.header().type == model::record_batch_type::raft_data) {
        auto source_serializer = kafka::make_backward_compatible_serializer();
        auto target_serializer = kafka::make_consumer_offsets_serializer();
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset{});

        for (auto& r : batch.copy_records()) {
            auto tp = source_serializer.get_metadata_type(r.share_key());
            switch (tp) {
            case group_metadata_type::group_metadata: {
                auto md = source_serializer.decode_group_metadata(std::move(r));
                auto kv = target_serializer.to_kv(std::move(md));
                builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
                break;
            }
            case group_metadata_type::offset_commit: {
                auto md = source_serializer.decode_offset_metadata(
                  std::move(r));
                auto kv = target_serializer.to_kv(std::move(md));
                builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
                break;
            }
            case noop:
                // skip over noop batches, they do not change the group state
                break;
            }
        }

        co_return std::move(builder).build();
    }
    co_return std::move(batch);
}

ss::future<bool> create_consumer_offsets_topic(
  cluster::controller& controller,
  std::vector<cluster::partition_assignment> assignments,
  model::timeout_clock::time_point timeout) {
    cluster::custom_assignable_topic_configuration topic(
      cluster::topic_configuration{
        model::kafka_consumer_offsets_nt.ns,
        model::kafka_consumer_offsets_nt.tp,
        static_cast<int32_t>(assignments.size()),
        static_cast<int16_t>(assignments.front().replicas.size())});
    topic.cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    for (auto& p_as : assignments) {
        cluster::custom_partition_assignment custom{.id = p_as.id};
        custom.replicas.reserve(p_as.replicas.size());
        for (auto bs : p_as.replicas) {
            custom.replicas.push_back(bs.node_id);
        }
        topic.custom_assignments.push_back(std::move(custom));
    }

    return controller.get_topics_frontend()
      .local()
      .create_topics({std::move(topic)}, timeout)
      .then([](std::vector<cluster::topic_result> res) {
          /*
           * kindly ask client to retry on error
           */
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec != cluster::errc::success) {
              vlog(
                mlog.warn,
                "can not create __consumer_offsets topic - {}",
                res[0].ec);
              return false;
          }
          return true;
      })
      .then([&controller, timeout](bool success) {
          if (success) {
              return controller.get_api()
                .local()
                .wait_for_topic(model::kafka_consumer_offsets_nt, timeout)
                .then([](std::error_code ec) { return !ec; });
          }
          return ss::make_ready_future<bool>(success);
      })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(mlog.warn, "can not create __consumer_offsets topic - {}", e);
          // various errors may returned such as a timeout, or if the
          // controller group doesn't have a leader. client will retry.
          return false;
      });
}

ss::future<group_recovery_consumer_state> recover_group_state(
  model::ntp ntp,
  ss::shard_id shard,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<ss::abort_source>& as,
  group_metadata_serializer_factory serializer_f) {
    using ret_t = group_recovery_consumer_state;
    return pm.invoke_on(
      shard,
      [ntp = std::move(ntp), &as, serializer_f = std::move(serializer_f)](
        cluster::partition_manager& pm) mutable {
          auto p = pm.get(ntp);
          if (!p) {
              return ss::make_ready_future<ret_t>(ret_t{});
          }
          auto reader_cfg = storage::log_reader_config(
            p->start_offset(),
            model::offset::max(),
            ss::default_priority_class());

          return p->make_reader(reader_cfg)
            .then([&as = as.local(), serializer_f = std::move(serializer_f)](
                    model::record_batch_reader reader) mutable {
                return std::move(reader).consume(
                  group_recovery_consumer(serializer_f(), as),
                  model::no_timeout);
            });
      });
}

bool are_offsets_equal(
  const group::offset_metadata& lhs, const group::offset_metadata& rhs) {
    return lhs.metadata == rhs.metadata && lhs.offset == rhs.offset
           && lhs.committed_leader_epoch == rhs.committed_leader_epoch;
}
bool are_stms_equivalent(const group_stm& source, const group_stm& target) {
    if (source.get_metadata() != target.get_metadata()) {
        vlog(
          mlog.debug,
          "source and target group metadata does not match, source: {}, "
          "target: {}",
          source.get_metadata(),
          target.get_metadata());
        return false;
    }
    if (source.offsets().size() != target.offsets().size()) {
        return false;
    }
    for (auto& [tp, o] : source.offsets()) {
        auto it = target.offsets().find(tp);
        if (it == target.offsets().end()) {
            vlog(
              mlog.debug,
              "unable to find offset for {} in target group offsets",
              tp);
            return false;
        }
        if (it->second.metadata != o.metadata) {
            vlog(
              mlog.debug,
              "{} offsets does not match. source: {}, target {}",
              tp,
              o.metadata,
              it->second.metadata);
            return false;
        }
    }

    if (source.fences() != target.fences()) {
        vlog(mlog.debug, "group fences does not match");
        return false;
    }

    if (source.prepared_txs().size() != target.prepared_txs().size()) {
        return false;
    }

    for (auto& [tp, tx] : source.prepared_txs()) {
        auto it = target.prepared_txs().find(tp);
        if (it == target.prepared_txs().end()) {
            vlog(
              mlog.debug,
              "unable to find preppared txs for {} in target group state",
              tp);
            return false;
        }
        if (it->second.tx_seq != tx.tx_seq || tx.pid != it->second.pid) {
            return false;
        }
        if (tx.offsets.size() != it->second.offsets.size()) {
            return false;
        }

        for (auto& [offset_tp, o] : tx.offsets) {
            auto oit = it->second.offsets.find(offset_tp);
            if (oit == it->second.offsets.end()) {
                return false;
            }
            if (!are_offsets_equal(oit->second, o)) {
                return false;
            }
        }
    }

    return true;
}

bool are_states_equivalent(
  const group_recovery_consumer_state& source,
  const group_recovery_consumer_state& target) {
    if (source.groups.size() != target.groups.size()) {
        return false;
    }

    for (auto& [g, stm] : source.groups) {
        auto it = target.groups.find(g);
        if (it == target.groups.end()) {
            vlog(mlog.debug, "group {} not found in target group state", g);
            return false;
        }
        if (!are_stms_equivalent(stm, it->second)) {
            return false;
        }
    }
    return true;
}

ss::future<std::error_code> replicate(
  ss::shard_id shard,
  model::record_batch_reader reader,
  ss::sharded<cluster::partition_manager>& pm,
  model::ntp ntp) {
    auto f_reader = make_foreign_record_batch_reader(std::move(reader));
    return pm.invoke_on(
      shard,
      [ntp = std::move(ntp),
       f_reader = std::move(f_reader)](cluster::partition_manager& pm) mutable {
          return pm.get(ntp)
            ->replicate(
              std::move(f_reader),
              raft::replicate_options(raft::consistency_level::quorum_ack))
            .then([ntp](result<raft::replicate_result> res) {
                if (res) {
                    vlog(
                      mlog.info,
                      "replicated {} data up to {} offset",
                      ntp,
                      res.value().last_offset);
                    return raft::make_error_code(raft::errc::success);
                }
                return res.error();
            });
      });
}

bool is_source_partition_ready(const ss::lw_shared_ptr<cluster::partition>& p) {
    if (!p->is_leader()) {
        vlog(mlog.info, "not yet leader for source partition: {}", p->ntp());
        return false;
    }

    if (p->dirty_offset() > p->committed_offset()) {
        vlog(
          mlog.info,
          "partition {} dirty offset {} is greater than committed offset {}",
          p->ntp(),
          p->dirty_offset(),
          p->committed_offset());
        return false;
    }
    return true;
}

ss::future<> do_dispatch_ntp_migration(
  model::ntp ntp,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::shard_table>& st,
  ss::sharded<ss::abort_source>& as) {
    auto source = pm.local().get(ntp);
    /**
     * not every node contain a replica of __consumer_offsets topic
     */
    if (!source) {
        co_return;
    }

    model::ntp target_ntp(
      model::kafka_consumer_offsets_nt.ns,
      model::kafka_consumer_offsets_nt.tp,
      source->ntp().tp.partition);

    auto target_shard = st.local().shard_for(target_ntp);
    while (!target_shard) {
        vlog(
          mlog.info,
          "unable to find shard for {}, waiting for it to be present",
          target_ntp);
        co_await ss::sleep_abortable(default_wait_time, as.local());
        target_shard = st.local().shard_for(target_ntp);
    }

    /**
     * Transform data from source to target partition, this loop is being
     * executed on all of the nodes until we reach the point in which last batch
     * of source and target partition matches. This replication can not be based
     * on offsets since source topic is compacted.
     */
    bool is_state_equal = false;
    group_recovery_consumer_state source_state;

    while (!(is_state_equal || as.local().abort_requested())) {
        try {
            auto target_is_leader = co_await pm.invoke_on(
              *target_shard, [target_ntp](cluster::partition_manager& pm) {
                  return pm.get(target_ntp)->is_leader();
              });
            vlog(
              mlog.info,
              "transforming data from {} to {} - is leader: {}",
              source->ntp(),
              target_ntp,
              target_is_leader);

            if (target_is_leader) {
                // check source partition status
                if (!is_source_partition_ready(source)) {
                    vlog(
                      mlog.info,
                      "waiting for source partition {} to become a leader",
                      source->ntp());
                    auto err = co_await source->request_leadership(
                      model::timeout_clock::now() + default_wait_time);
                    if (err) {
                        vlog(
                          mlog.warn,
                          "error requesting leadership for {} - {}",
                          source->ntp(),
                          err.message());
                        co_await ss::sleep_abortable(
                          default_wait_time, as.local());
                    }
                    continue;
                }
                source_state = co_await recover_group_state(
                  source->ntp(),
                  ss::this_shard_id(),
                  pm,
                  as,
                  make_backward_compatible_serializer);
                // no groups to migrate, return early
                if (source_state.groups.empty()) {
                    vlog(
                      mlog.info,
                      "finished migrating {}, no groups to migrate ",
                      source->ntp());
                    co_return;
                }
                auto reader_cfg = storage::log_reader_config(
                  source->start_offset(),
                  model::offset::max(),
                  ss::default_priority_class());

                auto rdr = co_await source->make_reader(reader_cfg);

                auto t_rdr = storage::make_transforming_reader(
                  std::move(rdr), transform_batch);

                if (t_rdr.is_end_of_stream()) {
                    continue;
                }
                auto err = co_await replicate(
                  *target_shard, std::move(t_rdr), pm, target_ntp);

                if (err) {
                    vlog(
                      mlog.warn,
                      "migration of {} failed with error: {}",
                      target_ntp,
                      err.message());
                    co_await ss::sleep_abortable(default_wait_time, as.local());
                    continue;
                } else {
                    vlog(
                      mlog.info,
                      "successfully replicated migrated data for {}",
                      target_ntp);
                }
            } else {
                co_await ss::sleep_abortable(default_wait_time, as.local());
            }
            auto target_state = co_await recover_group_state(
              target_ntp,
              *target_shard,
              pm,
              as,
              make_consumer_offsets_serializer);
            /**
             * Read source state once again to make sure that we are up to date,
             * (we read state from the follower so it might have changed)
             */
            source_state = co_await recover_group_state(
              source->ntp(),
              ss::this_shard_id(),
              pm,
              as,
              make_backward_compatible_serializer);
            is_state_equal = are_states_equivalent(source_state, target_state);

        } catch (...) {
            vlog(
              mlog.warn,
              "error while migrating {} to {} - {}",
              source->ntp(),
              target_ntp,
              std::current_exception());
        }
    }
    vlog(mlog.info, "finished migrating {}", source->ntp());
}

ss::future<> dispatch_ntps_migration(
  std::vector<model::ntp> ntps,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::shard_table>& st,
  ss::sharded<ss::abort_source>& as) {
    for (auto& ntp : ntps) {
        co_await do_dispatch_ntp_migration(std::move(ntp), pm, st, as);
    }
}

bool are_all_finished(
  const std::vector<cluster::ntp_reconciliation_state>& state) {
    return std::all_of(
      state.begin(),
      state.end(),
      [](const cluster::ntp_reconciliation_state& r_state) {
          return r_state.status() == cluster::reconciliation_status::done;
      });
}

ss::future<> wait_for_stable_group_topic(
  cluster::controller_api& api, ss::abort_source& as) {
    auto result = co_await api.get_reconciliation_state(model::kafka_group_nt);
    while (result.has_error() || !are_all_finished(result.value())) {
        if (result.has_error()) {
            vlog(
              mlog.warn,
              "unable to get kafka_internal/group reconciliation status - {}",
              result.error());
        } else {
            vlog(
              mlog.debug,
              "waiting for partition operations to finish, current state: {}",
              result.value());
        }

        co_await ss::sleep_abortable(default_wait_time, as);
        result = co_await api.get_reconciliation_state(model::kafka_group_nt);
    }
}

} // namespace

cluster::feature_table& group_metadata_migration::feature_table() {
    return _controller.get_feature_table().local();
}

cluster::feature_manager& group_metadata_migration::feature_manager() {
    return _controller.get_feature_manager().local();
}
ss::abort_source& group_metadata_migration::abort_source() {
    return _controller.get_abort_source().local();
}

ss::future<> group_metadata_migration::activate_feature(ss::abort_source& as) {
    vlog(mlog.info, "activating consumer offsets feature");
    while (!feature_table().is_active(cluster::feature::consumer_offsets)
           && !as.abort_requested()) {
        if (_controller.is_raft0_leader()) {
            co_await feature_table().await_feature_preparing(
              cluster::feature::consumer_offsets, as);

            auto err = co_await feature_manager().write_action(
              cluster::feature_update_action{
                .feature_name = ss::sstring(
                  _controller.get_feature_table()
                    .local()
                    .get_state(cluster::feature::consumer_offsets)
                    .spec.name),
                .action
                = cluster::feature_update_action::action_t::complete_preparing,
              });

            if (
              err
              || !feature_table().is_active(
                cluster::feature::consumer_offsets)) {
                if (err) {
                    vlog(
                      mlog.info,
                      "error activating consumer offsets feature: {}",
                      err.message());
                }
                co_await ss::sleep_abortable(default_timeout, as);
            }
        } else {
            co_await ss::sleep_abortable(default_timeout, as);
        }
    }
}

ss::future<> group_metadata_migration::do_apply() {
    vlog(
      mlog.info,
      "waiting for consumer offsets feature to be preparing - current "
      "state: {}",
      _controller.get_feature_table()
        .local()
        .get_state(cluster::feature::consumer_offsets)
        .get_state());
    co_await feature_table().await_feature_preparing(
      cluster::feature::consumer_offsets, abort_source());
    vlog(mlog.info, "disabling partition movement feature and group router");
    co_await _group_router.invoke_on_all(&group_router::disable);
    vlog(mlog.info, "shutting down source group manager");
    co_await _group_router.local().get_group_manager().invoke_on_all(
      &group_manager::stop);

    co_await _controller.get_topics_frontend().invoke_on_all(
      &cluster::topics_frontend::disable_partition_movement);
    vlog(mlog.info, "waiting for stable consumer group topic");
    co_await wait_for_stable_group_topic(
      _controller.get_api().local(), _controller.get_abort_source().local());
    vlog(mlog.info, "applying consumer groups migrations");
    co_await migrate_metadata();
    vlog(mlog.info, "waiting for all migrations to finish");
    co_await feature_manager().barrier(active_barrier_tag);
    vlog(mlog.info, "finishing migration - enabling consumer offsets feature");

    co_await activate_feature(_controller.get_abort_source().local());

    co_await feature_table().await_feature(
      cluster::feature::consumer_offsets, abort_source());
    co_await _group_router.invoke_on_all([](group_router& router) {
        return router.get_group_manager().local().reload_groups();
    });
    vlog(mlog.info, "consumer offset feature enabled");
    co_await _group_router.invoke_on_all(&group_router::enable);
    co_await _controller.get_topics_frontend().invoke_on_all(
      &cluster::topics_frontend::enable_partition_movement);
}

ss::future<> group_metadata_migration::migrate_metadata() {
    auto& topics = _controller.get_topics_state().local();
    auto group_topic_assignment = topics.get_topic_assignments(
      model::kafka_group_nt);
    // no group topic found, skip migration
    if (!group_topic_assignment) {
        co_return;
    }
    auto partitions = group_topic_assignment->size();

    // create consumer offsets topic based on the old configuration
    while (!topics.contains(
      model::kafka_consumer_offsets_nt, model::partition_id{0})) {
        if (_controller.is_raft0_leader()) {
            vlog(mlog.info, "creating consumer offsets topic");
            co_await create_consumer_offsets_topic(
              _controller,
              std::move(*group_topic_assignment),
              default_deadline());
            continue;
        }

        co_await ss::sleep_abortable(
          default_timeout, _controller.get_abort_source().local());
    }

    vlog(mlog.info, "waiting for preparing barrier");
    co_await feature_manager().barrier(preparing_barrier_tag);

    absl::node_hash_map<ss::shard_id, std::vector<model::ntp>> shard_ntps;
    for (int16_t p_id = 0; p_id < static_cast<int16_t>(partitions); ++p_id) {
        model::ntp ntp(
          model::kafka_group_nt.ns,
          model::kafka_group_nt.tp,
          model::partition_id(p_id));

        auto shard_id = _controller.get_shard_table().local().shard_for(ntp);

        if (shard_id) {
            shard_ntps[*shard_id].push_back(std::move(ntp));
        }
    }

    for (auto& [shard, ntps] : shard_ntps) {
        ssx::spawn_with_gate(
          _partitions_gate, [this, shard = shard, ntps = ntps]() mutable {
              vlog(
                mlog.info,
                "dispatching {} ntps migration on shard: {}",
                ntps.size(),
                shard);
              return ss::smp::submit_to(
                shard,
                [&pm = _controller.get_partition_manager(),
                 &st = _controller.get_shard_table(),
                 &as = _controller.get_abort_source(),
                 ntps = std::move(ntps)]() mutable -> ss::future<> {
                    return dispatch_ntps_migration(std::move(ntps), pm, st, as);
                });
          });
    }

    co_await _partitions_gate.close();

    vlog(mlog.info, "finished migrating kafka_internal/group partitions");
}

ss::future<> group_metadata_migration::start(ss::abort_source& as) {
    // if version is overriden for test purposes - skip migration
    if (
      feature_table().get_latest_logical_version()
      < cluster::cluster_version{2}) {
        co_return;
    }
    if (feature_table().is_active(cluster::feature::consumer_offsets)) {
        feature_manager().exit_barrier(preparing_barrier_tag);
        feature_manager().exit_barrier(active_barrier_tag);
        co_return;
    }

    // if no group topic is present just make feature active
    if (!_controller.get_topics_state().local().contains(
          model::kafka_group_nt, model::partition_id{0})) {
        vlog(
          mlog.info,
          "kafka_internal/group topic does not exists, activating "
          "consumer_offsets feature");

        ssx::spawn_with_gate(_background_gate, [this, &as]() -> ss::future<> {
            return activate_feature(as);
        });
        co_return;
    }
    // otherwise wait for feature to be preparing and execute migration
    ssx::spawn_with_gate(
      _background_gate, [this]() -> ss::future<> { return do_apply(); });
    co_return;
}

// awaits for the migration to finish
ss::future<> group_metadata_migration::await() {
    return _background_gate.close();
}

} // namespace kafka
