/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/shard_balancer.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "random/generators.h"
#include "ssx/async_algorithm.h"
#include "types.h"

namespace cluster {

namespace {

const bytes& state_kvstore_key() {
    static thread_local bytes key = []() {
        iobuf buf;
        serde::write(buf, shard_placement_kvstore_key_type::balancer_state);
        return iobuf_to_bytes(buf);
    }();
    return key;
}

struct persisted_state
  : serde::
      envelope<persisted_state, serde::version<0>, serde::compat_version<0>> {
    uint32_t last_rebalance_core_count = 0;

    bool operator==(const persisted_state&) const = default;
    auto serde_fields() { return std::tie(last_rebalance_core_count); }
};

} // namespace

shard_balancer::shard_balancer(
  ss::sharded<shard_placement_table>& spt,
  ss::sharded<features::feature_table>& features,
  ss::sharded<storage::api>& storage,
  ss::sharded<topic_table>& topics,
  ss::sharded<controller_backend>& cb,
  config::binding<bool> balancing_on_core_count_change,
  config::binding<bool> balancing_continuous,
  config::binding<std::chrono::milliseconds> debounce_timeout,
  config::binding<uint32_t> partitions_per_shard,
  config::binding<uint32_t> partitions_reserve_shard0)
  : _shard_placement(spt.local())
  , _features(features.local())
  , _storage(storage.local())
  , _topics(topics)
  , _controller_backend(cb)
  , _self(*config::node().node_id())
  , _balancing_on_core_count_change(std::move(balancing_on_core_count_change))
  , _balancing_continuous(std::move(balancing_continuous))
  , _debounce_timeout(std::move(debounce_timeout))
  , _debounce_jitter(_debounce_timeout())
  , _partitions_per_shard(std::move(partitions_per_shard))
  , _partitions_reserve_shard0(std::move(partitions_reserve_shard0))
  , _balance_timer([this] { balance_timer_callback(); })
  , _total_counts(ss::smp::count, 0) {
    _total_counts.at(0) += 1; // controller partition

    _debounce_timeout.watch([this] {
        _debounce_jitter = simple_time_jitter<ss::lowres_clock>(
          _debounce_timeout());
    });
}

ss::future<> shard_balancer::start(size_t kvstore_shard_count) {
    vassert(
      ss::this_shard_id() == shard_id,
      "method can only be invoked on shard {}",
      shard_id);

    auto gate_holder = _gate.hold();
    auto lock = co_await _mtx.get_units();

    // Collect the set of node-local ntps from topic_table

    // We expect topic_table to remain unchanged throughout the method
    // invocation because it is supposed to be called after local controller
    // replay is finished but before we start getting new controller updates
    // from the leader.
    auto tt_version = _topics.local().topics_map_revision();

    chunked_hash_map<raft::group_id, model::ntp> local_group2ntp;
    chunked_hash_map<model::ntp, model::revision_id> local_ntp2log_revision;
    const auto& topics = _topics.local();
    ssx::async_counter counter;
    for (const auto& [ns_tp, md_item] : topics.all_topics_metadata()) {
        vassert(
          tt_version == topics.topics_map_revision(),
          "topic_table unexpectedly changed");

        co_await ssx::async_for_each_counter(
          counter,
          md_item.get_assignments().begin(),
          md_item.get_assignments().end(),
          [&](const assignments_set::value_type& p) {
              vassert(
                tt_version == topics.topics_map_revision(),
                "topic_table unexpectedly changed");
              const auto& [_, p_as] = p;
              model::ntp ntp{ns_tp.ns, ns_tp.tp, p_as.id};
              auto replicas_view = topics.get_replicas_view(ntp, md_item, p_as);
              auto log_rev = log_revision_on_node(replicas_view, _self);
              if (log_rev) {
                  local_group2ntp.emplace(replicas_view.assignment.group, ntp);
                  local_ntp2log_revision.emplace(ntp, *log_rev);
              }
          });
    }

    if (kvstore_shard_count > ss::smp::count) {
        // Check that we can decrease shard count

        ss::sstring reject_reason;
        if (!_features.is_active(
              features::feature::node_local_core_assignment)) {
            reject_reason
              = "node_local_core_assignment feature flag is not yet active";
        }
        if (!_balancing_on_core_count_change()) {
            reject_reason = "balancing on core count change is disabled";
        }
        size_t max_capacity = ss::smp::count * _partitions_per_shard();
        max_capacity -= std::min(
          max_capacity, static_cast<size_t>(_partitions_reserve_shard0()));
        if (local_group2ntp.size() > max_capacity) {
            reject_reason = ssx::sformat(
              "the number of partition replicas on this node ({}) is greater "
              "than max capacity with this core count ({})",
              local_group2ntp.size(),
              max_capacity);
        }

        if (!reject_reason.empty()) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Detected decrease in number of cores dedicated to run Redpanda "
              "from {} to {}, but it is impossible because {}.",
              kvstore_shard_count,
              ss::smp::count,
              reject_reason));
        }
    }

    std::vector<std::unique_ptr<storage::kvstore>> extra_kvstores;
    for (ss::shard_id s = ss::smp::count; s < kvstore_shard_count; ++s) {
        extra_kvstores.push_back(co_await _storage.make_extra_kvstore(s));
    }

    co_await init_shard_placement(
      lock, local_group2ntp, local_ntp2log_revision, extra_kvstores)
      .finally([&] {
          return ss::parallel_for_each(
            extra_kvstores, [](auto& kvs) { return kvs->stop(); });
      });

    if (kvstore_shard_count > ss::smp::count) {
        // Now that all partition info is copied from extra kvstores, we can
        // remove them.
        co_await _storage.log_mgr().remove_orphan_files(
          config::node().data_directory().as_sstring(),
          {model::redpanda_ns},
          [](model::ntp ntp, storage::partition_path::metadata) {
              return ntp.tp.topic == model::kvstore_topic
                     && ntp.tp.partition() >= static_cast<int>(ss::smp::count);
          });
    }

    // we shouldn't be receiving any controller updates at this point, so no
    // risk of missing a notification between initializing shard_placement_table
    // and subscribing.
    _topic_table_notify_handle
      = _topics.local().register_ntp_delta_notification(
        [this](topic_table::ntp_delta_range_t deltas_range) {
            for (const auto& delta : deltas_range) {
                // Filter out only deltas that might change the set of partition
                // replicas on this node.
                switch (delta.type) {
                case topic_table_ntp_delta_type::disabled_flag_updated:
                case topic_table_ntp_delta_type::properties_updated:
                    continue;
                default:
                    _to_assign.insert(delta.ntp);
                    _wakeup_event.set();
                    break;
                }
            }
        });

    vassert(
      tt_version == _topics.local().topics_map_revision(),
      "topic_table unexpectedly changed");

    ssx::background = assign_fiber();
}

ss::future<> shard_balancer::init_shard_placement(
  mutex::units& lock,
  const chunked_hash_map<raft::group_id, model::ntp>& local_group2ntp,
  const chunked_hash_map<model::ntp, model::revision_id>&
    local_ntp2log_revision,
  const std::vector<std::unique_ptr<storage::kvstore>>& extra_kvstores) {
    // 1. restore shard_placement_table from the kvstore or from topic_table.

    if (
      _features.is_active(features::feature::node_local_core_assignment)
      && !_shard_placement.is_persistence_enabled()) {
        // Joiner node joining a cluster that has already enabled the feature?
        // Enable persistence before initializing.
        co_await _shard_placement.enable_persistence();
    }

    std::vector<std::unique_ptr<shard_placement_table>> extra_spts;

    if (_shard_placement.is_persistence_enabled()) {
        extra_spts = co_await _shard_placement.initialize_from_kvstore(
          local_group2ntp, extra_kvstores);
    } else {
        // topic_table is still the source of truth
        co_await _shard_placement.initialize_from_topic_table(_topics, _self);

        if (_features.is_preparing(
              features::feature::node_local_core_assignment)) {
            // We may have joined or restarted while the feature is still in the
            // preparing state. Enable persistence here before we get new
            // controller updates to avoid races with activation of the feature.
            co_await _shard_placement.enable_persistence();
        }
    }

    // 2. Initialize shard partition counts and assign non-assigned local ntps.
    //
    // Note: old assignments for ntps not in local_group2ntp have already been
    // removed during shard_placement_table initialization.

    co_await ssx::async_for_each(
      local_ntp2log_revision.begin(),
      local_ntp2log_revision.end(),
      [&](const std::pair<const model::ntp&, model::revision_id> kv) {
          const auto& [ntp, log_revision] = kv;
          auto existing_target = _shard_placement.get_target(ntp);

          if (existing_target) {
              update_counts(
                ntp,
                _topic2data[model::topic_namespace_view{ntp}],
                std::nullopt,
                existing_target);
          }

          if (
            !existing_target || existing_target->log_revision != log_revision) {
              _to_assign.insert(ntp);
          }
      });

    co_await do_assign_ntps(lock);

    // 3. Do balancing on startup if needed

    if (
      _balancing_on_core_count_change()
      && _features.is_active(features::feature::node_local_core_assignment)) {
        co_await balance_on_core_count_change(
          lock, ss::smp::count + extra_kvstores.size());
    }

    // 4. Move partition info from extra kvstores

    for (size_t i = 0; i < extra_kvstores.size(); ++i) {
        auto& extra_kvs = *extra_kvstores.at(i);
        auto& extra_spt = *extra_spts.at(i);
        co_await _controller_backend.local()
          .transfer_partitions_from_extra_shard(extra_kvs, extra_spt);
    }
}

ss::future<> shard_balancer::stop() {
    vassert(
      ss::this_shard_id() == shard_id,
      "method can only be invoked on shard {}",
      shard_id);

    _topics.local().unregister_ntp_delta_notification(
      _topic_table_notify_handle);
    _balance_timer.cancel();
    _wakeup_event.set();
    return _gate.close();
}

ss::future<> shard_balancer::enable_persistence() {
    auto gate_holder = _gate.hold();
    if (_shard_placement.is_persistence_enabled()) {
        co_return;
    }
    vassert(
      _features.is_preparing(features::feature::node_local_core_assignment),
      "unexpected feature state");
    co_await _shard_placement.enable_persistence();
}

ss::future<errc>
shard_balancer::reassign_shard(model::ntp ntp, ss::shard_id shard) {
    if (_gate.is_closed()) {
        co_return errc::shutting_down;
    }
    auto gate_holder = _gate.hold();

    if (!_features.is_active(features::feature::node_local_core_assignment)) {
        co_return errc::feature_disabled;
    }

    auto lock = co_await _mtx.get_units();

    if (shard >= ss::smp::count) {
        co_return errc::invalid_request;
    }
    auto replicas_view = _topics.local().get_replicas_view(ntp);
    if (!replicas_view) {
        co_return errc::partition_not_exists;
    }
    auto log_revision = log_revision_on_node(*replicas_view, _self);
    if (!log_revision) {
        co_return errc::replica_does_not_exist;
    }

    auto target = shard_placement_target{
      replicas_view->assignment.group, *log_revision, shard};
    vlog(
      clusterlog.info,
      "[{}] manually setting placement target to {}",
      ntp,
      target);

    update_counts(
      ntp,
      _topic2data[model::topic_namespace_view{ntp}],
      _shard_placement.get_target(ntp),
      target);
    co_await set_target(ntp, target, lock);
    co_return errc::success;
}

errc shard_balancer::trigger_rebalance() {
    if (_gate.is_closed()) {
        return errc::shutting_down;
    }

    if (!_features.is_active(features::feature::node_local_core_assignment)) {
        return errc::feature_disabled;
    }

    vlog(clusterlog.info, "triggering manual rebalancing");
    _balance_timer.rearm(ss::lowres_clock::now());
    return errc::success;
}

ss::future<> shard_balancer::assign_fiber() {
    if (_gate.is_closed()) {
        co_return;
    }
    auto gate_holder = _gate.hold();

    while (true) {
        co_await _wakeup_event.wait(1s);
        if (_gate.is_closed()) {
            co_return;
        }

        auto lock = co_await _mtx.get_units();
        co_await do_assign_ntps(lock);
    }
}

using ntp2target_t
  = chunked_hash_map<model::ntp, std::optional<shard_placement_target>>;

ss::future<> shard_balancer::do_assign_ntps(mutex::units& lock) {
    ntp2target_t new_targets;
    auto to_assign = std::exchange(_to_assign, {});
    co_await ssx::async_for_each(
      to_assign.begin(), to_assign.end(), [&](const model::ntp& ntp) {
          maybe_assign(ntp, /*can_reassign=*/false, new_targets);
      });

    co_await ss::max_concurrent_for_each(
      new_targets,
      128,
      [this, &lock](const decltype(new_targets)::value_type& kv) {
          const auto& [ntp, target] = kv;
          return set_target(ntp, target, lock)
            .handle_exception([this, &ntp](const std::exception_ptr&) {
                // Retry on the next tick.
                _to_assign.insert(ntp);
            });
      });
}

void shard_balancer::maybe_assign(
  const model::ntp& ntp, bool can_reassign, ntp2target_t& new_targets) {
    std::optional<shard_placement_target> prev_target
      = _shard_placement.get_target(ntp);

    std::optional<model::revision_id> log_revision;
    auto replicas_view = _topics.local().get_replicas_view(ntp);
    if (replicas_view) {
        log_revision = log_revision_on_node(*replicas_view, _self);
    }

    if (!log_revision && !prev_target) {
        return;
    }

    auto& topic_data = _topic2data[model::topic_namespace_view{ntp}];

    std::optional<shard_placement_target> target;
    if (log_revision) {
        // partition is expected to exist on this node, choose its shard.

        if (_features.is_active(
              features::feature::node_local_core_assignment)) {
            vassert(
              _shard_placement.is_persistence_enabled(),
              "expected persistence to be enabled");

            std::optional<ss::shard_id> prev_shard;
            if (prev_target && prev_target->log_revision == log_revision) {
                prev_shard = prev_target->shard;
            }

            if (prev_shard && !can_reassign) {
                // partition already assigned, keep current shard.
                return;
            }

            auto new_shard = choose_shard(ntp, topic_data, prev_shard);
            if (new_shard == prev_shard) {
                return;
            }

            target.emplace(
              replicas_view->assignment.group, log_revision.value(), new_shard);
        } else {
            // node-local shard placement not enabled yet, get target from
            // topic_table.
            target = placement_target_on_node(replicas_view.value(), _self);
        }
    } else {
        // partition is removed from this node, this will likely disrupt the
        // counts balance, so we set up the balancing timer.

        if (
          _features.is_active(features::feature::node_local_core_assignment)
          && _balancing_continuous() && !_balance_timer.armed()) {
            // Add jitter so that different nodes don't move replicas of the
            // same partition in unison.
            auto debounce_interval = _debounce_jitter.next_duration();
            vlog(
              clusterlog.info,
              "scheduling balancing in {}s.",
              debounce_interval / 1s);
            _balance_timer.arm(debounce_interval);
        }
    }

    vlog(
      clusterlog.debug,
      "[{}] assigning shard {} (prev: {}, topic counts: {}, total counts: {})",
      ntp,
      target ? std::optional(target->shard) : std::nullopt,
      prev_target ? std::optional(prev_target->shard) : std::nullopt,
      topic_data.shard2count,
      _total_counts);

    update_counts(ntp, topic_data, prev_target, target);
    new_targets.emplace(ntp, target);
}

ss::future<> shard_balancer::balance_on_core_count_change(
  mutex::units& lock, size_t kvstore_shard_count) {
    uint32_t last_rebalance_core_count = 0;
    auto state_buf = _storage.kvs().get(
      storage::kvstore::key_space::shard_placement, state_kvstore_key());
    if (state_buf) {
        last_rebalance_core_count = serde::from_iobuf<persisted_state>(
                                      std::move(*state_buf))
                                      .last_rebalance_core_count;
    }

    // If there is no state in kvstore (and therefore last_rebalance_core_count
    // is 0), this means that we are restarting with shard balancing enabled for
    // the first time, and this is a good time to rebalance as well.

    if (
      last_rebalance_core_count == ss::smp::count
      && kvstore_shard_count == ss::smp::count) {
        co_return;
    }

    vlog(
      clusterlog.info, "detected core count change, triggering rebalance...");
    co_await do_balance(lock);
}

void shard_balancer::balance_timer_callback() {
    ssx::spawn_with_gate(_gate, [this] {
        return _mtx.get_units()
          .then([this](mutex::units lock) {
              return ss::do_with(std::move(lock), [this](mutex::units& lock) {
                  return do_balance(lock);
              });
          })
          .handle_exception([this](const std::exception_ptr& e) {
              if (ssx::is_shutdown_exception(e)) {
                  return;
              }

              // Retry balancing after some time.
              if (!_balance_timer.armed()) {
                  _balance_timer.arm(_debounce_jitter.next_duration());
              }
              vlog(
                clusterlog.warn,
                "failed to balance: {}, retrying after {}s.",
                e,
                (_balance_timer.get_timeout() - ss::lowres_clock::now()) / 1s);
          });
    });
}

ss::future<> shard_balancer::do_balance(mutex::units& lock) {
    // Go over all node-local ntps in random order and try to find a more
    // optimal core for them.
    chunked_vector<model::ntp> ntps;
    co_await _shard_placement.for_each_ntp(
      [&](const model::ntp& ntp, const shard_placement_target&) {
          ntps.push_back(ntp);
      });
    std::shuffle(ntps.begin(), ntps.end(), random_generators::internal::gen);

    ntp2target_t new_targets;
    co_await ssx::async_for_each(
      ntps.begin(), ntps.end(), [&](const model::ntp& ntp) {
          maybe_assign(ntp, /*can_reassign=*/true, new_targets);
      });

    vlog(
      clusterlog.info,
      "after balancing {} ntps were reassigned",
      new_targets.size());

    co_await ss::max_concurrent_for_each(
      new_targets,
      128,
      [this, &lock](const decltype(new_targets)::value_type& kv) {
          const auto& [ntp, target] = kv;
          return set_target(ntp, target, lock);
      });

    co_await _storage.kvs().put(
      storage::kvstore::key_space::shard_placement,
      state_kvstore_key(),
      serde::to_iobuf(persisted_state{
        .last_rebalance_core_count = ss::smp::count,
      }));
}

ss::future<> shard_balancer::set_target(
  const model::ntp& ntp,
  const std::optional<shard_placement_target>& target,
  mutex::units& /*lock*/) {
    auto shard_callback = [this](const model::ntp& ntp) {
        _controller_backend.local().notify_reconciliation(ntp);
    };

    try {
        co_await _shard_placement.set_target(ntp, target, shard_callback);
    } catch (...) {
        auto ex = std::current_exception();
        if (!ssx::is_shutdown_exception(ex)) {
            vlog(
              clusterlog.warn,
              "[{}] exception while setting target: {}",
              ntp,
              ex);
        }

        // revert shard counts update if needed
        auto cur_target = _shard_placement.get_target(ntp);
        if (cur_target != target) {
            update_counts(
              ntp,
              _topic2data[model::topic_namespace_view{ntp}],
              target,
              cur_target);
        }

        throw;
    }
}

ss::shard_id shard_balancer::choose_shard(
  const model::ntp&,
  const topic_data_t& topic_data,
  std::optional<ss::shard_id> prev) const {
    std::vector<ss::shard_id> candidates;

    // lower score is better
    auto optimize_level = [&](auto candidates_range, auto get_shard_score) {
        std::vector<ss::shard_id> next_candidates;
        std::optional<decltype(get_shard_score(0))> min_score;
        for (ss::shard_id shard : candidates_range) {
            auto score = get_shard_score(shard);
            if (!min_score || *min_score >= score) {
                if (min_score != score) {
                    min_score = score;
                    next_candidates.clear();
                }
                next_candidates.push_back(shard);
            }
        }
        candidates = std::move(next_candidates);
    };

    // Hierarchical optimization, first optimize topic counts, then total
    // counts.

    auto topic_count_score = [&](ss::shard_id shard) {
        auto score = topic_data.shard2count.at(shard);
        if (prev != shard) {
            score += 1;
        }
        return score;
    };
    optimize_level(
      std::views::iota(ss::shard_id(0), ss::shard_id(ss::smp::count)),
      topic_count_score);

    auto total_count_score = [&](ss::shard_id shard) {
        auto score = _total_counts.at(shard);
        if (prev != shard) {
            score += 1;
        }
        return score;
    };
    optimize_level(candidates, total_count_score);

    if (prev) {
        for (ss::shard_id cand : candidates) {
            if (cand == prev) {
                // if prev shard has the optimal score, keep it.
                return cand;
            }
        }
    }

    return random_generators::random_choice(candidates);
}

void shard_balancer::update_counts(
  const model::ntp& ntp,
  topic_data_t& topic_data,
  const std::optional<shard_placement_target>& prev,
  const std::optional<shard_placement_target>& next) {
    // Shard values that are >= ss::smp::count are possible when initializing
    // shard placement after a core count decrease. We ignore them because
    // partition counts on extra shards are not needed for balancing.

    if (prev && prev->shard < ss::smp::count) {
        topic_data.shard2count.at(prev->shard) -= 1;
        topic_data.total_count -= 1;
        // TODO: check negative values
        _total_counts.at(prev->shard) -= 1;
    }

    if (next && next->shard < ss::smp::count) {
        topic_data.shard2count.at(next->shard) += 1;
        topic_data.total_count += 1;
        _total_counts.at(next->shard) += 1;
    }

    if (topic_data.total_count == 0) {
        _topic2data.erase(model::topic_namespace_view{ntp});
    }
}

} // namespace cluster
