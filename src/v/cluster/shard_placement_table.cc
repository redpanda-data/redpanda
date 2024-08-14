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

#include "cluster/shard_placement_table.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/topic_table.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/async_algorithm.h"
#include "types.h"

#include <seastar/util/defer.hh>

namespace cluster {

class shard_placement_table::probe {
public:
    probe() = default;
    probe(const probe&) = delete;
    probe& operator=(const probe&) = delete;
    probe(probe&&) = delete;
    probe& operator=(probe&&) = delete;

    void update_assigned(int64_t delta) { _total_assigned += delta; }
    void update_hosted(int64_t delta) { _total_hosted += delta; }
    void update_to_reconcile(int64_t delta) { _to_reconcile += delta; }

    void setup_metrics() {
        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        namespace sm = ss::metrics;
        _metrics.add_group(
          prometheus_sanitize::metrics_name("cluster:shard_placement"),
          {
            sm::make_gauge(
              "assigned_partitions",
              [this] { return _total_assigned; },
              sm::description("Number of partitions assigned to this shard")),
            sm::make_gauge(
              "hosted_partitions",
              [this] { return _total_hosted; },
              sm::description("Number of partitions hosted on this shard")),
            sm::make_gauge(
              "partitions_to_reconcile",
              [this] { return _to_reconcile; },
              sm::description("Number of partitions needing reconciliation of "
                              "shard-local state")),
          });
    }

private:
    int64_t _total_assigned = 0;
    int64_t _total_hosted = 0;
    int64_t _to_reconcile = 0;

    metrics::internal_metric_groups _metrics;
};

std::ostream& operator<<(
  std::ostream& o, const shard_placement_table::shard_local_assignment& as) {
    fmt::print(
      o,
      "{{group: {}, log_revision: {}, shard_revision: {}}}",
      as.group,
      as.log_revision,
      as.shard_revision);
    return o;
}

std::ostream&
operator<<(std::ostream& o, shard_placement_table::hosted_status s) {
    switch (s) {
    case shard_placement_table::hosted_status::receiving:
        return o << "receiving";
    case shard_placement_table::hosted_status::hosted:
        return o << "hosted";
    case shard_placement_table::hosted_status::obsolete:
        return o << "obsolete";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(
  std::ostream& o, const shard_placement_table::shard_local_state& ls) {
    fmt::print(
      o,
      "{{group: {}, log_revision: {}, status: {}, shard_revision: {}}}",
      ls.group,
      ls.log_revision,
      ls.status,
      ls.shard_revision);
    return o;
}

shard_placement_table::reconciliation_action
shard_placement_table::placement_state::get_reconciliation_action(
  std::optional<model::revision_id> expected_log_revision) const {
    if (!expected_log_revision) {
        if (_assigned) {
            return reconciliation_action::wait_for_target_update;
        }
        return reconciliation_action::remove_partition;
    }
    if (_current) {
        if (_current->log_revision < expected_log_revision) {
            return reconciliation_action::remove_partition;
        } else if (_current->log_revision > expected_log_revision) {
            return reconciliation_action::wait_for_target_update;
        } else if (_current->status == hosted_status::obsolete) {
            return reconciliation_action::remove_kvstore_state;
        }
    } else if (_is_initial_for) {
        if (_is_initial_for < expected_log_revision) {
            return reconciliation_action::remove_partition;
        } else if (_is_initial_for > expected_log_revision) {
            return reconciliation_action::wait_for_target_update;
        }
    }
    if (_assigned) {
        if (_assigned->log_revision != expected_log_revision) {
            return reconciliation_action::wait_for_target_update;
        }
        if (_next) {
            return reconciliation_action::transfer;
        }
        return reconciliation_action::create;
    } else {
        return reconciliation_action::transfer;
    }
}

bool shard_placement_table::placement_state::is_reconciled() const {
    if (!_assigned) {
        return !_current;
    }
    return _current && _current->log_revision == _assigned->log_revision
           && _current->status == hosted_status::hosted;
}

void shard_placement_table::placement_state::set_assigned(
  std::optional<shard_local_assignment> new_assigned,
  shard_placement_table::probe& probe) {
    int64_t assigned_delta = -(int64_t)_assigned.has_value();
    int64_t to_reconcile_delta = (int64_t)is_reconciled();

    _assigned = std::move(new_assigned);

    assigned_delta += _assigned.has_value();
    to_reconcile_delta -= is_reconciled();
    probe.update_assigned(assigned_delta);
    probe.update_to_reconcile(to_reconcile_delta);
}

void shard_placement_table::placement_state::set_current(
  std::optional<shard_local_state> new_current,
  shard_placement_table::probe& probe) {
    auto is_hosted = [this] {
        return _current && _current->status == hosted_status::hosted;
    };
    int64_t hosted_delta = -(int64_t)is_hosted();
    int64_t to_reconcile_delta = (int64_t)is_reconciled();

    _current = std::move(new_current);

    hosted_delta += is_hosted();
    to_reconcile_delta -= is_reconciled();
    probe.update_hosted(hosted_delta);
    probe.update_to_reconcile(to_reconcile_delta);
}

void shard_placement_table::placement_state::set_hosted_status(
  hosted_status new_status, shard_placement_table::probe& probe) {
    auto new_current = _current;
    new_current.value().status = new_status;
    set_current(std::move(new_current), probe);
}

std::ostream&
operator<<(std::ostream& o, const shard_placement_table::placement_state& ps) {
    fmt::print(
      o,
      "{{current: {}, assigned: {}, is_initial_for: {}, next: ",
      ps._current,
      ps._assigned,
      ps._is_initial_for);
    if (ps._next) {
        fmt::print(
          o, "{{s: {}, r: {}}}}}", ps._next->shard, ps._next->revision);
    } else {
        fmt::print(o, "{{nullopt}}}}");
    }
    return o;
}

namespace {

static constexpr auto kvstore_key_space
  = storage::kvstore::key_space::shard_placement;

using kvstore_key_type = shard_placement_kvstore_key_type;

bytes persistence_enabled_kvstore_key() {
    iobuf buf;
    serde::write(buf, kvstore_key_type::persistence_enabled);
    return iobuf_to_bytes(buf);
}

struct assignment_marker
  : serde::
      envelope<assignment_marker, serde::version<0>, serde::compat_version<0>> {
    model::revision_id log_revision;
    model::shard_revision_id shard_revision;

    auto serde_fields() { return std::tie(log_revision, shard_revision); }
};

bytes assignment_kvstore_key(const raft::group_id group) {
    iobuf buf;
    serde::write(buf, kvstore_key_type::assignment);
    serde::write(buf, group);
    return iobuf_to_bytes(buf);
}

struct current_state_marker
  : serde::envelope<
      current_state_marker,
      serde::version<0>,
      serde::compat_version<0>> {
    // NOTE: we need ntp in this marker because we want to be able to find and
    // clean garbage kvstore state for old groups that have already been deleted
    // from topic_table. Some of the partition kvstore state items use keys
    // based on group id and some - based on ntp, so we need both.
    model::ntp ntp;
    model::revision_id log_revision;
    model::shard_revision_id shard_revision;
    bool is_complete = false;

    auto serde_fields() {
        return std::tie(ntp, log_revision, shard_revision, is_complete);
    }
};

bytes current_state_kvstore_key(const raft::group_id group) {
    iobuf buf;
    serde::write(buf, kvstore_key_type::current_state);
    serde::write(buf, group);
    return iobuf_to_bytes(buf);
}

} // namespace

shard_placement_table::shard_placement_table(
  ss::shard_id shard, storage::kvstore& kvstore)
  : _shard(shard)
  , _kvstore(kvstore)
  , _probe(std::make_unique<probe>()) {
    if (ss::this_shard_id() == _shard) {
        _probe->setup_metrics();
    }
}

shard_placement_table::~shard_placement_table() = default;

bool shard_placement_table::is_persistence_enabled() const {
    assert_is_assignment_shard();

    if (_persistence_enabled) {
        return true;
    }

    return _kvstore.get(kvstore_key_space, persistence_enabled_kvstore_key())
      .has_value();
}

ss::future<> shard_placement_table::enable_persistence() {
    assert_is_assignment_shard();

    if (is_persistence_enabled()) {
        co_return;
    }

    vlog(clusterlog.info, "enabling table persistence...");

    auto write_locks = container().map([](shard_placement_table& local) {
        return local._persistence_lock.hold_write_lock().then(
          [](ss::rwlock::holder holder) {
              return ss::make_foreign(
                std::make_unique<ss::rwlock::holder>(std::move(holder)));
          });
    });

    co_await container().invoke_on_all([](shard_placement_table& local) {
        return local.persist_shard_local_state();
    });

    co_await _kvstore.put(
      kvstore_key_space, persistence_enabled_kvstore_key(), iobuf{});

    co_await container().invoke_on_all(
      [](shard_placement_table& local) { local._persistence_enabled = true; });

    vlog(clusterlog.debug, "persistence enabled");
}

ss::future<> shard_placement_table::persist_shard_local_state() {
    // 1. delete existing state

    chunked_vector<bytes> old_keys;
    co_await _kvstore.for_each(
      kvstore_key_space,
      [&](bytes_view key, const iobuf&) { old_keys.emplace_back(key); });

    co_await ss::max_concurrent_for_each(
      old_keys, 512, [this](const bytes& key) {
          return _kvstore.remove(kvstore_key_space, key);
      });

    // 2. persist current map

    co_await ss::max_concurrent_for_each(
      _states, 512, [this](const decltype(_states)::value_type& kv) {
          const auto& [ntp, pstate] = kv;
          auto f1 = ss::now();
          if (pstate.assigned()) {
              auto marker = assignment_marker{
                .log_revision = pstate.assigned()->log_revision,
                .shard_revision = pstate.assigned()->shard_revision,
              };
              f1 = _kvstore.put(
                kvstore_key_space,
                assignment_kvstore_key(pstate.assigned()->group),
                serde::to_iobuf(marker));
          }

          auto f2 = ss::now();
          if (pstate.current()) {
              auto marker = current_state_marker{
                .ntp = ntp,
                .log_revision = pstate.current()->log_revision,
                .shard_revision = pstate.current()->shard_revision,
                .is_complete = pstate.current()->status
                               == hosted_status::hosted,
              };
              f2 = _kvstore.put(
                kvstore_key_space,
                current_state_kvstore_key(pstate.current()->group),
                serde::to_iobuf(marker));
          }

          return ss::when_all(std::move(f1), std::move(f2)).discard_result();
      });
}

/// A struct used during initialization to merge kvstore markers recovered from
/// different shards and restore the most up-to-date shard_placement_table
/// state.
struct shard_placement_table::ntp_init_data {
    struct versioned_shard {
        std::optional<ss::shard_id> shard;
        model::shard_revision_id revision;

        void update(
          std::optional<ss::shard_id> s, model::shard_revision_id new_rev) {
            if (new_rev > revision) {
                shard = s;
                revision = new_rev;
            }
        }
    };

    raft::group_id group;
    model::revision_id log_revision;
    versioned_shard hosted;
    versioned_shard receiving;
    versioned_shard assigned;

    void update_log_revision(raft::group_id gr, model::revision_id new_rev) {
        if (new_rev > log_revision) {
            *this = ntp_init_data{};
            group = gr;
            log_revision = new_rev;
        }
    }

    void update(ss::shard_id s, const shard_local_assignment& new_assigned) {
        update_log_revision(new_assigned.group, new_assigned.log_revision);
        if (new_assigned.log_revision == log_revision) {
            assigned.update(s, new_assigned.shard_revision);
        }
    }

    void update(ss::shard_id s, const shard_local_state& new_current) {
        update_log_revision(new_current.group, new_current.log_revision);
        if (new_current.log_revision == log_revision) {
            switch (new_current.status) {
            case hosted_status::hosted:
                hosted.update(s, new_current.shard_revision);
                receiving.update(std::nullopt, new_current.shard_revision);
                break;
            case hosted_status::receiving:
                receiving.update(s, new_current.shard_revision);
                break;
            default:
                break;
            }
        }
    }
};

ss::future<std::vector<std::unique_ptr<shard_placement_table>>>
shard_placement_table::initialize_from_kvstore(
  const chunked_hash_map<raft::group_id, model::ntp>& local_group2ntp,
  const std::vector<std::unique_ptr<storage::kvstore>>& extra_kvstores) {
    assert_is_assignment_shard();

    vassert(
      is_persistence_enabled(),
      "can't initialize from kvstore, persistence hasn't been enabled yet");
    co_await container().invoke_on_all(
      [](shard_placement_table& spt) { spt._persistence_enabled = true; });

    std::vector<std::unique_ptr<shard_placement_table>> extra_spts;
    for (size_t i = 0; i < extra_kvstores.size(); ++i) {
        extra_spts.push_back(std::make_unique<shard_placement_table>(
          ss::smp::count + i, *extra_kvstores[i]));
    }

    // 1. gather kvstore markers from all shards

    auto shard2init_states = co_await container().map(
      [&local_group2ntp](shard_placement_table& spt) {
          return spt.gather_init_states(local_group2ntp);
      });
    for (const auto& spt : extra_spts) {
        shard2init_states.push_back(
          co_await spt->gather_init_states(local_group2ntp));
    }

    // 2. merge into up-to-date shard_placement_table state

    chunked_hash_map<model::ntp, ntp_init_data> ntp2init_data;
    model::shard_revision_id max_shard_revision;
    ssx::async_counter counter;
    for (ss::shard_id s = 0; s < shard2init_states.size(); ++s) {
        co_await ssx::async_for_each_counter(
          counter,
          shard2init_states[s]->begin(),
          shard2init_states[s]->end(),
          [&](const ntp2state_t::value_type& kv) {
              const auto& [ntp, state] = kv;
              auto& init_data = ntp2init_data.try_emplace(ntp).first->second;

              if (state.assigned()) {
                  init_data.update(s, state.assigned().value());
                  max_shard_revision = std::max(
                    max_shard_revision, state.assigned()->shard_revision);
              }

              if (state.current()) {
                  init_data.update(s, state.current().value());
                  max_shard_revision = std::max(
                    max_shard_revision, state.current()->shard_revision);
              }
          });
    }

    // 3. based on merged data, update in-memory state everywhere

    if (max_shard_revision != model::shard_revision_id{}) {
        _cur_shard_revision = max_shard_revision + model::shard_revision_id{1};
    }

    co_await container().invoke_on_all(
      [&ntp2init_data](shard_placement_table& spt) {
          return spt.scatter_init_data(ntp2init_data);
      });
    for (auto& spt : extra_spts) {
        co_await spt->scatter_init_data(ntp2init_data);
    }

    co_await ssx::async_for_each(
      ntp2init_data.begin(),
      ntp2init_data.end(),
      [this](const decltype(ntp2init_data)::value_type& kv) {
          const auto& [ntp, init_data] = kv;
          vlog(
            clusterlog.trace,
            "[{}] init data: group: {}, log_revision: {}, "
            "assigned: {}, hosted: {}, receiving: {}",
            ntp,
            init_data.group,
            init_data.log_revision,
            init_data.assigned.shard,
            init_data.hosted.shard,
            init_data.receiving.shard);

          if (init_data.assigned.shard) {
              auto entry = std::make_unique<entry_t>();
              entry->target = shard_placement_target(
                init_data.group,
                init_data.log_revision,
                init_data.assigned.shard.value());
              _ntp2entry.emplace(ntp, std::move(entry));
          }
      });

    co_return extra_spts;
}

ss::future<ss::foreign_ptr<std::unique_ptr<shard_placement_table::ntp2state_t>>>
shard_placement_table::gather_init_states(
  const chunked_hash_map<raft::group_id, model::ntp>& partitions) {
    chunked_vector<raft::group_id> orphan_assignments;

    co_await _kvstore.for_each(
      kvstore_key_space, [&](bytes_view key_str, const iobuf& val) {
          iobuf_parser key_parser(bytes_to_iobuf(key_str));

          auto key_type = serde::read_nested<kvstore_key_type>(key_parser, 0);
          switch (key_type) {
          default:
              return;
          case kvstore_key_type::assignment: {
              auto group = serde::read_nested<raft::group_id>(key_parser, 0);
              auto ntp_it = partitions.find(group);
              if (ntp_it == partitions.end()) {
                  vlog(
                    clusterlog.trace,
                    "shard {}: recovered orphan assigned marker, group: {}",
                    _shard,
                    group);
                  orphan_assignments.push_back(group);
              } else {
                  auto marker = serde::from_iobuf<assignment_marker>(
                    val.copy());
                  vlog(
                    clusterlog.trace,
                    "[{}] shard {}: recovered assigned marker, lr: {} sr: {}",
                    ntp_it->second,
                    _shard,
                    marker.log_revision,
                    marker.shard_revision);

                  _states[ntp_it->second].set_assigned(
                    shard_local_assignment{
                      .group = group,
                      .log_revision = marker.log_revision,
                      .shard_revision = marker.shard_revision},
                    *_probe);
              }
              break;
          }
          case kvstore_key_type::current_state: {
              auto group = serde::read_nested<raft::group_id>(key_parser, 0);
              auto marker = serde::from_iobuf<current_state_marker>(val.copy());
              vlog(
                clusterlog.trace,
                "[{}] shard {}: recovered cur state marker, lr: {} sr: {} "
                "complete: {}",
                marker.ntp,
                _shard,
                marker.log_revision,
                marker.shard_revision,
                marker.is_complete);

              auto& state = _states[marker.ntp];
              if (state.current()) {
                  throw std::runtime_error(fmt_with_ctx(
                    ssx::sformat,
                    "duplicate ntp {} in kvstore map on shard {}",
                    marker.ntp,
                    _shard));
              }
              state.set_current(
                shard_local_state(
                  group,
                  marker.log_revision,
                  marker.is_complete ? hosted_status::hosted
                                     : hosted_status::receiving,
                  marker.shard_revision),
                *_probe);
              break;
          }
          }
      });

    co_await ss::max_concurrent_for_each(
      orphan_assignments.begin(),
      orphan_assignments.end(),
      512,
      [this](raft::group_id group) {
          return _kvstore.remove(
            kvstore_key_space, assignment_kvstore_key(group));
      });

    co_return ss::make_foreign(std::make_unique<ntp2state_t>(_states));
}

ss::future<> shard_placement_table::scatter_init_data(
  const chunked_hash_map<model::ntp, shard_placement_table::ntp_init_data>&
    ntp2init_data) {
    return ss::max_concurrent_for_each(
      ntp2init_data.begin(),
      ntp2init_data.end(),
      512,
      [this](const std::pair<model::ntp, ntp_init_data>& kv) {
          const auto& [ntp, init_data] = kv;
          auto it = _states.find(ntp);
          if (it == _states.end()) {
              return ss::now();
          }
          auto& state = it->second;

          if (state.current()) {
              if (_shard == init_data.hosted.shard) {
                  if (
                    init_data.receiving.shard
                    && init_data.receiving.shard < ss::smp::count) {
                      state._next = placement_state::versioned_shard{
                        .shard = init_data.receiving.shard.value(),
                        .revision = init_data.receiving.revision};
                  }
              } else if (
                _shard != init_data.receiving.shard || !init_data.hosted.shard
                || _shard >= ss::smp::count) {
                  state.set_hosted_status(hosted_status::obsolete, *_probe);
              }
          }

          ss::future<> fut = ss::now();
          if (state.assigned()) {
              if (_shard != init_data.assigned.shard) {
                  fut = _kvstore.remove(
                    kvstore_key_space,
                    assignment_kvstore_key(state.assigned()->group));
                  state.set_assigned(std::nullopt, *_probe);
              } else if (!init_data.hosted.shard) {
                  state._is_initial_for = init_data.log_revision;
              }

              if (_shard >= ss::smp::count) {
                  // mark states on extra shards as ready to transfer
                  state.set_assigned(std::nullopt, *_probe);
              }
          }

          if (state.is_empty()) {
              _states.erase(it);
          } else {
              vlog(
                clusterlog.info,
                "[{}] shard {}: recovered placement state: {}",
                ntp,
                _shard,
                state);
          }

          return fut;
      });
}

ss::future<> shard_placement_table::initialize_from_topic_table(
  ss::sharded<topic_table>& topics, model::node_id self) {
    assert_is_assignment_shard();

    vassert(
      !is_persistence_enabled(),
      "can't initialize from topic_table, persistence has already been "
      "enabled");

    co_await container().invoke_on_all(
      [&topics, self](shard_placement_table& spt) {
          return spt.do_initialize_from_topic_table(topics.local(), self);
      });

    if (!_ntp2entry.empty()) {
        _cur_shard_revision += 1;
    }
}

ss::future<> shard_placement_table::do_initialize_from_topic_table(
  const topic_table& topics, model::node_id self) {
    vassert(
      ss::this_shard_id() == _shard,
      "trying to init table for extra shard {} from topic_table",
      _shard);

    // We expect topic_table to remain unchanged throughout the loop because the
    // method is supposed to be called after local controller replay is finished
    // but before we start getting new controller updates from the leader.
    auto tt_version = topics.topics_map_revision();
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
              auto& [_, p_as] = p;
              model::ntp ntp{ns_tp.ns, ns_tp.tp, p_as.id};
              auto replicas_view = topics.get_replicas_view(ntp, md_item, p_as);
              auto target = placement_target_on_node(replicas_view, self);
              if (!target) {
                  return;
              }

              if (ss::this_shard_id() == assignment_shard_id) {
                  auto it = _ntp2entry.emplace(ntp, std::make_unique<entry_t>())
                              .first;
                  it->second->target = target.value();
              }

              // We add an initial hosted marker for the partition on the shard
              // from the original replica set (even in the case of cross-shard
              // move). The reason for this is that if there is an ongoing
              // cross-shard move, we can't be sure if it was done before the
              // previous shutdown or not, so during reconciliation we'll first
              // look for kvstore state on the original shard, and, if there is
              // none (meaning that the update was finished previously), use the
              // state on the destination shard.

              auto orig_shard = find_shard_on_node(
                replicas_view.orig_replicas(), self);

              if (ss::this_shard_id() == target->shard) {
                  vlog(
                    clusterlog.info,
                    "expecting partition {} with log revision {} on this shard "
                    "(original shard {})",
                    ntp,
                    target->log_revision,
                    orig_shard);
              }

              auto placement = placement_state();
              auto assigned = shard_local_assignment{
                .group = target->group,
                .log_revision = target->log_revision,
                .shard_revision = _cur_shard_revision};

              if (orig_shard && target->shard != orig_shard) {
                  // cross-shard transfer, orig_shard gets the hosted marker
                  if (ss::this_shard_id() == orig_shard) {
                      placement.set_current(
                        shard_local_state(assigned, hosted_status::hosted),
                        *_probe);
                      _states.emplace(ntp, placement);
                  } else if (ss::this_shard_id() == target->shard) {
                      placement.set_assigned(assigned, *_probe);
                      _states.emplace(ntp, placement);
                  }
              } else if (ss::this_shard_id() == target->shard) {
                  // in other cases target shard gets the hosted marker
                  placement.set_current(
                    shard_local_state(assigned, hosted_status::hosted),
                    *_probe);
                  placement.set_assigned(assigned, *_probe);
                  _states.emplace(ntp, placement);
              }
          });
    }
}

ss::future<> shard_placement_table::set_target(
  const model::ntp& ntp,
  std::optional<shard_placement_target> target,
  shard_callback_t shard_callback) {
    assert_is_assignment_shard();

    if (target) {
        vassert(
          target->shard < ss::smp::count, "[{}] bad target: {}", ntp, target);
    }

    // ensure that there is no concurrent enable_persistence() call
    auto persistence_lock_holder = co_await _persistence_lock.hold_read_lock();

    if (!target && !_ntp2entry.contains(ntp)) {
        co_return;
    }

    auto entry_it = _ntp2entry.try_emplace(ntp).first;
    if (!entry_it->second) {
        entry_it->second = std::make_unique<entry_t>();
    }
    entry_t& entry = *entry_it->second;

    auto release_units = ss::defer(
      [&, units = co_await entry.mtx.get_units()]() mutable {
          bool had_waiters = entry.mtx.waiters() > 0;
          units.return_all();
          if (!had_waiters && !entry.target) {
              _ntp2entry.erase(ntp);
          }
      });

    const auto prev_target = entry.target;
    if (prev_target == target) {
        vlog(
          clusterlog.trace,
          "[{}] modify target no-op, current: {}",
          ntp,
          prev_target);
        co_return;
    }

    const model::shard_revision_id shard_rev = _cur_shard_revision;
    _cur_shard_revision += 1;

    vlog(
      clusterlog.trace,
      "[{}] modify target: {} -> {}, shard_rev: {}",
      ntp,
      prev_target,
      target,
      shard_rev);

    // 1. Persist the new target in kvstore

    if (_persistence_enabled) {
        if (target) {
            co_await container().invoke_on(
              target->shard,
              [&target, shard_rev, &ntp](shard_placement_table& other) {
                  auto marker_buf = serde::to_iobuf(assignment_marker{
                    .log_revision = target->log_revision,
                    .shard_revision = shard_rev,
                  });
                  vlog(
                    clusterlog.trace,
                    "[{}] put assigned marker, lr: {} sr: {}",
                    ntp,
                    target->log_revision,
                    shard_rev);
                  return other._kvstore.put(
                    kvstore_key_space,
                    assignment_kvstore_key(target->group),
                    std::move(marker_buf));
              });
        } else if (prev_target.value().shard < ss::smp::count) {
            co_await container().invoke_on(
              prev_target.value().shard,
              [group = prev_target->group, &ntp](shard_placement_table& other) {
                  vlog(clusterlog.trace, "[{}] remove assigned marker", ntp);
                  return other._kvstore.remove(
                    kvstore_key_space, assignment_kvstore_key(group));
              });
        }
    }

    // 2. At this point we've successfully committed the new target to
    // persistent storage. Update in-memory state.

    entry.target = target;

    if (target) {
        const bool is_initial
          = (!prev_target || prev_target->log_revision != target->log_revision);
        shard_local_assignment as{
          .group = target->group,
          .log_revision = target->log_revision,
          .shard_revision = shard_rev,
        };
        co_await container().invoke_on(
          target->shard,
          [&ntp, &as, is_initial, shard_callback](shard_placement_table& spt) {
              auto& state = spt._states.try_emplace(ntp).first->second;

              vlog(
                clusterlog.trace,
                "[{}] setting assigned on this shard to: {} (was: {}), "
                "is_initial: {}",
                ntp,
                as,
                state.assigned(),
                is_initial);

              state.set_assigned(as, *spt._probe);
              if (is_initial) {
                  state._is_initial_for = as.log_revision;
              }

              // Notify the caller that something has changed on this shard.
              shard_callback(ntp);
          });
    }

    if (
      prev_target && prev_target->shard < ss::smp::count
      && (!target || target->shard != prev_target->shard)) {
        co_await container().invoke_on(
          prev_target->shard,
          [&ntp, shard_callback](shard_placement_table& other) {
              auto it = other._states.find(ntp);
              if (it == other._states.end() || !it->second.assigned()) {
                  return;
              }

              vlog(
                clusterlog.trace,
                "[{}] removing assigned on this shard (was: {})",
                ntp,
                it->second.assigned());

              it->second.set_assigned(std::nullopt, *other._probe);
              if (it->second.is_empty()) {
                  // We are on a shard that was previously a target, but didn't
                  // get to starting the transfer.
                  other._states.erase(it);
              }

              // Notify the caller that something has changed on this shard.
              shard_callback(ntp);
          });
    }

    // 3. Lastly, remove obsolete kvstore marker

    if (
      _persistence_enabled && prev_target && prev_target->shard < ss::smp::count
      && (!target || target->shard != prev_target->shard)) {
        co_await container().invoke_on(
          prev_target->shard,
          [group = prev_target->group, &ntp](shard_placement_table& other) {
              vlog(
                clusterlog.trace, "[{}] remove obsolete assigned marker", ntp);
              return other._kvstore
                .remove(kvstore_key_space, assignment_kvstore_key(group))
                .handle_exception([group](std::exception_ptr ex) {
                    // Ignore the exception because the update has already been
                    // committed. Obsolete marker will be deleted after the next
                    // restart.
                    vlog(
                      clusterlog.debug,
                      "failed to remove assignment marker for group {}: {}",
                      group,
                      ex);
                });
          });
    }
}

std::optional<shard_placement_table::placement_state>
shard_placement_table::state_on_this_shard(const model::ntp& ntp) const {
    auto it = _states.find(ntp);
    if (it != _states.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<shard_placement_target>
shard_placement_table::get_target(const model::ntp& ntp) const {
    assert_is_assignment_shard();
    auto it = _ntp2entry.find(ntp);
    if (it != _ntp2entry.end()) {
        return it->second->target;
    }
    return std::nullopt;
}

ss::future<> shard_placement_table::for_each_ntp(
  ss::noncopyable_function<
    void(const model::ntp&, const shard_placement_target&)> func) const {
    assert_is_assignment_shard();
    return ssx::async_for_each(
      _ntp2entry.begin(),
      _ntp2entry.end(),
      [&func](const decltype(_ntp2entry)::value_type& kv) {
          const auto& [ntp, entry] = kv;
          vassert(
            entry && entry->target && entry->mtx.ready(),
            "[{}]: unexpected concurrent set_target()",
            ntp);
          func(ntp, *entry->target);
      });
}

ss::future<std::error_code> shard_placement_table::prepare_create(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    // ensure that there is no concurrent enable_persistence() call
    auto persistence_lock_holder = co_await _persistence_lock.hold_read_lock();

    auto state_it = _states.find(ntp);
    if (state_it == _states.end()) {
        // assignments got updated while we were waiting for the lock
        co_return errc::waiting_for_shard_placement_update;
    }
    auto& state = state_it->second;

    if (state.assigned()->log_revision != expected_log_rev) {
        // assignments got updated while we were waiting for the lock
        co_return errc::waiting_for_shard_placement_update;
    }

    if (state.current() && state.current()->log_revision != expected_log_rev) {
        // wait until partition with obsolete log revision is removed
        co_return errc::waiting_for_reconfiguration_finish;
    }

    // copy assigned as it may change while we are updating kvstore
    auto assigned = *state.assigned();

    if (!state.current()) {
        if (state._is_initial_for == expected_log_rev) {
            if (_persistence_enabled) {
                auto marker_buf = serde::to_iobuf(current_state_marker{
                  .ntp = ntp,
                  .log_revision = expected_log_rev,
                  .shard_revision = assigned.shard_revision,
                  .is_complete = true,
                });
                vlog(
                  clusterlog.trace,
                  "[{}] put initial cur state marker, lr: {} sr: {}",
                  ntp,
                  expected_log_rev,
                  assigned.shard_revision);
                co_await _kvstore.put(
                  kvstore_key_space,
                  current_state_kvstore_key(assigned.group),
                  std::move(marker_buf));
            }

            state.set_current(
              shard_local_state(assigned, hosted_status::hosted), *_probe);
            if (state._is_initial_for == expected_log_rev) {
                // could have changed while we were updating kvstore.
                state._is_initial_for = std::nullopt;
            }
        } else {
            // x-shard transfer hasn't started yet, wait for it.
            co_return errc::waiting_for_partition_shutdown;
        }
    }

    if (state.current()->status != hosted_status::hosted) {
        // x-shard transfer is in progress, wait for it to end.
        co_return errc::waiting_for_partition_shutdown;
    }

    // ready to create
    co_return errc::success;
}

ss::future<shard_placement_table::prepare_transfer_info>
shard_placement_table::prepare_transfer(
  const model::ntp& ntp,
  model::revision_id expected_log_rev,
  ss::sharded<shard_placement_table>& sharded_spt) {
    // ensure that there is no concurrent enable_persistence() call
    auto persistence_lock_holder = co_await _persistence_lock.hold_read_lock();

    auto state_it = _states.find(ntp);
    vassert(state_it != _states.end(), "[{}] expected state", ntp);
    auto& state = state_it->second;

    prepare_transfer_info ret;

    if (state.current()) {
        vassert(
          state.current()->log_revision >= expected_log_rev,
          "[{}] unexpected current: {} (expected log revision: {})",
          ntp,
          state.current(),
          expected_log_rev);

        if (state.current()->log_revision > expected_log_rev) {
            // New log revision transferred from another shard, but we don't
            // know about it yet. Wait for the assignment update.
            ret.source_error = errc::waiting_for_shard_placement_update;
            co_return ret;
        }

        if (state.current()->status == hosted_status::receiving) {
            // This shard needs to transfer partition state somewhere else, but
            // haven't yet received it itself. Wait for it.
            ret.source_error = errc::waiting_for_partition_shutdown;
            co_return ret;
        }

        vassert(
          state.current()->status == hosted_status::hosted,
          "[{}] unexpected current: {} (expected hosted status)",
          ntp,
          state.current());
    } else {
        vassert(
          state._is_initial_for >= expected_log_rev,
          "[{}] unexpected is_initial_for: {} (expected log revision: {})",
          ntp,
          state._is_initial_for,
          expected_log_rev);

        if (state._is_initial_for > expected_log_rev) {
            ret.source_error = errc::waiting_for_shard_placement_update;
            co_return ret;
        }
    }

    const bool is_initial = !state.current();

    if (state._next) {
        ret.destination = state._next->shard;
        // TODO: check that _next is still waiting for our transfer
    } else {
        if (state.assigned()) {
            ret.source_error = errc::waiting_for_shard_placement_update;
            co_return ret;
        }

        auto maybe_dest = co_await sharded_spt.invoke_on(
          assignment_shard_id,
          [&ntp, expected_log_rev](shard_placement_table& spt) {
              auto it = spt._ntp2entry.find(ntp);
              if (it == spt._ntp2entry.end()) {
                  return std::optional<ss::shard_id>{};
              }
              const auto& target = it->second->target;
              if (target && target->log_revision == expected_log_rev) {
                  return std::optional{target->shard};
              }
              return std::optional<ss::shard_id>{};
          });
        if (!maybe_dest || maybe_dest == _shard) {
            // Inconsistent state, likely because we are in the middle of
            // shard_placement_table update, wait for it to finish.
            ret.source_error = errc::waiting_for_shard_placement_update;
            co_return ret;
        }
        ret.destination = maybe_dest.value();

        // check if destination is ready
        model::shard_revision_id shard_rev;
        co_await sharded_spt.invoke_on(
          ret.destination.value(),
          [&ntp, &shard_rev, &ret, expected_log_rev, is_initial](
            shard_placement_table& dest) {
              auto dest_it = dest._states.find(ntp);
              if (
                dest_it == dest._states.end() || !dest_it->second.assigned()
                || dest_it->second.assigned()->log_revision
                     != expected_log_rev) {
                  // We are in the middle of shard_placement_table update, and
                  // the destination shard doesn't yet know that it is the
                  // destination. Wait for the update to finish.
                  ret.dest_error = errc::waiting_for_shard_placement_update;
                  return ss::now();
              }
              auto& dest_state = dest_it->second;

              if (dest_state.current() || dest_state._is_initial_for) {
                  // Wait for the destination to clear obsolete data
                  ret.dest_error = errc::waiting_for_partition_shutdown;
                  return ss::now();
              }

              if (is_initial) {
                  dest_state._is_initial_for = expected_log_rev;
                  return ss::now();
              }

              // at this point we commit to the transfer on the
              // destination shard
              shard_rev = dest_state.assigned().value().shard_revision;
              dest_state.set_current(
                shard_local_state(
                  dest_state.assigned().value(), hosted_status::receiving),
                *dest._probe);

              if (dest._persistence_enabled) {
                  auto marker_buf = serde::to_iobuf(current_state_marker{
                    .ntp = ntp,
                    .log_revision = expected_log_rev,
                    .shard_revision = dest_state.current()->shard_revision,
                    .is_complete = false,
                  });
                  vlog(
                    clusterlog.trace,
                    "[{}] put receiving cur state marker, lr: {} sr: {}",
                    ntp,
                    expected_log_rev,
                    dest_state.current()->shard_revision);
                  return dest._kvstore
                    .put(
                      kvstore_key_space,
                      current_state_kvstore_key(dest_state.current()->group),
                      std::move(marker_buf))
                    .handle_exception(
                      [&dest, &dest_state](std::exception_ptr ex) {
                          // "unlock" destination in case of kvstore errors so
                          // that we can retry later.
                          dest_state.set_current(std::nullopt, *dest._probe);
                          return ss::make_exception_future(std::move(ex));
                      });
              } else {
                  return ss::now();
              }
          });

        if (ret.dest_error != errc::success) {
            co_return ret;
        }

        if (is_initial) {
            if (state._is_initial_for == expected_log_rev) {
                state._is_initial_for = std::nullopt;
                if (state.is_empty()) {
                    _states.erase(ntp);
                }
            }

            ret.is_finished = true;

            co_return ret;
        }

        // at this point we commit to the transfer on the source shard
        state._next = placement_state::versioned_shard{
          .shard = ret.destination.value(),
          .revision = shard_rev,
        };
    }

    co_return ret;
}

ss::future<> shard_placement_table::finish_transfer(
  const model::ntp& ntp,
  model::revision_id expected_log_rev,
  ss::sharded<shard_placement_table>& sharded_spt,
  shard_callback_t shard_callback) {
    // ensure that there is no concurrent enable_persistence() call
    auto persistence_lock_holder = co_await _persistence_lock.hold_read_lock();

    auto it = _states.find(ntp);
    vassert(it != _states.end(), "[{}] expected state", ntp);
    auto& state = it->second;

    vassert(state._next, "[{}] expected _next, state: {}", ntp, state);
    co_await sharded_spt.invoke_on(
      state._next->shard,
      [&ntp, expected_shard_rev = state._next->revision, shard_callback](
        shard_placement_table& dest) {
          auto dest_it = dest._states.find(ntp);
          vassert(dest_it != dest._states.end(), "[{}] expected state", ntp);
          auto& dest_state = dest_it->second;

          vassert(
            dest_state.current()
              && dest_state.current()->shard_revision == expected_shard_rev
              && dest_state.current()->status == hosted_status::receiving,
            "[{}] unexpected current: {} (expected shard revision: {})",
            ntp,
            dest_state.current(),
            expected_shard_rev);

          ss::future<> fut = ss::now();
          if (dest._persistence_enabled) {
              auto marker_buf = serde::to_iobuf(current_state_marker{
                .ntp = ntp,
                .log_revision = dest_state.current()->log_revision,
                .shard_revision = dest_state.current()->shard_revision,
                .is_complete = true,
              });
              vlog(
                clusterlog.trace,
                "[{}] put transferred cur state marker, lr: {} sr: {}",
                ntp,
                dest_state.current()->log_revision,
                dest_state.current()->shard_revision);
              fut = dest._kvstore.put(
                kvstore_key_space,
                current_state_kvstore_key(dest_state.current()->group),
                std::move(marker_buf));
          }

          return std::move(fut).then(
            [&dest, &ntp, &dest_state, shard_callback] {
                dest_state.set_hosted_status(
                  hosted_status::hosted, *dest._probe);
                vlog(
                  clusterlog.trace,
                  "[{}] finished transfer on destination, placement: {}",
                  ntp,
                  dest_state);
                shard_callback(ntp);
            });
      });
    state._next = std::nullopt;

    if (state.current() && state.current()->log_revision == expected_log_rev) {
        state.set_hosted_status(hosted_status::obsolete, *_probe);
    }

    if (state._is_initial_for == expected_log_rev) {
        state._is_initial_for = std::nullopt;
    }

    if (state.is_empty()) {
        _states.erase(ntp);
    }
}

ss::future<std::error_code> shard_placement_table::prepare_delete(
  const model::ntp& ntp, model::revision_id cmd_revision) {
    // ensure that there is no concurrent enable_persistence() call
    auto persistence_lock_holder = co_await _persistence_lock.hold_read_lock();

    auto it = _states.find(ntp);
    vassert(it != _states.end(), "[{}] expected state", ntp);
    auto& state = it->second;

    if (state.current()) {
        if (state.current()->log_revision >= cmd_revision) {
            // New log revision transferred from another shard, but we didn't
            // expect it. Wait for the update.
            co_return errc::waiting_for_shard_placement_update;
        }

        if (state.current()->status == hosted_status::receiving) {
            // If transfer to this shard is still in progress, we'll wait for
            // the source shard to finish or cancel it before deleting.
            co_return errc::waiting_for_partition_shutdown;
        }

        state.set_hosted_status(hosted_status::obsolete, *_probe);
    }

    if (state._next) {
        // notify destination shard that the transfer won't finish
        co_await container().invoke_on(
          state._next->shard,
          [&ntp, expected_shard_rev = state._next->revision](
            shard_placement_table& dest) {
              auto it = dest._states.find(ntp);
              if (
                it != dest._states.end() && it->second.current()
                && it->second.current()->shard_revision == expected_shard_rev
                && it->second.current()->status == hosted_status::receiving) {
                  it->second.set_hosted_status(
                    hosted_status::obsolete, *dest._probe);
              }

              // TODO: notify reconciliation fiber
          });

        state._next = std::nullopt;
    }

    if (state._is_initial_for && state._is_initial_for < cmd_revision) {
        state._is_initial_for = std::nullopt;
        if (state.is_empty()) {
            _states.erase(it);
        }
    }

    co_return errc::success;
}

ss::future<> shard_placement_table::finish_delete(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    // ensure that there is no concurrent enable_persistence() call
    auto persistence_lock_holder = co_await _persistence_lock.hold_read_lock();

    auto it = _states.find(ntp);
    vassert(it != _states.end(), "[{}] expected state", ntp);
    auto& state = it->second;
    vassert(
      state.current() && state.current()->log_revision == expected_log_rev,
      "[{}] unexpected current: {} (expected log revision: {})",
      ntp,
      state.current(),
      expected_log_rev);

    co_await do_delete(ntp, state, persistence_lock_holder);
}

ss::future<> shard_placement_table::do_delete(
  const model::ntp& ntp,
  placement_state& state,
  ss::rwlock::holder& /*persistence_lock_holder*/) {
    state._next = std::nullopt;

    if (state.current()) {
        if (_persistence_enabled) {
            vlog(clusterlog.trace, "[{}] remove cur state marker", ntp);
            co_await _kvstore.remove(
              kvstore_key_space,
              current_state_kvstore_key(state.current()->group));
        }
        state.set_current(std::nullopt, *_probe);
    }

    if (state.is_empty()) {
        _states.erase(ntp);
    }
    co_return;
}

void shard_placement_table::assert_is_assignment_shard() const {
    vassert(
      ss::this_shard_id() == _shard
        && ss::this_shard_id() == assignment_shard_id,
      "method can only be invoked on shard {} (table for shard: {})",
      assignment_shard_id,
      _shard);
}

} // namespace cluster
