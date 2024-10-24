// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/recovery_scheduler.h"

#include "base/vassert.h"
#include "metrics/prometheus_sanitize.h"
#include "raft/consensus.h"
#include "raft/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>

#include <fmt/ranges.h>

namespace raft {

follower_recovery_state::follower_recovery_state(
  recovery_scheduler_base& scheduler,
  consensus& parent,
  model::offset our_last,
  model::offset leader_last,
  bool already_recovering)
  : _parent(&parent)
  , _is_active(already_recovering)
  , _our_last_offset(our_last)
  , _leader_last_offset(leader_last)
  , _scheduler(&scheduler) {
    _scheduler->add(*this);
}

void follower_recovery_state::force_active() {
    if (_scheduler && !_is_active) {
        _scheduler->activate(*this);
    }
}

void follower_recovery_state::update_progress(
  model::offset our_last, model::offset leader_last) {
    auto prev_pending = pending_offset_count();

    vlog(
      raftlog.trace,
      "follower_recovery_state {} new offsets: our: {}, leader {}",
      *this,
      our_last,
      leader_last);

    _our_last_offset = our_last;

    // Leader last offset can go backwards in the following cases:
    // 1. leadership change
    // 2. reordered request
    // 3. request from pre 23.3 redpanda (where we can't always determine the
    // last offset and it will be model::offset{}).
    //
    // Of these only 1 is valid but for the purposes of progress tracking it
    // should be okay to just take the max.
    _leader_last_offset = std::max(_leader_last_offset, leader_last);

    if (_scheduler) {
        _scheduler->_offsets_pending += pending_offset_count() - prev_pending;

        if (!_is_active) {
            // This partition might have just received an update from a new
            // leader after being leaderless for a while, therefore we must
            // ensure that there will be a tick in the near future.
            _scheduler->request_tick();
        }
    }
}

void follower_recovery_state::yield() {
    if (_scheduler && _is_active) {
        _scheduler->yield(*this);
    }
}

const model::ntp& follower_recovery_state::ntp() const {
    return _parent->ntp();
}

int64_t follower_recovery_state::pending_offset_count() const {
    // Follower last offset can be greater if it hasn't yet truncated its
    // log in response to leader requests.

    if (_leader_last_offset > _our_last_offset) {
        if (_our_last_offset() >= 0) {
            return _leader_last_offset - _our_last_offset;
        } else {
            return _leader_last_offset + model::offset{1};
        }
    } else {
        return model::offset{0};
    }
}

follower_recovery_state::~follower_recovery_state() noexcept {
    if (_scheduler) {
        _scheduler->remove(*this);
        _scheduler = nullptr;
    }
}

std::ostream& operator<<(std::ostream& o, const follower_recovery_state& frs) {
    fmt::print(
      o,
      "{{ntp: {} is_active: {}, our_last_offset: {} leader_last_offset: {}}}",
      frs.ntp(),
      frs._is_active,
      frs._our_last_offset,
      frs._leader_last_offset);
    return o;
}

recovery_scheduler_base::recovery_scheduler_base(
  config::binding<size_t> max_active)
  : _max_active(std::move(max_active)) {
    _max_active.watch([this] {
        if (_active.size() < _max_active()) {
            request_tick();
        }
    });

    setup_metrics();
}

recovery_scheduler_base::~recovery_scheduler_base() {
    auto disposer = [](follower_recovery_state* frs) {
        frs->_scheduler = nullptr;
    };
    _active.clear_and_dispose(disposer);
    _pending.clear_and_dispose(disposer);
}

static bool is_internal(const model::ntp& ntp) {
    return ntp.ns == model::redpanda_ns
           || ntp.ns == model::kafka_internal_namespace
           || (ntp.ns == model::kafka_namespace && ntp.tp.topic == model::kafka_consumer_offsets_topic);
}

void recovery_scheduler_base::add(follower_recovery_state& frs) {
    if (frs.ntp() == model::controller_ntp) {
        frs._is_active = true;
    } else if (is_internal(frs.ntp()) && _active.size() < _max_active()) {
        frs._is_active = true;
    } else if (
      frs._our_last_offset == model::offset{}
      && frs._leader_last_offset == model::offset{0}) {
        // Special case for newly created partitions: we probably missed the
        // first append_entries with the configuration batch because the
        // partition hasn't yet been created.
        //
        // Allow "recovery" (which will hopefully be short and sweet)
        // immediately. This is not necessary for correctness, but we do it to
        // avoid a spurious spike in stats when the new topic is created.
        frs._is_active = true;
    }

    if (frs._is_active) {
        _active.push_back(frs);
    } else {
        _pending.push_back(frs);
        request_tick();
    }

    _offsets_pending += frs.pending_offset_count();
    vlog(raftlog.trace, "frs {}: add", frs);
}

void recovery_scheduler_base::activate(follower_recovery_state& st) {
    vassert(!st._is_active, "ntp {}: recovery already active!", st.ntp());
    _pending.erase(state_list::s_iterator_to(st));
    st._is_active = true;
    _active.push_back(st);
    vlog(raftlog.trace, "frs {}: activate", st);
}

void recovery_scheduler_base::yield(follower_recovery_state& st) {
    vassert(st._is_active, "ntp {}: recovery not active!", st.ntp());
    _active.erase(state_list::s_iterator_to(st));
    st._is_active = false;
    _pending.push_back(st);
    request_tick();
    vlog(raftlog.trace, "frs {}: yield", st);
}

void recovery_scheduler_base::remove(follower_recovery_state& frs) {
    auto it = recovery_scheduler_base::state_list::s_iterator_to(frs);
    if (frs._is_active) {
        _active.erase(it);
        request_tick();
    } else {
        _pending.erase(it);
    }
    _offsets_pending -= frs.pending_offset_count();
    vlog(raftlog.trace, "frs {}: remove", frs);
}

void recovery_scheduler_base::activate_some() {
    if (_active.size() >= _max_active() || _pending.empty()) {
        return;
    }

    struct item {
        model::node_id leader_id;
        follower_recovery_state* frs = nullptr;
    };

    // 1. Split the pending queue into sub-queues based on the priority class.
    // A sub-queue for each class will be scheduled separately.

    absl::btree_map<int, ss::chunked_fifo<item>> priority2items;
    size_t leaderless_count = 0;
    for (auto& frs : _pending) {
        auto leader_id = frs._parent->get_leader_id();
        if (!leader_id) {
            // don't schedule leaderless partitions at all, as it is unclear if
            // they will be able to make progress.
            leaderless_count += 1;
            continue;
        }

        // ordinary user partition needing recovery
        int priority = 0;

        if (is_internal(frs.ntp())) {
            // internal partitions get higher priority as we want them fully
            // operational before waiting for all the user data to be recovered.
            priority = -1;
        } else {
            auto config = frs._parent->config();
            bool is_learner = !config.contains(frs._parent->self())
                              || !config.is_voter(frs._parent->self());
            if (is_learner) {
                // learners (i.e. partitions on the nodes where they are being
                // moved to) get lower priority as we want partitions to regain
                // their full replication factor first (we typically wait for
                // learners to fully recover before removing other replicas and
                // thus they don't conribute to the number of under-replicated
                // partitions)
                priority = 1;
            }
        }

        priority2items[priority].push_back(
          item{.leader_id = *leader_id, .frs = &frs});
    }

    // 2. Schedule each sub-queue fairly - by trying to allocate roughly equal
    // number of slots to each peer node.

    absl::flat_hash_map<model::node_id, size_t> node2active_count;
    for (const auto& frs : _active) {
        auto leader_id = frs._parent->get_leader_id();
        if (!leader_id) {
            leaderless_count += 1;
            continue;
        }
        node2active_count[*leader_id] += 1;
    }

    auto fairly_schedule = [&](const ss::chunked_fifo<item>& items) {
        absl::flat_hash_map<
          model::node_id,
          ss::chunked_fifo<follower_recovery_state*>>
          node2items;
        for (const auto& item : items) {
            node2items[item.leader_id].push_back(item.frs);
        }

        struct node {
            model::node_id id;
            size_t active_count = 0;

            bool operator<(const node& other) const {
                return active_count > other.active_count;
            }
        };

        std::priority_queue<node> node_queue;
        for (const auto& [id, items] : node2items) {
            node_queue.push(
              node{.id = id, .active_count = node2active_count[id]});
        }

        while (!node_queue.empty() && _active.size() < _max_active()) {
            auto cur = node_queue.top();
            node_queue.pop();
            auto& items = node2items[cur.id];
            vassert(
              !items.empty(), "items for a node in the queue cannot be empty");
            activate(*items.front());
            items.pop_front();
            auto& active = node2active_count[cur.id];
            active += 1;

            if (!items.empty()) {
                node_queue.push(node{.id = cur.id, .active_count = active});
            }
        }
    };

    vlog(
      raftlog.debug,
      "leaderless count: {}; active counts by node before scheduling: {}",
      leaderless_count,
      node2active_count);

    for (const auto& [prio, items] : priority2items) {
        if (_active.size() >= _max_active()) {
            break;
        }

        fairly_schedule(items);

        vlog(
          raftlog.debug,
          "active counts by node after scheduling priority class {}: {}",
          prio,
          node2active_count);
    }
}

void recovery_scheduler_base::tick() {
    auto gate_guard = _gate.hold();

    size_t prev_active = _active.size();

    activate_some();

    // recalculate _offsets_pending just in case
    _offsets_pending = 0;
    for (const auto& frs : _active) {
        _offsets_pending += frs.pending_offset_count();
    }
    for (const auto& frs : _pending) {
        _offsets_pending += frs.pending_offset_count();
    }

    vlog(
      raftlog.debug,
      "after tick: "
      "active count: {} -> {}, pending count: {}, offsets pending: {}",
      prev_active,
      _active.size(),
      _pending.size(),
      _offsets_pending);
}

recovery_status recovery_scheduler_base::get_status() {
    return {
      .partitions_to_recover = _active.size() + _pending.size(),
      .partitions_active = _active.size(),
      .offsets_pending = uint64_t(std::max(_offsets_pending, int64_t(0))),
    };
}

void recovery_scheduler_base::setup_metrics() {
    namespace sm = ss::metrics;

    auto setup = [this]<typename MetricDef>(
                   const std::vector<sm::label>& aggregate_labels) {
        std::vector<MetricDef> defs;
        defs.emplace_back(
          sm::make_gauge(
            "partitions_to_recover",
            [this] { return _active.size() + _pending.size(); },
            sm::description("Number of partition replicas that have to "
                            "recover for this node."))
            .aggregate(aggregate_labels));
        defs.emplace_back(
          sm::make_gauge(
            "partitions_active",
            [this] { return _active.size(); },
            sm::description("Number of partition replicas are currently "
                            "recovering on this node."))
            .aggregate(aggregate_labels));
        defs.emplace_back(
          sm::make_gauge(
            "offsets_pending",
            [this] { return _offsets_pending; },
            sm::description("Sum of offsets that partitions on this node "
                            "need to recover."))
            .aggregate(aggregate_labels));

        return defs;
    };

    auto group_name = prometheus_sanitize::metrics_name("raft:recovery");

    if (!config::shard_local_cfg().disable_metrics()) {
        _metrics.add_group(
          group_name,
          setup.template operator()<ss::metrics::impl::metric_definition_impl>(
            {}),
          {},
          {sm::shard_label});
    }

    if (!config::shard_local_cfg().disable_public_metrics()) {
        _public_metrics.add_group(
          group_name,
          setup.template operator()<ss::metrics::metric_definition>(
            {sm::shard_label}));
    }
}

} // namespace raft
