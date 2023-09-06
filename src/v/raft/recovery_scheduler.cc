// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/recovery_scheduler.h"

#include "raft/consensus.h"
#include "seastar/core/coroutine.hh"
#include "vassert.h"

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

    std::vector<std::pair<int, follower_recovery_state*>> to_sort;
    to_sort.reserve(_pending.size());
    for (auto& frs : _pending) {
        int priority = (is_internal(frs.ntp()) ? 0 : 1);
        to_sort.emplace_back(priority, &frs);
    }

    size_t to_activate = std::min(
      _pending.size(), _max_active() - _active.size());
    std::nth_element(
      to_sort.begin(), to_sort.begin() + to_activate, to_sort.end());
    to_sort.resize(to_activate);

    for (auto [prio, frs] : to_sort) {
        activate(*frs);
    }
}

void recovery_scheduler_base::tick() {
    auto gate_guard = _gate.hold();

    vlog(
      raftlog.debug,
      "tick: active {}/{}, pending {}",
      _active.size(),
      _max_active(),
      _pending.size());

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
      "updated recovery stats (active: {}, pending: {}, offsets pending: {})",
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

} // namespace raft
