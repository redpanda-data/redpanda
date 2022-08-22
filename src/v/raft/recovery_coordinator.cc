/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "recovery_coordinator.h"

#include "seastar/core/coroutine.hh"
#include "vassert.h"

namespace raft {

recovery_coordinator_base::recovery_coordinator_base(
  config::binding<size_t> rps)
  // TODO: use something like adjustable_allowance to handle changes to this
  // binding
  : _concurrent_recoveries(rps(), "concurrent_recoveries") {}

recovery_coordinator_base::~recovery_coordinator_base() {
    for (auto s : _states) {
        s->detach();
    }
    _states.clear();
}

static bool is_internal(const model::ntp& ntp) {
    return ntp.ns == model::redpanda_ns
           || ntp.ns == model::kafka_internal_namespace
           || (ntp.ns == model::kafka_namespace && ntp.tp.topic == model::kafka_consumer_offsets_topic);
}

/**
 * Called when a follower_recovery_state is constructed or moved
 */
void recovery_coordinator_base::register_follower(
  follower_recovery_state* frs) {
    vlog(
      raftlog.debug,
      "register_follower: {} ({}) ({} states)",
      *frs,
      fmt::ptr(frs),
      _states.size());
    _states.push_back(frs);

    // Special case for newly created partitions:
    // proceed to "recover" immediately.  This is not necessary for
    // correctness, we do it because:
    //  - improved observability, where "recovery means recovery" rather than
    //    "recovery" stats also spiking up when a partition is created.
    //  - exploit our knowledge that a newly created partition will almost
    //    always just have a single config event in the log.
    //  - simplify testing, so that tests don't have to filter out
    //    spurious "recovery" events that occur during partition creation
    if (frs->last_offset == model::offset{-1} && frs->hwm == model::offset{0}) {
        frs->recovering = true;
    }

    // If we register a follower that is _already_ recovering, it means the
    // leader sent is a recovery message without us asking: perhaps this node
    // restarted while the leader already had a recovery_stm constructed and
    // running.
    // To avoid exceeding our concurrency limit, subtract from our semaphore
    // to account for this partition already being in recovery.
    if (frs->recovering) {
        vlog(
          raftlog.debug,
          "register_follower: {} already in recovery, updating accounting",
          *frs);
        frs->start_recovery(ss::consume_units(_concurrent_recoveries, 1));
        _status.offsets_pending += frs->offset_count();
        _status.offsets_hwm = std::max(
          _status.offsets_pending, _status.offsets_hwm);
        _status.partitions_active += 1;

        return;
    }

    bool fast_dispatch = is_internal(frs->ntp)
                         && _concurrent_recoveries.current() > 0;

    if (fast_dispatch) {
        // If there are units available, dispatch
        // their recovery immediately rather than waiting for the next tick.
        vlog(
          raftlog.debug, "register_follower: fast-start recovery for {}", *frs);
        start_one(frs);
        _status.offsets_pending += frs->offset_count();
        _status.offsets_hwm = std::max(
          _status.offsets_pending, _status.offsets_hwm);
        _status.partitions_active += 1;
    } else {
        // In general, registering a follower for recovery does not start it
        // immediately, but waits til the next tick to group the followers
        // together & prioritize.
        maybe_advance(frs->ntp, false);
    }
}

/**
 * Called when a follower_recovery_state is destroyed or moved.
 */
void recovery_coordinator_base::unregister_follower(
  follower_recovery_state* frs) {
    vlog(
      raftlog.debug,
      "unregister_follower: {} ({}) ({} states)",
      *frs,
      fmt::ptr(frs),
      _states.size());

    std::erase_if(
      _states, [frs](follower_recovery_state* i) { return i == frs; });

    maybe_advance(frs->ntp, false);
}

void recovery_coordinator_base::start_one(follower_recovery_state* st) {
    vlog(
      raftlog.debug,
      "Starting recovery of partition {} ({}) from {} to {}",
      st->ntp,
      fmt::ptr(st),
      st->last_offset,
      st->hwm);
    st->start_recovery(ss::consume_units(_concurrent_recoveries, 1));
}

void recovery_coordinator_base::sort() {
    class sorter {
    public:
        sorter(const std::vector<follower_recovery_state*>& states) {
            for (const auto& s : states) {
                if (is_internal(s->ntp)) {
                    continue;
                }

                auto remaining = std::max(
                  s->hwm - s->last_offset, model::offset{0});
                topic_offset_counts[s->ntp.tp.topic] += remaining;
            }
        }

        bool
        operator()(follower_recovery_state* a, follower_recovery_state* b) {
            // Anything already recovering stays at the front of the queue
            bool a_recovering = a->recovering;
            bool b_recovering = b->recovering;
            if (a_recovering != b_recovering) {
                return a_recovering > b_recovering;
            }

            // Internal topics are the highest priority
            bool a_internal = is_internal(a->ntp);
            bool b_internal = is_internal(b->ntp);
            if (a_internal != b_internal) {
                return a_internal > b_internal;
            }

            // For other topics, put the topic with the lowest outstanding
            // offsets first.
            auto a_count = topic_offset_counts[a->ntp.tp.topic];
            auto b_count = topic_offset_counts[b->ntp.tp.topic];
            return a_count < b_count;
        }

    private:
        std::map<model::topic, model::offset> topic_offset_counts;
    };

    auto s = sorter(_states);
    std::sort(_states.begin(), _states.end(), s);
}

ss::future<> recovery_coordinator_base::tick() {
    auto gate_guard = _gate.hold();

    vlog(
      raftlog.debug,
      "tick: units available {}, queue {}",
      _concurrent_recoveries.current(),
      _states.size());

    if (_followers_dirty) {
        _followers_dirty = false;

        if (_concurrent_recoveries.current() > 0) {
            // We have some units to hand out: sort the list before
            // doing so.  Otherwise sorting is a waste of time.
            sort();
        }

        if (_states.empty()) {
            vlog(raftlog.debug, "No followers awaiting recovery.");
        } else {
            vlog(raftlog.trace, "Current groups awaiting recovery:");
            for (const auto& s : _states) {
                vlog(raftlog.trace, "  {}", *s);
            }
        }

        for (const auto st : _states) {
            if (_concurrent_recoveries.current() <= 0) {
                break;
            }

            if (st->recovering == false) {
                start_one(st);
            }
        }
    } else {
        vlog(raftlog.trace, "Updating stats (no changes to follower list)");
    }

    // Update status summary
    _status.offsets_pending = 0;
    _status.partitions_pending = 0;
    _status.partitions_active = 0;
    for (const auto st : _states) {
        if (st->recovering) {
            _status.partitions_active += 1;
        } else {
            _status.partitions_pending += 1;
        }

        _status.offsets_pending += st->offset_count();
    }
    _status.offsets_hwm = std::max(
      _status.offsets_pending, _status.offsets_hwm);

    vlog(
      raftlog.trace,
      "Updated stats ({} active, {} total)",
      _status.partitions_active,
      _states.size());

    co_return;
}

const recovery_status& recovery_coordinator_base::get_status() {
    return _status;
}

} // namespace raft

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<raft::follower_recovery_state, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  raft::follower_recovery_state const& frs,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return fmt::format_to(
      ctx.out(),
      "follower({}, {}, {}-{})",
      frs.ntp,
      frs.recovering,
      frs.last_offset,
      frs.hwm);
}
