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

#include "raft/replication_monitor.h"

#include "raft/consensus.h"
#include "raft/errc.h"

#include <ranges>

namespace raft {

replication_monitor::replication_monitor(consensus* r)
  : _raft(r) {
    ssx::repeat_until_gate_closed(_gate, [this] {
        return do_notify_replicated().handle_exception(
          [this](const std::exception_ptr& e) {
              if (!ssx::is_shutdown_exception(e)) {
                  vlog(
                    _raft->_ctxlog.error,
                    "Exception running replication monitor, ignoring: {}",
                    e);
              }
          });
    });

    ssx::repeat_until_gate_closed(_gate, [this] {
        return do_notify_committed().handle_exception(
          [this](const std::exception_ptr& e) {
              if (!ssx::is_shutdown_exception(e)) {
                  vlog(
                    _raft->_ctxlog.error,
                    "Exception running replication monitor, ignoring: {}",
                    e);
              }
          });
    });
}

std::ostream& operator<<(std::ostream& o, const replication_monitor& mon) {
    static constexpr long max_waiters_to_print = 3;
    auto view = std::views::values(mon._waiters);
    auto end = std::ranges::next(
      view.begin(), max_waiters_to_print, view.end());
    fmt::print(
      o,
      "{{count: {}, pending_majority_waiters: {}, waiters: [{}]}}",
      mon._waiters.size(),
      mon._pending_majority_replication_waiters,
      fmt::join(view.begin(), end, ","));
    return o;
}

std::ostream&
operator<<(std::ostream& o, const replication_monitor::waiter& entry) {
    fmt::print(
      o, "{{type: {}, append result: {}}}", entry.type, entry.append_info);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const replication_monitor::wait_type& type) {
    switch (type) {
    case replication_monitor::commit:
        fmt::print(o, "commit");
        break;
    case replication_monitor::majority_replication:
        fmt::print(o, "majority_replication");
        break;
    }
    return o;
}

ss::future<> replication_monitor::stop() {
    auto f = _gate.close();
    for (auto& [_, waiter] : _waiters) {
        waiter->done.set_value(errc::shutting_down);
    }
    _committed_event_cv.broken();
    _replicated_event_cv.broken();
    return f;
}

ss::future<errc> replication_monitor::do_wait_until(
  storage::append_result append, wait_type type) {
    if (_gate.is_closed()) {
        return ssx::now(errc::not_leader);
    }
    auto it = _waiters.emplace(
      append.base_offset, ss::make_lw_shared<waiter>(this, append, type));
    return it->second->done.get_future();
}

ss::future<errc>
replication_monitor::wait_until_committed(storage::append_result append) {
    auto done = is_append_committed_or_truncated(append);
    if (done) {
        return ssx::now(done.value());
    }
    return do_wait_until(append, wait_type::commit);
}

ss::future<errc> replication_monitor::wait_until_majority_replicated(
  storage::append_result append) {
    if (is_append_replicated(append)) {
        return ssx::now(errc::success);
    }
    return do_wait_until(append, wait_type::majority_replication);
}

std::optional<errc> replication_monitor::is_append_committed_or_truncated(
  const storage::append_result& append_info) const {
    auto committed_offset = _raft->committed_offset();
    auto committed_offset_term = _raft->get_term(committed_offset);
    auto appended_term = _raft->get_term(append_info.last_offset);
    // Truncation is sealed once the following events happen
    // - There is new entry from a different term replacing the
    //   appended entries.
    // - The new entry is committed.
    // The second condition is important because without that the
    // original entries may be reinstanted after another leadership
    // change. For example:
    //
    // 5 replicas A, B, C, D, E, leader=A, term=5
    //
    // A - replicate([base: 10, last: 20]) term=5
    // A - append_local([10, 20]) term=5, dirty=20
    // A -> B - append_entries([10, 20]) term=5, dirty=20
    // A - frozen briefly, cannot send further append_entries
    // (C, D, E), elect C as leader, term=6, dirty=9
    // C - append_local([10]) - configuration batch - term=6
    // C -> A append_entries([10]), term=6
    // C - crashes
    // A truncates, term=6, dirty_offset=10 -> First truncation
    // (B, D, E), elect B as leader, term=7, dirty=20
    // B -> A, D, E append_entries([10, 20])
    // committed offset = 20
    //
    // In the above example if we do not wait for committed
    // offset and stop at first truncation event, we risk an
    // incorrect truncation detection.
    auto truncated = [&] {
        return committed_offset_term > append_info.last_term
               && appended_term != append_info.last_term;
    };
    if (
      appended_term == append_info.last_term
      && committed_offset >= append_info.last_offset) {
        // committed
        return errc::success;
    } else if (truncated()) {
        // truncated
        return errc::replicated_entry_truncated;
    }
    return std::nullopt;
}

bool replication_monitor::is_append_replicated(
  const storage::append_result& append_info) const {
    auto replicated_term = _raft->get_term(append_info.last_offset);
    // We need the offset replicated in the same term it is appended in
    // for the replication to be successful.
    // Truncation is detected based on committed offset updates.
    return replicated_term == append_info.last_term
           && _raft->_majority_replicated_index >= append_info.last_offset;
}

ss::future<> replication_monitor::do_notify_replicated() {
    if (_pending_majority_replication_waiters > 0) {
        auto majority_replicated_offset = _raft->_majority_replicated_index;
        auto it = _waiters.begin();
        while (it != _waiters.end()
               && it->first <= majority_replicated_offset) {
            auto& entry = it->second;
            auto& append_info = entry->append_info;
            if (
              entry->type == wait_type::majority_replication
              && is_append_replicated(append_info)) {
                entry->done.set_value(errc::success);
                it = _waiters.erase(it);
            } else {
                ++it;
            }
        }
    }
    co_await _replicated_event_cv.wait();
}

ss::future<> replication_monitor::do_notify_committed() {
    auto committed_offset = _raft->committed_offset();
    auto committed_offset_term = _raft->get_term(committed_offset);
    auto it = _waiters.begin();
    while (it != _waiters.end()) {
        auto& entry = it->second;
        auto& append_info = entry->append_info;

        // The loop is only interested in the waiters that the new committed
        // offset may affect. The term condition is to detect truncation events.
        auto has_affected_waiters
          = (append_info.base_offset <= committed_offset || append_info.last_term < committed_offset_term);
        if (!has_affected_waiters) {
            break;
        }

        auto committed_or_truncated = is_append_committed_or_truncated(
          append_info);
        if (committed_or_truncated) {
            entry->done.set_value(committed_or_truncated.value());
            it = _waiters.erase(it);
        } else {
            ++it;
        }
    }
    co_await _committed_event_cv.wait();
}

void replication_monitor::notify_replicated() {
    if (_gate.is_closed()) {
        return;
    }
    _replicated_event_cv.signal();
}

void replication_monitor::notify_committed() {
    if (_gate.is_closed()) {
        return;
    }
    _committed_event_cv.signal();
}

replication_monitor::waiter::waiter(
  replication_monitor* mon, storage::append_result r, wait_type t) noexcept
  : monitor(mon)
  , append_info(r)
  , type(t) {
    if (type == wait_type::majority_replication) {
        monitor->_pending_majority_replication_waiters++;
    }
}
replication_monitor::waiter::~waiter() noexcept {
    if (type == wait_type::majority_replication) {
        monitor->_pending_majority_replication_waiters--;
    }
}

} // namespace raft
