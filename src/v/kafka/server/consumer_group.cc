/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/consumer_group.h"

#include <algorithm>
#include <exception>
#include <utility>

namespace kafka {

std::string_view to_string_view(consumer_group_state s) {
    switch (s) {
    case consumer_group_state::empty:
        return group_state_name_empty;
    case consumer_group_state::assigning:
        return group_state_name_assigning;
    case consumer_group_state::reconciling:
        return group_state_name_reconciling;
    case consumer_group_state::stable:
        return group_state_name_stable;
    case consumer_group_state::dead:
        return group_state_name_dead;
    }
    std::terminate();
}

consumer_group::consumer_group(
  kafka::group_id id,
  config::configuration& conf,
  ss::lw_shared_ptr<ss::rwlock> catchup_lock,
  std::unique_ptr<offset_writer> writer,
  model::term_id term,
  std::unique_ptr<tx_coordinator_client> tx_coordinator,
  ss::sharded<features::feature_table>& feature_table)
  : _id(std::move(id))
  , _offset_store(
      _id,
      conf,
      std::move(catchup_lock),
      std::move(writer),
      term,
      std::move(tx_coordinator),
      feature_table,
      [this] { return _removed; }) {}

void consumer_group::upsert_member(consumer_group_member member) {
    auto id = member.id;
    _members.insert_or_assign(std::move(id), std::move(member));
}

consumer_group_state consumer_group::state() const {
    if (_members.empty()) {
        return consumer_group_state::empty;
    }
    if (_epoch() > _assignment_epoch()) {
        return consumer_group_state::assigning;
    }
    // Reaching the epoch the target was computed at is not enough: a member
    // whose epoch was bumped still holds partitions it owes back, or waits for
    // partitions their previous owner has not released, and reports that in its
    // own state. Its epoch and the assignment epoch are separate types because
    // assigning one to the other is always a bug, so compare their values.
    const auto reconciled = [this](const members_map::value_type& m) {
        return m.second.state == consumer_group_member_state::stable
               && m.second.epoch() == _assignment_epoch();
    };
    return std::all_of(_members.begin(), _members.end(), reconciled)
             ? consumer_group_state::stable
             : consumer_group_state::reconciling;
}

} // namespace kafka
