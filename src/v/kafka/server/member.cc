// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

/**
 * TODO
 *  - as noted below we should be tracking some statistics on real-world sizes
 *  of data structures (e.g. the number of member protocols) and choosing
 *  alternative data structures (e.g. sorted vector vs map) if it makes sense.
 *  for now, we may be non-optimal for very small sizes, but degrade gracefully
 *  if we have some big inputs.
 */
#include "kafka/server/member.h"

#include "utils/named_type.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

#include <fmt/ostream.h>

#include <algorithm>
#include <stdexcept>

namespace kafka {

[[noreturn]] [[gnu::cold]] static void
throw_out_of_range(const ss::sstring& msg) {
    throw std::out_of_range(msg);
}

const kafka::protocol_name& group_member::vote_for_protocol(
  const absl::flat_hash_set<protocol_name>& candidates) const {
    auto it = std::find_if(
      _protocols.cbegin(),
      _protocols.cend(),
      [&candidates](const member_protocol& p) {
          return candidates.find(p.name) != candidates.end();
      });
    if (it == _protocols.cend()) {
        throw_out_of_range("no matching protocol found");
    } else {
        return it->name;
    }
}

const bytes& group_member::get_protocol_metadata(
  const kafka::protocol_name& protocol) const {
    auto it = std::find_if(
      _protocols.cbegin(),
      _protocols.cend(),
      [&protocol](const member_protocol& p) { return p.name == protocol; });
    if (it == _protocols.cend()) {
        throw_out_of_range(fmt::format("protocol {} not found", protocol));
    } else {
        return it->metadata;
    }
}

bool group_member::should_keep_alive(
  clock_type::time_point deadline, clock_type::duration new_join_timeout) {
    if (is_joining()) {
        return _is_new || (_latest_heartbeat + new_join_timeout) > deadline;
    }

    if (is_syncing()) {
        return (_latest_heartbeat + session_timeout()) > deadline;
    }

    return false;
}

std::ostream& operator<<(std::ostream& o, const group_member& m) {
    auto timer_expires =
      [](const auto& timer) -> std::optional<group_member::duration_type> {
        if (timer.armed()) {
            return timer.get_timeout() - group_member::clock_type::now();
        }
        return std::nullopt;
    };

    fmt::print(
      o,
      "id={} group={} group_inst={} proto_type={} assignment_len={} "
      "timeouts={}/{} protocols={} is_new={} joining={} syncing={} "
      "latest_heartbeat={} expires={}",
      m.id(),
      m.group_id(),
      m.group_instance_id(),
      m.protocol_type(),
      m.assignment().size(),
      m.session_timeout(),
      m.rebalance_timeout(),
      m._protocols,
      m._is_new,
      m.is_joining(),
      m.is_syncing(),
      m._latest_heartbeat.time_since_epoch(),
      timer_expires(m._expire_timer));
    return o;
}

described_group_member
group_member::describe(const kafka::protocol_name& protocol) const {
    auto desc = describe_without_metadata();
    desc.member_metadata = get_protocol_metadata(protocol);
    desc.member_assignment = assignment();
    return desc;
}

described_group_member group_member::describe_without_metadata() const {
    described_group_member desc{
      .member_id = id(),
      .group_instance_id = group_instance_id(),
      .client_id = _state.client_id,
      .client_host = _state.client_host,
    };
    return desc;
}

} // namespace kafka
