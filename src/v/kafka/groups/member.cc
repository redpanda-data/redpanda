/**
 * TODO
 *  - as noted below we should be tracking some statistics on real-world sizes
 *  of data structures (e.g. the number of member protocols) and choosing
 *  alternative data structures (e.g. sorted vector vs map) if it makes sense.
 *  for now, we may be non-optimal for very small sizes, but degrade gracefully
 *  if we have some big inputs.
 */
#include "kafka/groups/member.h"

#include "utils/named_type.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

#include <fmt/ostream.h>

#include <algorithm>
#include <stdexcept>

namespace kafka {

const kafka::protocol_name& group_member::vote_for_protocol(
  const absl::flat_hash_set<protocol_name>& candidates) const {
    auto it = std::find_if(
      _state.protocols.cbegin(),
      _state.protocols.cend(),
      [&candidates](const member_protocol& p) {
          return candidates.find(p.name) != candidates.end();
      });
    if (it == _state.protocols.cend()) {
        throw std::out_of_range("no matching protocol found");
    } else {
        return it->name;
    }
}

const bytes& group_member::get_protocol_metadata(
  const kafka::protocol_name& protocol) const {
    auto it = std::find_if(
      _state.protocols.cbegin(),
      _state.protocols.cend(),
      [&protocol](const member_protocol& p) { return p.name == protocol; });
    if (it == _state.protocols.cend()) {
        throw std::out_of_range(fmt::format("protocol {} not found", protocol));
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
    return fmt_print(
      o,
      "id={} group={} group_inst={} proto_type={} assignment_len={} "
      "timeouts={}/{} protocols={}",
      m.id(),
      m.group_id(),
      m.group_instance_id(),
      m.protocol_type(),
      m.assignment().size(),
      m.session_timeout(),
      m.rebalance_timeout(),
      m._state.protocols);
}

} // namespace kafka
