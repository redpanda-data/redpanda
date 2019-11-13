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

/*
 * priority ordered search in candidates. if candidates is always small it
 * should be faster to use a sorted vector.
 */
const kafka::protocol_name&
group_member::vote(const std::set<protocol_name>& candidates) const {
    auto it = std::find_if(
      std::cbegin(_protocols),
      std::cend(_protocols),
      [&candidates](const member_protocol& p) {
          return candidates.find(p.name) != candidates.end();
      });
    if (it == _protocols.end()) {
        throw std::out_of_range("no matching protocol found");
    } else {
        return it->name;
    }
}

/*
 * linear in the number of member protocols. the protocols vector implicitly
 * encodes priority, so sorting it would mean a search for priority elsewhere or
 * keeping a parallel data structure.
 */
const bytes&
group_member::metadata(const kafka::protocol_name& protocol) const {
    auto it = std::find_if(
      std::cbegin(_protocols),
      std::cend(_protocols),
      [&protocol](const member_protocol& p) { return p.name == protocol; });
    if (it == _protocols.end()) {
        throw std::out_of_range(fmt::format("protocol {} not found", protocol));
    } else {
        return it->metadata;
    }
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
      m._protocols);
}

} // namespace kafka
