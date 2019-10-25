#include "redpanda/kafka/groups/group.h"

#include "bytes/bytes.h"
#include "redpanda/kafka/requests/sync_group_request.h"
#include "utils/to_string.h"

#include <seastar/core/future.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <ostream>
#include <set>
#include <stdexcept>
#include <streambuf>
#include <utility>

namespace kafka {

logger log("k:groups");

using member_config = join_group_response::member_config;

bool group::valid_previous_state(group_state s) const {
    using g = group_state;

    switch (s) {
    case g::empty:
        return _state == g::preparing_rebalance;
    case g::completing_rebalance:
        return _state == g::preparing_rebalance;
    case g::preparing_rebalance:
        return _state == g::empty || _state == g::stable
               || _state == g::completing_rebalance;
    case g::stable:
        return _state == g::completing_rebalance;
    case g::dead:
        return true;
    default:
        std::abort(); // make gcc happy
    }
}

void group::set_state(group_state s) {
    log.debug("group state transition {} -> {}", _state, s);
    if (!valid_previous_state(s)) {
        std::abort();
    }
    _state = s;
}

bool group::supports_protocols(const join_group_request& r) {
    // first member decides so make sure its defined
    if (in_state(group_state::empty)) {
        return !r.protocol_type().empty() && !r.protocols.empty();
    }

    if (!_protocol_type || *_protocol_type != r.protocol_type) {
        return false;
    }

    // check that at least one of the protocols in the request is supported
    // by all other group members.
    return std::any_of(
      r.protocols.cbegin(),
      r.protocols.cend(),
      [this](const kafka::member_protocol& p) {
          auto it = _supported_protocols.find(p.name);
          return it != _supported_protocols.end()
                 && it->second == _members.size();
      });
}

future<join_group_response> group::add_member(member_ptr member) {
    if (_members.empty()) {
        _protocol_type = member->protocol_type();
    }

    if (!_leader) {
        _leader = member->id();
    }

    auto res = _members.emplace(member->id(), member);
    if (!res.second) {
        throw std::runtime_error(
          fmt::format("group already contains member {}", member));
    }

    member->for_each_protocol(
      [this](const member_protocol& p) { _supported_protocols[p.name]++; });

    // TODO
    //  - when restoring persisted group state members are created and added but
    //  there will be no associated request/context to account for.
    _num_members_joining++;
    return member->get_join_response();
}

future<join_group_response> group::update_member(
  member_ptr member, std::vector<member_protocol>&& new_protocols) {
    // subtract out old protocols
    member->for_each_protocol(
      [this](const member_protocol& p) { _supported_protocols[p.name]--; });

    // add in the new protocols
    member->set_protocols(std::move(new_protocols));
    member->for_each_protocol(
      [this](const member_protocol& p) { _supported_protocols[p.name]++; });

    if (!member->is_joining()) {
        _num_members_joining++;
        return member->get_join_response();
    }

    return make_exception_future<join_group_response>(std::runtime_error(
      fmt::format("updating non-joining member {}", member->id())));
}

group::duration_type group::rebalance_timeout() const {
    auto it = std::max_element(
      std::cbegin(_members),
      std::cend(_members),
      [](const member_map::value_type& a, const member_map::value_type& b) {
          return a.second->rebalance_timeout() < b.second->rebalance_timeout();
      });
    if (__builtin_expect(it != _members.end(), true)) {
        return it->second->rebalance_timeout();
    } else {
        throw std::runtime_error("no members in group");
    }
}

void group::remove_unjoined_members() {
    for (auto it = _members.begin(); it != _members.end();) {
        if (!it->second->is_joining()) {
            log.debug("removing unjoined member {}", it->first);
            // TODO: ensure that member heartbeat timers are canceled in
            // group destructor and in member dtor
            it = _members.erase(it);
        } else {
            it++;
        }
    }
}

std::vector<member_config> group::member_metadata() const {
    if (
      in_state(group_state::dead)
      || in_state(group_state::preparing_rebalance)) {
        throw std::runtime_error(
          fmt::format("invalid group state: {}", _state));
    }

    std::vector<member_config> out;
    std::transform(
      std::cbegin(_members),
      std::cend(_members),
      std::back_inserter(out),
      [this](const member_map::value_type& m) {
          auto& group_inst = m.second->group_instance_id();
          auto metadata = m.second->metadata(*_protocol);
          return member_config{.member_id = m.first,
                               .group_instance_id = group_inst,
                               .metadata = std::move(metadata)};
      });
    return out;
}

void group::add_missing_assignments(assignments_type& assignments) const {
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [&assignments](const member_map::value_type& m) mutable {
          // emplace does nothing if an entry exists
          assignments.emplace(m.first, bytes{});
      });
}

void group::set_assignments(assignments_type assignments) const {
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [&assignments](const member_map::value_type& m) mutable {
          m.second->set_assignment(std::move(assignments.at(m.first)));
      });
}

void group::clear_assignments() const {
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [](const member_map::value_type& m) { m.second->clear_assignment(); });
}

void group::advance_generation() {
    ++_generation;
    if (_members.empty()) {
        _protocol = std::nullopt;
        set_state(group_state::empty);
    } else {
        _protocol = select_protocol();
        set_state(group_state::completing_rebalance);
    }
    log.debug(
      "advanced to group generation {} protocol {} state {}",
      _generation,
      _protocol,
      _state);
}

/*
 * TODO
 *   - it is reasonable to try and consolidate the vote collection and final
 *   result reduction for efficiency as long as its done without lots of string
 *   creation / destruction.
 */
kafka::protocol_name group::select_protocol() const {
    log.debug("selecting group protocol");

    // index of protocols supported by all members
    std::set<kafka::protocol_name> candidates;
    std::for_each(
      std::cbegin(_supported_protocols),
      std::cend(_supported_protocols),
      [this, &candidates](const protocol_support::value_type& p) mutable {
          if (p.second == _members.size()) {
              candidates.insert(p.first);
              log.debug("candidate: {}", p.first);
          }
      });

    // collect votes from members
    protocol_support votes;
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [&votes, &candidates](const member_map::value_type& m) mutable {
          auto& choice = m.second->vote(candidates);
          auto total = ++votes[choice];
          log.debug(
            "member {} votes for protocol {} ({})", m.first, choice, total);
      });

    // select the candidate protocol with the most votes
    auto winner = std::max_element(
      std::cbegin(votes),
      std::cend(votes),
      [](
        const protocol_support::value_type& v1,
        const protocol_support::value_type& v2) {
          return v1.second < v2.second;
      });

    // this is guaranteed to succeed because `member->vote` will throw if it
    // is unable to vote on some protocol candidate.
    log.debug("selected group protocol {}", winner->first);
    return winner->first;
}

/*
 * TODO
 *   - after the promise is fulfilled we need to re-arm the member's heartbeat
 *   expiration timer.
 */
void group::finish_joining_members() {
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [this](const member_map::value_type& m) mutable {
          if (!m.second->is_joining()) {
              return;
          }

          // leader    -> member metadata
          // followers -> []
          std::vector<member_config> md;
          if (is_leader(m.first)) {
              md = member_metadata();
          }

          auto reply = join_group_response(
            error_code::none,
            _generation,
            _protocol.value_or(kafka::protocol_name()),
            _leader.value_or(kafka::member_id()),
            m.first,
            std::move(md));

          log.debug(
            "set join response for member {} reply {}", m.second, reply);

          _num_members_joining--;
          m.second->set_join_response(std::move(reply));
      });
}

/*
 * TODO
 * - cancel and re-arm member heartbeat after setting promise value
 */
void group::finish_syncing_members(error_code error) const {
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [](const member_map::value_type& m) {
          auto& member = m.second;
          if (!member->is_syncing()) {
              return;
          }
          auto reply = sync_group_response(member->assignment());
          log.debug("set sync response for member {}", member);
          member->set_sync_response(std::move(reply));
      });
}

bool group::leader_rejoined() {
    if (!_leader) {
        log.debug("group has no leader");
        return false;
    }

    auto leader = get_member(*_leader);
    if (leader->is_joining()) {
        log.debug("leader has rejoined");
        return true;
    } else {
        log.debug("leader has not rejoined {}", *_leader);
    }

    // look for a replacement
    auto it = std::find_if(
      std::cbegin(_members),
      std::cend(_members),
      [](const member_map::value_type& member) {
          return member.second->is_joining();
      });

    if (it == _members.end()) {
        log.debug("group has no leader replacement");
        return false;
    } else {
        _leader = it->first;
        log.debug("selected new leader {}", *_leader);
        return true;
    }
}

kafka::member_id group::generate_member_id(const join_group_request& r) {
    auto client_id = r.client_id ? *r.client_id : "";
    auto id = r.group_instance_id ? (*r.group_instance_id)() : client_id;
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return kafka::member_id(fmt::format("{}-{}", id, uuid));
}

std::ostream& operator<<(std::ostream& o, const group& g) {
    return fmt_print(
      o,
      "id={} state={} gen={} ntp={} proto_type={} proto={} leader={} "
      "empty={}",
      g.id(),
      g.state(),
      g.generation(),
      g.ntp(),
      g.protocol_type(),
      g.protocol(),
      g.leader(),
      !g.has_members());
}

std::ostream& operator<<(std::ostream& o, group_state gs) {
    switch (gs) {
    case group_state::empty:
        return o << "empty";
    case group_state::preparing_rebalance:
        return o << "preparing_rebalance";
    case group_state::completing_rebalance:
        return o << "completing_rebalance";
    case group_state::stable:
        return o << "stable";
    case group_state::dead:
        return o << "dead";
    default:
        std::abort(); // make gcc happy
    }
}

} // namespace kafka
