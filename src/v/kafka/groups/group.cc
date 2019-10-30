#include "kafka/groups/group.h"

#include "bytes/bytes.h"
#include "kafka/requests/sync_group_request.h"
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
        std::terminate(); // make gcc happy
    }
}

group_state group::set_state(group_state s) {
    kglog.trace("group state transition {} -> {}", _state, s);
    if (!valid_previous_state(s)) {
        std::terminate();
    }
    return std::exchange(_state, s);
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
    kglog.trace(
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
    kglog.trace("selecting group protocol");

    // index of protocols supported by all members
    std::set<kafka::protocol_name> candidates;
    std::for_each(
      std::cbegin(_supported_protocols),
      std::cend(_supported_protocols),
      [this, &candidates](const protocol_support::value_type& p) mutable {
          if (p.second == _members.size()) {
              candidates.insert(p.first);
              kglog.trace("candidate: {}", p.first);
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
          kglog.trace(
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
    kglog.trace("selected group protocol {}", winner->first);
    return winner->first;
}

void group::finish_syncing_members(error_code error) {
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [this, error](const member_map::value_type& m) {
          auto member = m.second;
          if (member->is_syncing()) {
              auto reply = sync_group_response(error, member->assignment());
              kglog.trace("set sync response for member {}", member);
              member->set_sync_response(std::move(reply));
              // <kafka>reset the session timeout for members after propagating
              // the member's assignment. This is because if any member's
              // session expired while we were still awaiting either the leader
              // sync group or the storage callback, its expiration will be
              // ignored and no future heartbeat expectations will not be
              // scheduled.</kafka>
              schedule_next_heartbeat_expiration(member);
          }
      });
}

bool group::leader_rejoined() {
    if (!_leader) {
        kglog.trace("group has no leader");
        return false;
    }

    auto leader = get_member(*_leader);
    if (leader->is_joining()) {
        kglog.trace("leader has rejoined");
        return true;
    } else {
        kglog.trace("leader has not rejoined {}", *_leader);
    }

    // look for a replacement
    auto it = std::find_if(
      std::cbegin(_members),
      std::cend(_members),
      [](const member_map::value_type& member) {
          return member.second->is_joining();
      });

    if (it == _members.end()) {
        kglog.trace("group has no leader replacement");
        return false;
    } else {
        _leader = it->first;
        kglog.trace("selected new leader {}", *_leader);
        return true;
    }
}

future<join_group_response> group::handle_join_group(join_group_request&& r) {
    if (r.member_id == unknown_member_id) {
        return join_group_unknown_member(std::move(r));
    }
    return join_group_known_member(std::move(r));
    /*
     * TODO
     *   - after join_group completes the group may be in a state where the join
     *   phase can complete immediately rather than waiting on the join timer to
     *   fire. thus, we want to check if thats the case before returning.
     *   however, we can also be in a completable state but want to delay anyway
     *   for debouncing optimization. currently we debounce but we don't have a
     *   way to distinguish between these scenarios. this identification is
     *   needed in other area for optimization heuristics. it won't affect
     *   correctness.
     */
}

future<join_group_response>
group::join_group_unknown_member(join_group_request&& r) {
    kglog.trace("unknown member joining group {}", *this);

    if (in_state(group_state::dead)) {
        kglog.trace("group is in the dead state");
        return make_join_error(
          unknown_member_id, error_code::coordinator_not_available);

    } else if (!supports_protocols(r)) {
        kglog.trace("requested protocols not supported by group");
        return make_join_error(
          unknown_member_id, error_code::inconsistent_group_protocol);
    }

    auto new_member_id = group::generate_member_id(r);

    // <kafka>Only return MEMBER_ID_REQUIRED error if joinGroupRequest version
    // is >= 4 and groupInstanceId is configured to unknown.</kafka>
    auto require_member_id = r.version >= api_version(4)
                             && !r.group_instance_id;

    if (require_member_id) {
        // <kafka>If member id required (dynamic membership), register the
        // member in the pending member list and send back a response to
        // call for another join group request with allocated member id.
        // </kafka>
        add_pending_member(new_member_id);
        kglog.trace("requesting member rejoin with new id {}", new_member_id);
        return make_join_error(new_member_id, error_code::member_id_required);
    } else {
        return add_member_and_rebalance(std::move(new_member_id), std::move(r));
    }
}

future<join_group_response>
group::join_group_known_member(join_group_request&& r) {
    kglog.trace("member {} joining group {}", r.member_id, *this);

    if (in_state(group_state::dead)) {
        kglog.trace("group is in the dead state");
        return make_join_error(
          r.member_id, error_code::coordinator_not_available);

    } else if (!supports_protocols(r)) {
        kglog.trace("requested protocols not supported by group");
        return make_join_error(
          r.member_id, error_code::inconsistent_group_protocol);

    } else if (contains_pending_member(r.member_id)) {
        kglog.trace("making pending member a regular member");
        if (r.group_instance_id) {
            throw std::runtime_error(
              fmt::format("unexpected static pending member {}", r.member_id));
        }
        kafka::member_id new_member_id = std::move(r.member_id);
        return add_member_and_rebalance(std::move(new_member_id), std::move(r));

    } else if (!contains_member(r.member_id)) {
        kglog.trace("member is not registered in the group");
        return make_join_error(r.member_id, error_code::unknown_member_id);
    }

    auto member = get_member(r.member_id);

    switch (state()) {
    case group_state::preparing_rebalance:
        return update_member_and_rebalance(member, std::move(r));

    case group_state::completing_rebalance:
        if (member->matching_protocols(r)) {
            // <kafka>member is joining with the same metadata (which could be
            // because it failed to receive the initial JoinGroup response), so
            // just return current group information for the current
            // generation.</kafka>
            kglog.trace("resending the member join group response");

            // the leader receives group member metadata
            std::vector<join_group_response::member_config> members;
            if (is_leader(r.member_id)) {
                members = member_metadata();
            }

            join_group_response response(
              error_code::none,
              kafka::generation_id(-1),
              protocol().value_or(protocol_name("")),
              leader().value_or(member_id("")),
              std::move(r.member_id),
              std::move(members));

            return make_ready_future<join_group_response>(std::move(response));

        } else {
            // <kafka>member has changed metadata, so force a rebalance</kafka>
            kglog.trace("member rejoined while completing rebalance");
            return update_member_and_rebalance(member, std::move(r));
        }

    case group_state::stable:
        if (is_leader(r.member_id) || !member->matching_protocols(r)) {
            // <kafka>force a rebalance if a member has changed metadata or if
            // the leader sends JoinGroup. The latter allows the leader to
            // trigger rebalances for changes affecting assignment which do not
            // affect the member metadata (such as topic metadata changes for
            // the consumer)</kafka>
            kglog.trace(
              "member rejoining (leader={}) stable group causing rebalance",
              (is_leader(r.member_id) ? "yes" : "no"));

            return update_member_and_rebalance(member, std::move(r));

        } else {
            // <kafka>for followers with no actual change to their metadata,
            // just return group information for the current generation which
            // will allow them to issue SyncGroup</kafka>
            kglog.trace("follower rejoined stable group with identical state");

            join_group_response response(
              error_code::none,
              generation(),
              protocol().value_or(protocol_name("")),
              leader().value_or(member_id("")),
              std::move(r.member_id));

            return make_ready_future<join_group_response>(std::move(response));
        }

    case group_state::empty:
        [[fallthrough]];

    case group_state::dead:
        kglog.trace(
          "member {} rejoin in unexpected state {}", r.member_id, state());
        return make_join_error(r.member_id, error_code::unknown_member_id);

    default:
        std::terminate(); // make gcc happy
    }
}

future<join_group_response> group::add_member_and_rebalance(
  kafka::member_id member_id, join_group_request&& r) {
    auto member = make_lw_shared<group_member>(
      std::move(member_id),
      id(),
      std::move(r.group_instance_id),
      r.session_timeout,
      r.rebalance_timeout,
      std::move(r.protocol_type),
      std::move(r.protocols));

    // mark member as new. this is used in heartbeat expiration heuristics.
    member->set_new(true);

    // <kafka>update the newMemberAdded flag to indicate that the join group can
    // be further delayed</kafka>
    if (
      in_state(group_state::preparing_rebalance)
      && generation() == kafka::generation_id(0)) {
        _new_member_added = true;
    }

    // adding the member initializes the member's join promise that's fulfilled
    // when all the group members show up.  if this is the last member to join
    // then this fiber will synchronously fulfill and _reset_ the join promises
    // of all members, including the member associated with this request (this
    // is done below in `try_prepare_rebalance`). therefore, we grab the future
    // now since the promise may be invalidated before we return.
    auto response = add_member(member);
    kglog.trace("added member {} to group {}", member, *this);
    _pending_members.erase(member_id);

    // <kafka>The session timeout does not affect new members since they do not
    // have their memberId and cannot send heartbeats. Furthermore, we cannot
    // detect disconnects because sockets are muted while the JoinGroup is in
    // purgatory. If the client does disconnect (e.g. because of a request
    // timeout during a long rebalance), they may simply retry which will lead
    // to a lot of defunct members in the rebalance. To prevent this going on
    // indefinitely, we timeout JoinGroup requests for new members. If the new
    // member is still there, we expect it to retry.</kafka>
    member->expire_timer().cancel();
    auto now = clock_type::now();
    member->set_latest_heartbeat(now);
    auto deadline = now + _conf.group_new_member_join_timeout();
    member->expire_timer().set_callback(
      [this, deadline, member_id = member->id()]() {
          heartbeat_expire(member_id, deadline);
      });
    member->expire_timer().arm(deadline);

    try_prepare_rebalance();
    return response;
}

future<join_group_response>
group::update_member_and_rebalance(member_ptr member, join_group_request&& r) {
    auto response = update_member(member, std::move(r.protocols));
    try_prepare_rebalance();
    return response;
}

void group::try_prepare_rebalance() {
    if (!valid_previous_state(group_state::preparing_rebalance)) {
        kglog.trace("skipping prepare rebalance state={}", state());
        return;
    }

    // <kafka>if any members are awaiting sync, cancel their request and have
    // them rejoin.</kafka>
    if (in_state(group_state::completing_rebalance)) {
        kglog.trace("requesting rejoin from syncing members");
        clear_assignments();
        finish_syncing_members(error_code::rebalance_in_progress);
    }

    auto prev_state = set_state(group_state::preparing_rebalance);

    if (prev_state == group_state::empty) {
        // debounce joins to an empty group. for a bounded delay, we'll avoid
        // competing the join phase as long as new members are arriving.
        auto rebalance = rebalance_timeout();
        auto initial = _conf.group_initial_rebalance_delay();
        auto remaining = std::max(rebalance - initial, duration_type(0));

        _join_timer.cancel();
        _join_timer.set_callback(
          [this, initial, delay = initial, remaining]() mutable {
              if (_new_member_added && remaining.count()) {
                  _new_member_added = false;
                  auto prev_delay = delay;
                  delay = std::min(initial, remaining);
                  remaining = std::max(
                    remaining - prev_delay, duration_type(0));
                  _join_timer.arm(delay);
              } else {
                  complete_join();
              }
          });

        kglog.trace("debouncing join {}", initial);
        _join_timer.arm(initial);

    } else if (all_members_joined()) {
        complete_join();

    } else {
        auto timeout = rebalance_timeout();
        _join_timer.cancel();
        _join_timer.set_callback([this]() { complete_join(); });
        _join_timer.arm(timeout);
    }
}

void group::complete_join() {
    kglog.trace("completing join for group {}", *this);

    // <kafka>remove dynamic members who haven't joined the group yet</kafka>
    // this is the old group->remove_unjoined_members();
    for (auto it = _members.begin(); it != _members.end();) {
        if (!it->second->is_joining()) {
            kglog.trace("removing unjoined member {}", it->first);
            it->second->expire_timer().cancel();
            it = _members.erase(it);
        } else {
            it++;
        }
    }

    if (in_state(group_state::dead)) {
        kglog.trace("skipping join completion because group is dead");

    } else if (!leader_rejoined() && has_members()) {
        // <kafka>If all members are not rejoining, we will postpone the
        // completion of rebalance preparing stage, and send out another
        // delayed operation until session timeout removes all the
        // non-responsive members.</kafka>
        //
        // the callback needs to be reset because we may have arrived here via
        // the initial delayed callback which implements debouncing.
        kglog.trace("could not complete rebalance because no members rejoined");
        auto timeout = rebalance_timeout();
        _join_timer.cancel();
        _join_timer.set_callback([this]() { complete_join(); });
        _join_timer.arm(timeout);

    } else {
        advance_generation();

        if (in_state(group_state::empty)) {
            /*
             * TODO
             * Package up the empty group state and replicate to raft.
             */
            return;
        } else {
            std::for_each(
              std::cbegin(_members),
              std::cend(_members),
              [this](const member_map::value_type& m) {
                  auto member = m.second;

                  // leader    -> member metadata
                  // followers -> []
                  std::vector<member_config> md;
                  if (is_leader(member->id())) {
                      md = member_metadata();
                  }

                  auto reply = join_group_response(
                    error_code::none,
                    generation(),
                    protocol().value_or(kafka::protocol_name()),
                    leader().value_or(kafka::member_id()),
                    member->id(),
                    std::move(md));

                  kglog.trace(
                    "set join response for member {} reply {}", member, reply);

                  try_finish_joining_member(member, std::move(reply));
                  schedule_next_heartbeat_expiration(member);
                  member->set_new(false);
              });
        }
    }
}

void group::heartbeat_expire(
  kafka::member_id member_id, clock_type::time_point deadline) {
    if (in_state(group_state::dead)) {
        kglog.trace("heartbeat expire for dead group");

    } else if (contains_pending_member(member_id)) {
        kglog.trace("heartbeat expire for pending member");
        remove_pending_member(member_id);

    } else if (!contains_member(member_id)) {
        kglog.trace("heartbeat expire for unknown member");

    } else {
        auto member = get_member(member_id);
        if (!member->should_keep_alive(
              deadline, _conf.group_new_member_join_timeout())) {
            remove_member(member);
        }
    }
}

void group::try_finish_joining_member(
  member_ptr member, join_group_response&& response) {
    if (member->is_joining()) {
        member->set_join_response(std::move(response));
        _num_members_joining--;
    }
}

void group::schedule_next_heartbeat_expiration(member_ptr member) {
    member->expire_timer().cancel();
    auto now = clock_type::now();
    member->set_latest_heartbeat(now);
    auto deadline = now + member->session_timeout();
    member->expire_timer().set_callback(
      [this, deadline, member_id = member->id()]() {
          heartbeat_expire(member_id, deadline);
      });
    member->expire_timer().arm(deadline);
}

void group::remove_pending_member(const kafka::member_id& member_id) {
    _pending_members.erase(member_id);
    if (in_state(group_state::preparing_rebalance)) {
        if (_join_timer.armed() && all_members_joined()) {
            _join_timer.cancel();
            complete_join();
        }
    }
}

void group::remove_member(member_ptr member) {
    // <kafka>New members may timeout with a pending JoinGroup while the group
    // is still rebalancing, so we have to invoke the callback before removing
    // the member. We return UNKNOWN_MEMBER_ID so that the consumer will retry
    // the JoinGroup request if is still active.</kafka>
    try_finish_joining_member(
      member, _make_join_error(no_member, error_code::unknown_member_id));

    // TODO: (not blocker) avoid the double look-up. we could do the removal
    // from the index in the caller and then pass in the shared_ptr.
    auto it = _members.find(member->id());
    if (it != _members.end()) {
        auto member = it->second;
        member->for_each_protocol([this, member](const member_protocol& p) {
            _supported_protocols[p.name]--;
            if (member->is_joining()) {
                _num_members_joining--;
            }
        });
        _members.erase(it);
    }

    if (is_leader(member->id())) {
        if (!_members.empty()) {
            _leader = _members.begin()->first;
        } else {
            _leader = std::nullopt;
        }
    }

    switch (state()) {
    case group_state::preparing_rebalance:
        // TODO: (not blocker) this doesn't distinguish between normal and
        // debounced join delays. in order to fix that, we need to introduce an
        // abstraction over the timer that implements the decision below for
        // each scenario.  all other checks like this have the same issue.
        if (_join_timer.armed() && all_members_joined()) {
            _join_timer.cancel();
            complete_join();
        }
        break;

    case group_state::stable:
        [[fallthrough]];
    case group_state::completing_rebalance:
        try_prepare_rebalance();
        break;

    case group_state::empty:
        [[fallthrough]];
    case group_state::dead:
        break;

    default:
        std::terminate(); // make gcc happy
    }
}

future<sync_group_response> group::handle_sync_group(sync_group_request&& r) {
    if (in_state(group_state::dead)) {
        kglog.trace("group is dead");
        return make_sync_error(error_code::coordinator_not_available);

    } else if (!contains_member(r.member_id)) {
        kglog.trace("member not found");
        return make_sync_error(error_code::unknown_member_id);

    } else if (r.generation_id != generation()) {
        kglog.trace(
          "invalid generation request {} != group {}",
          r.generation_id,
          generation());
        return make_sync_error(error_code::illegal_generation);
    }

    // the two states of interest are `completing rebalance` and `stable`.
    //
    // in the stable state all members have assignments. when a member makes
    // a `sync group` requests on a stable group that member's assignment is
    // returned immediately.
    //
    // when a `sync group` request is made on a group in the `completing
    // rebalance` state then the requesting member is either the group
    // leader which will register all member assignments with the group, or
    // it is a non-leader member which will wait until the leader request
    // provides the assignments.
    //
    // when the leader provides its assignments, it unblocks any members
    // waiting on assignments, transitions the group into the stable state,
    // and returns the assignment for itself.
    switch (state()) {
    case group_state::empty:
        kglog.trace("group is in the empty state");
        return make_sync_error(error_code::unknown_member_id);

    case group_state::preparing_rebalance:
        kglog.trace("group is in the preparing rebalance state");
        return make_sync_error(error_code::rebalance_in_progress);

    case group_state::completing_rebalance: {
        kglog.trace("completing rebalance");
        auto member = get_member(r.member_id);
        return sync_group_completing_rebalance(member, std::move(r));
    }

    case group_state::stable: {
        kglog.trace("group is stable. returning current assignment");
        // <kafka>if the group is stable, we just return the current
        // assignment</kafka>
        auto member = get_member(r.member_id);
        schedule_next_heartbeat_expiration(member);
        return make_ready_future<sync_group_response>(
          sync_group_response(error_code::none, member->assignment()));
    }

    case group_state::dead:
        // checked above
        [[fallthrough]];

    default:
        std::terminate(); // make gcc happy
    }
}

future<sync_group_response> group::sync_group_completing_rebalance(
  member_ptr member, sync_group_request&& r) {
    // this response will be set by the leader when it arrives. the leader also
    // sets its own response which reduces the special cases in the code, but we
    // also need to grab the future here before the corresponding promise is
    // destroyed after its value is set.
    auto response = member->get_sync_response();

    // wait for the leader to show up and fulfill the promise
    if (!is_leader(r.member_id)) {
        kglog.trace("non-leader member waiting for assignment");
        return response;
    }

    // construct a member assignment structure that will be persisted to the
    // underlying metadata topic for group recovery. the mapping is the
    // assignments in the request plus any missing assignments for group
    // members.
    auto assignments = std::move(r).member_assignments();
    add_missing_assignments(assignments);

    /*
     * TODO
     * Package up the member assignments and other group state into this
     * entry and send it off to be replicated on the group metadata
     * partition.
     */
#if 0
    raft::entry e(
      model::record_batch_type(1), model::record_batch_reader(nullptr));

    auto part = _partitions.get(group->ntp());
    return part->replicate(std::move(e))
#else
    auto part = make_ready_future<>();
    return part
#endif
    .then([this,
           response = std::move(response),
           expected_generation = generation(),
           assignments = std::move(assignments)]() mutable {
        // the group's state changed while waiting for this completion to
        // run. for example, another member joined. there's nothing to do
        // now except wait for an update.
        if (
          !in_state(group_state::completing_rebalance)
          || expected_generation != generation()) {
            return std::move(response);
        }

        // TODO: this is the error code from raft
        auto error = error_code::none;

        if (error == error_code::none) {
            // the group state was successfully persisted:
            //   - save the member assignments; clients may re-request
            //   - unblock any clients waiting on their assignment
            //   - transition the group to the stable state
            set_assignments(std::move(assignments));
            finish_syncing_members(error_code::none);
            set_state(group_state::stable);
        } else {
            // an error was encountered persisting the group state:
            //   - clear all the member assignments
            //   - propogate error back to waiting clients
            clear_assignments();
            finish_syncing_members(error);
        }

        return std::move(response);
    });
}

future<heartbeat_response> group::handle_heartbeat(heartbeat_request&& r) {
    if (in_state(group_state::dead)) {
        kglog.trace("group is dead");
        return make_heartbeat_error(error_code::coordinator_not_available);

    } else if (!contains_member(r.member_id)) {
        kglog.trace("member not found");
        return make_heartbeat_error(error_code::unknown_member_id);

    } else if (r.generation_id != generation()) {
        kglog.trace("generation does not match group");
        return make_heartbeat_error(error_code::illegal_generation);
    }

    switch (state()) {
    case group_state::empty:
        kglog.trace("group is in the empty state");
        return make_heartbeat_error(error_code::unknown_member_id);

    case group_state::completing_rebalance:
        kglog.trace("group is completing rebalance");
        return make_heartbeat_error(error_code::rebalance_in_progress);

    case group_state::preparing_rebalance: {
        auto member = get_member(r.member_id);
        schedule_next_heartbeat_expiration(member);
        return make_heartbeat_error(error_code::rebalance_in_progress);
    }

    case group_state::stable: {
        auto member = get_member(r.member_id);
        schedule_next_heartbeat_expiration(member);
        return make_heartbeat_error(error_code::none);
    }

    case group_state::dead:
        // checked above
        [[fallthrough]];

    default:
        std::terminate(); // mame gcc happy
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
        std::terminate(); // make gcc happy
    }
}

} // namespace kafka
