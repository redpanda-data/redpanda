// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/group.h"

#include "bytes/bytes.h"
#include "cluster/partition.h"
#include "cluster/simple_batch_builder.h"
#include "config/configuration.h"
#include "kafka/protocol/schemata/describe_groups_response.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/logger.h"
#include "likely.h"
#include "utils/to_string.h"
#include "vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_set.h>
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

using member_config = join_group_response_member;

group::group(
  kafka::group_id id,
  group_state s,
  config::configuration& conf,
  ss::lw_shared_ptr<cluster::partition> partition)
  : _id(id)
  , _state(s)
  , _state_timestamp(clock_type::now())
  , _generation(0)
  , _num_members_joining(0)
  , _new_member_added(false)
  , _conf(conf)
  , _partition(partition) {}

group::group(
  kafka::group_id id,
  group_log_group_metadata& md,
  config::configuration& conf,
  ss::lw_shared_ptr<cluster::partition> partition)
  : _id(id)
  , _num_members_joining(0)
  , _new_member_added(false)
  , _conf(conf)
  , _partition(partition) {
    _state = md.members.empty() ? group_state::empty : group_state::stable;
    _generation = md.generation;
    _protocol_type = md.protocol_type;
    _protocol = md.protocol;
    _leader = md.leader;
    _state_timestamp = clock_type::time_point(
      std::chrono::milliseconds(md.state_timestamp));

    for (auto& m : md.members) {
        auto member = ss::make_lw_shared<group_member>(std::move(m), id);
        add_member_no_join(member);
    }
}

bool group::valid_previous_state(group_state s) const {
    using g = group_state;

    switch (s) {
    case g::empty:
        [[fallthrough]];
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
    klog.trace("group state transition {} -> {}", _state, s);
    if (!valid_previous_state(s)) {
        std::terminate();
    }
    _state_timestamp = clock_type::now();
    return std::exchange(_state, s);
}

bool group::supports_protocols(const join_group_request& r) {
    // first member decides so make sure its defined
    if (in_state(group_state::empty)) {
        return !r.data.protocol_type().empty() && !r.data.protocols.empty();
    }

    if (!_protocol_type || *_protocol_type != r.data.protocol_type) {
        return false;
    }

    // check that at least one of the protocols in the request is supported
    // by all other group members.
    return std::any_of(
      r.data.protocols.cbegin(),
      r.data.protocols.cend(),
      [this](const kafka::join_group_request_protocol& p) {
          auto it = _supported_protocols.find(p.name);
          return it != _supported_protocols.end()
                 && (size_t)it->second == _members.size();
      });
}

void group::add_member_no_join(member_ptr member) {
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

    for (auto& p : member->protocols()) {
        _supported_protocols[p.name]++;
    }
}

ss::future<join_group_response> group::add_member(member_ptr member) {
    add_member_no_join(member);
    _num_members_joining++;
    return member->get_join_response();
}

ss::future<join_group_response> group::update_member(
  member_ptr member, std::vector<member_protocol>&& new_protocols) {
    /*
     * before updating the member, subtract its existing protocols from
     * group-level aggregate tracking. finally, update the group to reflect the
     * new protocols.
     */
    for (auto& p : member->protocols()) {
        auto& count = _supported_protocols[p.name];
        --count;
        vassert(count >= 0, "supported protocols cannot be negative");
    }
    member->set_protocols(std::move(new_protocols));
    for (auto& p : member->protocols()) {
        _supported_protocols[p.name]++;
    }

    if (!member->is_joining()) {
        _num_members_joining++;
        return member->get_join_response();
    }

    return ss::make_exception_future<join_group_response>(std::runtime_error(
      fmt::format("updating non-joining member {}", member->id())));
}

group::duration_type group::rebalance_timeout() const {
    auto it = std::max_element(
      std::cbegin(_members),
      std::cend(_members),
      [](const member_map::value_type& a, const member_map::value_type& b) {
          return a.second->rebalance_timeout() < b.second->rebalance_timeout();
      });
    if (likely(it != _members.end())) {
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
          auto metadata = m.second->get_protocol_metadata(*_protocol);
          return member_config{
            .member_id = m.first,
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
    klog.trace(
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
    klog.trace("selecting group protocol");

    // index of protocols supported by all members
    absl::flat_hash_set<kafka::protocol_name> candidates;
    std::for_each(
      std::cbegin(_supported_protocols),
      std::cend(_supported_protocols),
      [this, &candidates](const protocol_support::value_type& p) mutable {
          if ((size_t)p.second == _members.size()) {
              candidates.insert(p.first);
              klog.trace("candidate: {}", p.first);
          }
      });

    // collect votes from members
    protocol_support votes;
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [&votes, &candidates](const member_map::value_type& m) mutable {
          auto& choice = m.second->vote_for_protocol(candidates);
          auto total = ++votes[choice];
          klog.trace(
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
    klog.trace("selected group protocol {}", winner->first);
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
              klog.trace("set sync response for member {}", member);
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
        klog.trace("group has no leader");
        return false;
    }

    auto leader = get_member(*_leader);
    if (leader->is_joining()) {
        klog.trace("leader has rejoined");
        return true;
    } else {
        klog.trace("leader has not rejoined {}", *_leader);
    }

    // look for a replacement
    auto it = std::find_if(
      std::cbegin(_members),
      std::cend(_members),
      [](const member_map::value_type& member) {
          return member.second->is_joining();
      });

    if (it == _members.end()) {
        klog.trace("group has no leader replacement");
        return false;
    } else {
        _leader = it->first;
        klog.trace("selected new leader {}", *_leader);
        return true;
    }
}

ss::future<join_group_response>
group::handle_join_group(join_group_request&& r, bool is_new_group) {
    auto ret = ss::make_ready_future<join_group_response>(
      join_group_response(error_code::none));

    if (r.data.member_id == unknown_member_id) {
        ret = join_group_unknown_member(std::move(r));
    } else {
        ret = join_group_known_member(std::move(r));
    }

    // TODO: move the logic in this method up to group manager to make the
    // handling of is_new_group etc.. clearner rather than passing these flags
    // down into the group-level handler.
    if (!is_new_group && in_state(group_state::preparing_rebalance)) {
        /*
         * - after join_group completes the group may be in a state where the
         * join phase can complete immediately rather than waiting on the join
         * timer to fire. thus, we want to check if thats the case before
         * returning. however, we can also be in a completable state but want to
         * delay anyway for debouncing optimization. currently we debounce but
         * we don't have a way to distinguish between these scenarios. this
         * identification is needed in other area for optimization heuristics.
         * it won't affect correctness.
         *
         * while it is true that it doesn't affect correctness, what i failed to
         * take into account was the tolerance for timeouts by clients. this
         * handles the case that all clients join after a rebalance, but the
         * last client doesn't complete the join (see the case in
         * try_complete_join where we return immedaitely rather than completing
         * the join if we are in the preparing rebalance state). this check
         * handles that before returning.
         */
        if (all_members_joined()) {
            _join_timer.cancel();
            complete_join();
        }
    }

    return ret;
}

ss::future<join_group_response>
group::join_group_unknown_member(join_group_request&& r) {
    klog.trace("unknown member joining group {}", *this);

    if (in_state(group_state::dead)) {
        klog.trace("group is in the dead state");
        return make_join_error(
          unknown_member_id, error_code::coordinator_not_available);

    } else if (!supports_protocols(r)) {
        klog.trace("requested protocols not supported by group");
        return make_join_error(
          unknown_member_id, error_code::inconsistent_group_protocol);
    }

    auto new_member_id = group::generate_member_id(r);

    // <kafka>Only return MEMBER_ID_REQUIRED error if joinGroupRequest version
    // is >= 4 and groupInstanceId is configured to unknown.</kafka>
    if (r.version >= api_version(4) && !r.data.group_instance_id) {
        // <kafka>If member id required (dynamic membership), register the
        // member in the pending member list and send back a response to
        // call for another join group request with allocated member id.
        // </kafka>
        add_pending_member(new_member_id);
        klog.trace("requesting member rejoin with new id {}", new_member_id);
        return make_join_error(new_member_id, error_code::member_id_required);
    } else {
        return add_member_and_rebalance(std::move(new_member_id), std::move(r));
    }
}

ss::future<join_group_response>
group::join_group_known_member(join_group_request&& r) {
    klog.trace("member {} joining group {}", r.data.member_id, *this);

    if (in_state(group_state::dead)) {
        klog.trace("group is in the dead state");
        return make_join_error(
          r.data.member_id, error_code::coordinator_not_available);

    } else if (!supports_protocols(r)) {
        klog.trace("requested protocols not supported by group");
        return make_join_error(
          r.data.member_id, error_code::inconsistent_group_protocol);

    } else if (contains_pending_member(r.data.member_id)) {
        klog.trace("making pending member a regular member");
        kafka::member_id new_member_id = std::move(r.data.member_id);
        return add_member_and_rebalance(std::move(new_member_id), std::move(r));

    } else if (!contains_member(r.data.member_id)) {
        klog.trace("member is not registered in the group");
        return make_join_error(r.data.member_id, error_code::unknown_member_id);
    }

    auto member = get_member(r.data.member_id);

    switch (state()) {
    case group_state::preparing_rebalance:
        return update_member_and_rebalance(member, std::move(r));

    case group_state::completing_rebalance:
        if (r.data.protocols == member->protocols()) {
            // <kafka>member is joining with the same metadata (which could be
            // because it failed to receive the initial JoinGroup response), so
            // just return current group information for the current
            // generation.</kafka>
            klog.trace("resending the member join group response");

            // the leader receives group member metadata
            std::vector<member_config> members;
            if (is_leader(r.data.member_id)) {
                members = member_metadata();
            }

            join_group_response response(
              error_code::none,
              generation(),
              protocol().value_or(protocol_name("")),
              leader().value_or(member_id("")),
              std::move(r.data.member_id),
              std::move(members));

            return ss::make_ready_future<join_group_response>(
              std::move(response));

        } else {
            // <kafka>member has changed metadata, so force a rebalance</kafka>
            klog.trace("member rejoined while completing rebalance");
            return update_member_and_rebalance(member, std::move(r));
        }

    case group_state::stable:
        if (
          is_leader(r.data.member_id)
          || r.data.protocols != member->protocols()) {
            // <kafka>force a rebalance if a member has changed metadata or if
            // the leader sends JoinGroup. The latter allows the leader to
            // trigger rebalances for changes affecting assignment which do not
            // affect the member metadata (such as topic metadata changes for
            // the consumer)</kafka>
            klog.trace(
              "member rejoining (leader={}) stable group causing rebalance",
              (is_leader(r.data.member_id) ? "yes" : "no"));

            return update_member_and_rebalance(member, std::move(r));

        } else {
            // <kafka>for followers with no actual change to their metadata,
            // just return group information for the current generation which
            // will allow them to issue SyncGroup</kafka>
            klog.trace("follower rejoined stable group with identical state");

            join_group_response response(
              error_code::none,
              generation(),
              protocol().value_or(protocol_name("")),
              leader().value_or(member_id("")),
              std::move(r.data.member_id));

            return ss::make_ready_future<join_group_response>(
              std::move(response));
        }

    case group_state::empty:
        [[fallthrough]];

    case group_state::dead:
        klog.trace(
          "member {} rejoin in unexpected state {}", r.data.member_id, state());
        return make_join_error(r.data.member_id, error_code::unknown_member_id);

    default:
        std::terminate(); // make gcc happy
    }
}

ss::future<join_group_response> group::add_member_and_rebalance(
  kafka::member_id member_id, join_group_request&& r) {
    auto member = ss::make_lw_shared<group_member>(
      std::move(member_id),
      id(),
      std::move(r.data.group_instance_id),
      r.data.session_timeout_ms,
      r.data.rebalance_timeout_ms,
      std::move(r.data.protocol_type),
      r.native_member_protocols());

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
    klog.trace("added member {} to group {}", member, *this);
    _pending_members.erase(member->id());

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

ss::future<join_group_response>
group::update_member_and_rebalance(member_ptr member, join_group_request&& r) {
    auto response = update_member(member, r.native_member_protocols());
    try_prepare_rebalance();
    return response;
}

void group::try_prepare_rebalance() {
    if (!valid_previous_state(group_state::preparing_rebalance)) {
        klog.trace("skipping prepare rebalance state={}", state());
        return;
    }

    // <kafka>if any members are awaiting sync, cancel their request and have
    // them rejoin.</kafka>
    if (in_state(group_state::completing_rebalance)) {
        klog.trace("requesting rejoin from syncing members");
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
                  klog.trace(
                    "rearming debounce timer for {}ms after new member join. "
                    "remaining {}ms",
                    delay,
                    remaining);
                  _join_timer.arm(delay);
              } else {
                  klog.trace("completing join after debounce timer expiration");
                  complete_join();
              }
          });

        klog.trace("debouncing empty group join for {}ms", initial);
        _join_timer.arm(initial);

    } else if (all_members_joined()) {
        complete_join();

    } else {
        klog.trace(
          "not all members have joined cur {} waiting {} pending {}",
          _members.size(),
          _num_members_joining,
          _pending_members.size());
        auto timeout = rebalance_timeout();
        _join_timer.cancel();
        _join_timer.set_callback([this]() {
            vlog(
              klog.trace,
              "completing join on group rebalance timeout {}",
              id());
            complete_join();
        });
        klog.trace("scheduling join completion for {}ms", timeout);
        _join_timer.arm(timeout);
    }
}

/*
 * TODO: complete_join may dispatch a background write to raft. we need to (1)
 * gate that and (2) protect the group update in general with a semaphore since
 * atomicity spans continuations. this will be completed when the recovery code
 * is added.
 */
void group::complete_join() {
    klog.trace("completing join for group {}", *this);

    // <kafka>remove dynamic members who haven't joined the group yet</kafka>
    // this is the old group->remove_unjoined_members();
    for (auto it = _members.begin(); it != _members.end();) {
        if (!it->second->is_joining()) {
            vlog(klog.trace, "removing unjoined member {}", it->first);

            // cancel the heartbeat timer
            it->second->expire_timer().cancel();

            // update supported protocols count
            for (auto& p : it->second->protocols()) {
                auto& count = _supported_protocols[p.name];
                --count;
                vassert(count >= 0, "supported protocols cannot be negative");
            }

            auto leader = is_leader(it->second->id());
            _members.erase(it++);

            if (leader) {
                if (!_members.empty()) {
                    _leader = _members.begin()->first;
                } else {
                    _leader = std::nullopt;
                }
            }

        } else {
            ++it;
        }
    }

    if (in_state(group_state::dead)) {
        klog.trace("skipping join completion because group is dead");

    } else if (!leader_rejoined() && has_members()) {
        // <kafka>If all members are not rejoining, we will postpone the
        // completion of rebalance preparing stage, and send out another
        // delayed operation until session timeout removes all the
        // non-responsive members.</kafka>
        //
        // the callback needs to be reset because we may have arrived here via
        // the initial delayed callback which implements debouncing.
        klog.trace("could not complete rebalance because no members rejoined");
        auto timeout = rebalance_timeout();
        _join_timer.cancel();
        _join_timer.set_callback([this]() {
            vlog(
              klog.trace, "completing join after waiting for leader {}", id());
            complete_join();
        });
        _join_timer.arm(timeout);

    } else {
        advance_generation();

        if (in_state(group_state::empty)) {
            auto batch = checkpoint(assignments_type{});
            auto reader = model::make_memory_record_batch_reader(
              std::move(batch));
            (void)_partition
              ->replicate(
                std::move(reader),
                raft::replicate_options(raft::consistency_level::quorum_ack))
              .then([]([[maybe_unused]] result<raft::replicate_result> r) {});
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

                  klog.trace(
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
        klog.trace("heartbeat expire for dead group");

    } else if (contains_pending_member(member_id)) {
        klog.trace("heartbeat expire for pending member");
        remove_pending_member(member_id);

    } else if (!contains_member(member_id)) {
        klog.trace("heartbeat expire for unknown member");

    } else {
        auto member = get_member(member_id);
        if (!member->should_keep_alive(
              deadline, _conf.group_new_member_join_timeout())) {
            vlog(
              klog.trace,
              "expired member heartbeat. removing member {}",
              member->id());
            remove_member(member);
        }
    }
}

void group::try_finish_joining_member(
  member_ptr member, join_group_response&& response) {
    if (member->is_joining()) {
        member->set_join_response(std::move(response));
        _num_members_joining--;
        vassert(_num_members_joining >= 0, "negative members joining");
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
        for (auto& p : member->protocols()) {
            auto& count = _supported_protocols[p.name];
            --count;
            vassert(count >= 0, "supported protocols cannot be negative");
            if (member->is_joining()) {
                _num_members_joining--;
                vassert(_num_members_joining >= 0, "negative members joining");
            }
        }
        vlog(klog.trace, "removing member {}", member->id());
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

ss::future<sync_group_response>
group::handle_sync_group(sync_group_request&& r) {
    if (in_state(group_state::dead)) {
        klog.trace("group is dead");
        return make_sync_error(error_code::coordinator_not_available);

    } else if (!contains_member(r.data.member_id)) {
        klog.trace("member not found");
        return make_sync_error(error_code::unknown_member_id);

    } else if (r.data.generation_id != generation()) {
        klog.trace(
          "invalid generation request {} != group {}",
          r.data.generation_id,
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
        klog.trace("group is in the empty state");
        return make_sync_error(error_code::unknown_member_id);

    case group_state::preparing_rebalance:
        klog.trace("group is in the preparing rebalance state");
        return make_sync_error(error_code::rebalance_in_progress);

    case group_state::completing_rebalance: {
        klog.trace("completing rebalance");
        auto member = get_member(r.data.member_id);
        return sync_group_completing_rebalance(member, std::move(r));
    }

    case group_state::stable: {
        klog.trace("group is stable. returning current assignment");
        // <kafka>if the group is stable, we just return the current
        // assignment</kafka>
        auto member = get_member(r.data.member_id);
        schedule_next_heartbeat_expiration(member);
        return ss::make_ready_future<sync_group_response>(
          sync_group_response(error_code::none, member->assignment()));
    }

    case group_state::dead:
        // checked above
        [[fallthrough]];

    default:
        std::terminate(); // make gcc happy
    }
}

model::record_batch group::checkpoint(const assignments_type& assignments) {
    group_log_group_metadata gr;

    gr.protocol_type = protocol_type().value_or(kafka::protocol_type(""));
    gr.generation = generation();
    gr.protocol = protocol();
    gr.leader = leader();
    auto state_timestamp = _state_timestamp.time_since_epoch();
    gr.state_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                           state_timestamp)
                           .count();

    for (const auto& it : _members) {
        auto& member = it.second;
        auto state = it.second->state().copy();
        // this is not coming from the member itself because the checkpoint
        // occurs right before the members go live and get their assignments.
        state.assignment = bytes_to_iobuf(assignments.at(member->id()));
        gr.members.push_back(std::move(state));
    }

    cluster::simple_batch_builder builder(
      raft::data_batch_type, model::offset(0));

    group_log_record_key key{
      .record_type = group_log_record_key::type::group_metadata,
      .key = reflection::to_iobuf(_id),
    };

    builder.add_kv(std::move(key), std::move(gr));

    return std::move(builder).build();
}

ss::future<sync_group_response> group::sync_group_completing_rebalance(
  member_ptr member, sync_group_request&& r) {
    // this response will be set by the leader when it arrives. the leader also
    // sets its own response which reduces the special cases in the code, but we
    // also need to grab the future here before the corresponding promise is
    // destroyed after its value is set.
    auto response = member->get_sync_response();

    // wait for the leader to show up and fulfill the promise
    if (!is_leader(r.data.member_id)) {
        klog.trace("non-leader member waiting for assignment");
        return response;
    }

    // construct a member assignment structure that will be persisted to the
    // underlying metadata topic for group recovery. the mapping is the
    // assignments in the request plus any missing assignments for group
    // members.
    auto assignments = std::move(r).member_assignments();
    add_missing_assignments(assignments);

    auto batch = checkpoint(assignments);
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    return _partition
      ->replicate(
        std::move(reader),
        raft::replicate_options(raft::consistency_level::quorum_ack))
      .then([this,
             response = std::move(response),
             expected_generation = generation(),
             assignments = std::move(assignments)](
              result<raft::replicate_result> r) mutable {
          /*
           * the group's state has changed (e.g. another member joined). there's
           * nothing to do now except have the client wait for an update.
           */
          if (
            !in_state(group_state::completing_rebalance)
            || expected_generation != generation()) {
              klog.trace("sync group state changed");
              return std::move(response);
          }

          if (r) {
              klog.trace("sync group state success {}", id());
              // the group state was successfully persisted:
              //   - save the member assignments; clients may re-request
              //   - unblock any clients waiting on their assignment
              //   - transition the group to the stable state
              set_assignments(std::move(assignments));
              finish_syncing_members(error_code::none);
              set_state(group_state::stable);
          } else {
              klog.trace("sync group state failure {}", id());
              // an error was encountered persisting the group state:
              //   - clear all the member assignments
              //   - propogate error back to waiting clients
              clear_assignments();
              finish_syncing_members(error_code::not_coordinator);
              try_prepare_rebalance();
          }

          return std::move(response);
      });
}

ss::future<heartbeat_response> group::handle_heartbeat(heartbeat_request&& r) {
    if (in_state(group_state::dead)) {
        klog.trace("group is dead");
        return make_heartbeat_error(error_code::coordinator_not_available);

    } else if (!contains_member(r.data.member_id)) {
        klog.trace("member not found");
        return make_heartbeat_error(error_code::unknown_member_id);

    } else if (r.data.generation_id != generation()) {
        klog.trace("generation does not match group");
        return make_heartbeat_error(error_code::illegal_generation);
    }

    switch (state()) {
    case group_state::empty:
        klog.trace("group is in the empty state");
        return make_heartbeat_error(error_code::unknown_member_id);

    case group_state::completing_rebalance:
        klog.trace("group is completing rebalance");
        return make_heartbeat_error(error_code::rebalance_in_progress);

    case group_state::preparing_rebalance: {
        auto member = get_member(r.data.member_id);
        schedule_next_heartbeat_expiration(member);
        return make_heartbeat_error(error_code::rebalance_in_progress);
    }

    case group_state::stable: {
        auto member = get_member(r.data.member_id);
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

ss::future<leave_group_response>
group::handle_leave_group(leave_group_request&& r) {
    if (in_state(group_state::dead)) {
        klog.trace("group is dead");
        return make_leave_error(error_code::coordinator_not_available);

    } else if (contains_pending_member(r.data.member_id)) {
        // <kafka>if a pending member is leaving, it needs to be removed
        // from the pending list, heartbeat cancelled and if necessary,
        // prompt a JoinGroup completion.</kafka>
        klog.trace("pending member leaving group");
        remove_pending_member(r.data.member_id);
        return make_leave_error(error_code::none);

    } else if (!contains_member(r.data.member_id)) {
        klog.trace("member not found");
        return make_leave_error(error_code::unknown_member_id);

    } else {
        vlog(klog.trace, "member has left {}", r.data.member_id);
        auto member = get_member(r.data.member_id);
        member->expire_timer().cancel();
        remove_member(member);
        return make_leave_error(error_code::none);
    }
}

void group::complete_offset_commit(
  const model::topic_partition& tp, const offset_metadata& md) {
    // check if tp is pending
    auto p_it = _pending_offset_commits.find(tp);
    if (p_it != _pending_offset_commits.end()) {
        // save the tp commit if it hasn't yet been seen, or we are completing
        // for an instance that is newer based on log offset
        auto o_it = _offsets.find(tp);
        if (o_it == _offsets.end() || o_it->second.log_offset < md.log_offset) {
            _offsets[tp] = md;
        }

        // clear pending for this tp
        if (p_it->second.offset == md.offset) {
            _pending_offset_commits.erase(p_it);
        }
    }
}

void group::fail_offset_commit(
  const model::topic_partition& tp, const offset_metadata& md) {
    auto p_it = _pending_offset_commits.find(tp);
    if (p_it != _pending_offset_commits.end()) {
        // clear pending for this tp
        if (p_it->second.offset == md.offset) {
            _pending_offset_commits.erase(p_it);
        }
    }
}

ss::future<offset_commit_response>
group::store_offsets(offset_commit_request&& r) {
    cluster::simple_batch_builder builder(
      raft::data_batch_type, model::offset(0));

    std::vector<std::pair<model::topic_partition, offset_metadata>>
      offset_commits;

    for (const auto& t : r.data.topics) {
        for (const auto& p : t.partitions) {
            group_log_record_key key{
              .record_type = group_log_record_key::type::offset_commit,
              .key = reflection::to_iobuf(group_log_offset_key{
                _id,
                t.name,
                p.partition_index,
              }),
            };
            group_log_offset_metadata val{
              p.committed_offset,
              p.committed_leader_epoch,
              p.committed_metadata,
            };
            builder.add_kv(std::move(key), std::move(val));

            model::topic_partition tp(t.name, p.partition_index);
            offset_metadata md{
              .offset = p.committed_offset,
              .metadata = p.committed_metadata.value_or(""),
            };

            offset_commits.emplace_back(std::make_pair(tp, md));

            // record the offset commits as pending commits which will be
            // inspected after the append to catch concurrent updates.
            _pending_offset_commits[tp] = md;
        }
    }

    auto batch = std::move(builder).build();
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    return _partition
      ->replicate(
        std::move(reader),
        raft::replicate_options(raft::consistency_level::quorum_ack))
      .then([this, req = std::move(r), commits = std::move(offset_commits)](
              result<raft::replicate_result> r) mutable {
          error_code error = r ? error_code::none : error_code::not_coordinator;
          if (in_state(group_state::dead)) {
              return offset_commit_response(req, error);
          }

          if (error == error_code::none) {
              for (auto& e : commits) {
                  e.second.log_offset = r.value().last_offset;
                  complete_offset_commit(e.first, e.second);
              }
          } else {
              for (const auto& e : commits) {
                  fail_offset_commit(e.first, e.second);
              }
          }

          return offset_commit_response(req, error);
      });
}

ss::future<offset_commit_response>
group::handle_offset_commit(offset_commit_request&& r) {
    if (in_state(group_state::dead)) {
        return ss::make_ready_future<offset_commit_response>(
          offset_commit_response(r, error_code::coordinator_not_available));

    } else if (r.data.generation_id < 0 && in_state(group_state::empty)) {
        // <kafka>The group is only using Kafka to store offsets.</kafka>
        return store_offsets(std::move(r));

    } else if (!contains_member(r.data.member_id)) {
        return ss::make_ready_future<offset_commit_response>(
          offset_commit_response(r, error_code::unknown_member_id));

    } else if (r.data.generation_id != generation()) {
        return ss::make_ready_future<offset_commit_response>(
          offset_commit_response(r, error_code::illegal_generation));
    } else if (
      in_state(group_state::stable)
      || in_state(group_state::preparing_rebalance)) {
        // <kafka>During PreparingRebalance phase, we still allow a commit
        // request since we rely on heartbeat response to eventually notify the
        // rebalance in progress signal to the consumer</kafka>
        auto member = get_member(r.data.member_id);
        schedule_next_heartbeat_expiration(member);
        return store_offsets(std::move(r));
    } else if (in_state(group_state::completing_rebalance)) {
        return ss::make_ready_future<offset_commit_response>(
          offset_commit_response(r, error_code::rebalance_in_progress));
    } else {
        return ss::make_exception_future<offset_commit_response>(
          std::runtime_error(
            fmt::format("Unexpected group state {} for {}", _state, *this)));
    }
}

ss::future<offset_fetch_response>
group::handle_offset_fetch(offset_fetch_request&& r) {
    if (in_state(group_state::dead)) {
        return ss::make_ready_future<offset_fetch_response>(
          offset_fetch_response(r.data.topics));
    }

    offset_fetch_response resp;
    resp.data.error_code = error_code::none;

    // retrieve all topics available
    if (!r.data.topics) {
        absl::flat_hash_map<
          model::topic,
          std::vector<offset_fetch_response_partition>>
          tmp;
        for (const auto& e : _offsets) {
            offset_fetch_response_partition p = {
              .partition_index = e.first.partition,
              .committed_offset = e.second.offset,
              .metadata = e.second.metadata,
              .error_code = error_code::none,
            };
            tmp[e.first.topic].push_back(std::move(p));
        }
        for (auto& e : tmp) {
            resp.data.topics.push_back(
              {.name = e.first, .partitions = std::move(e.second)});
        }

        return ss::make_ready_future<offset_fetch_response>(std::move(resp));
    }

    // retrieve for the topics specified in the request
    for (const auto& topic : *r.data.topics) {
        offset_fetch_response_topic t;
        t.name = topic.name;
        for (auto id : topic.partition_indexes) {
            model::topic_partition tp(topic.name, id);
            auto res = offset(tp);
            if (res) {
                offset_fetch_response_partition p = {
                  .partition_index = id,
                  .committed_offset = res->offset,
                  .metadata = res->metadata,
                  .error_code = error_code::none,
                };
                t.partitions.push_back(std::move(p));
            } else {
                offset_fetch_response_partition p = {
                  .partition_index = id,
                  .committed_offset = model::offset(-1),
                  .metadata = "",
                  .error_code = error_code::none,
                };
                t.partitions.push_back(std::move(p));
            }
        }
        resp.data.topics.push_back(std::move(t));
    }

    return ss::make_ready_future<offset_fetch_response>(std::move(resp));
}

kafka::member_id group::generate_member_id(const join_group_request& r) {
    auto client_id = r.client_id ? *r.client_id : "";
    auto id = r.data.group_instance_id ? (*r.data.group_instance_id)()
                                       : client_id;
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return kafka::member_id(fmt::format("{}-{}", id, uuid));
}

described_group group::describe() const {
    described_group desc{
      .error_code = error_code::none,
      .group_id = id(),
      .group_state = group_state_to_kafka_name(state()),
      .protocol_type = protocol_type().value_or(kafka::protocol_type("")),
    };

    if (in_state(group_state::stable)) {
        if (!_protocol) {
            throw std::runtime_error(
              fmt::format("Stable group {} has no protocol", _id));
        }
        desc.protocol_data = *_protocol;
        for (const auto& it : _members) {
            desc.members.push_back(it.second->describe(*_protocol));
        }
    } else {
        for (const auto& it : _members) {
            desc.members.push_back(it.second->describe_without_metadata());
        }
    }

    return desc;
}

ss::future<error_code> group::remove() {
    switch (state()) {
    case group_state::dead:
        co_return error_code::group_id_not_found;

    case group_state::empty:
        set_state(group_state::dead);
        break;

    default:
        co_return error_code::non_empty_group;
    }

    // build offset tombstones
    storage::record_batch_builder builder(
      raft::data_batch_type, model::offset(0));

    for (auto& offset : _offsets) {
        group_log_record_key key{
          .record_type = group_log_record_key::type::offset_commit,
          .key = reflection::to_iobuf(group_log_offset_key{
            _id,
            offset.first.topic,
            offset.first.partition,
          }),
        };

        builder.add_raw_kv(reflection::to_iobuf(std::move(key)), std::nullopt);
    }

    // build group tombstone
    group_log_record_key key{
      .record_type = group_log_record_key::type::group_metadata,
      .key = reflection::to_iobuf(_id),
    };

    builder.add_raw_kv(reflection::to_iobuf(std::move(key)), std::nullopt);

    auto batch = std::move(builder).build();
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    try {
        auto result = co_await _partition->replicate(
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        if (result) {
            vlog(
              klog.trace,
              "Replicated group delete record {} at offset {}",
              _id,
              result.value().last_offset);
        } else {
            vlog(
              klog.error,
              "Error occured replicating group {} delete records {}",
              _id,
              result.error());
        }
    } catch (const std::exception& e) {
        vlog(
          klog.error,
          "Exception occured replicating group {} delete records {}",
          _id,
          e);
    }

    // kafka chooses to report no error even if replication fails. the in-memory
    // state on this node is still correctly represented.
    co_return error_code::none;
}

ss::future<>
group::remove_topic_partitions(const std::vector<model::topic_partition>& tps) {
    std::vector<std::pair<model::topic_partition, offset_metadata>> removed;
    for (const auto& tp : tps) {
        _pending_offset_commits.erase(tp);
        if (auto offset = _offsets.extract(tp); offset) {
            removed.emplace_back(
              std::move(offset.key()), std::move(offset.mapped()));
        }
    }

    // if no members and no offsets
    if (
      in_state(group_state::empty) && _pending_offset_commits.empty()
      && _offsets.empty()) {
        vlog(
          klog.debug,
          "Marking group {} as dead at {} generation",
          _id,
          generation());

        set_state(group_state::dead);
    }

    if (removed.empty()) {
        co_return;
    }

    _offsets.rehash(0);
    _pending_offset_commits.rehash(0);

    // build offset tombstones
    storage::record_batch_builder builder(
      raft::data_batch_type, model::offset(0));

    // create deletion records for offsets from deleted partitions
    for (auto& offset : removed) {
        vlog(
          klog.trace, "Removing offset for group {} tp {}", _id, offset.first);

        group_log_record_key key{
          .record_type = group_log_record_key::type::offset_commit,
          .key = reflection::to_iobuf(group_log_offset_key{
            _id,
            offset.first.topic,
            offset.first.partition,
          }),
        };

        builder.add_raw_kv(reflection::to_iobuf(std::move(key)), std::nullopt);
    }

    // gc the group?
    if (in_state(group_state::dead) && generation() > 0) {
        group_log_record_key key{
          .record_type = group_log_record_key::type::group_metadata,
          .key = reflection::to_iobuf(_id),
        };
        builder.add_raw_kv(reflection::to_iobuf(std::move(key)), std::nullopt);
    }

    auto batch = std::move(builder).build();
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    try {
        auto result = co_await _partition->replicate(
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        if (result) {
            vlog(
              klog.trace,
              "Replicated group cleanup record {} at offset {}",
              _id,
              result.value().last_offset);
        } else {
            vlog(
              klog.error,
              "Error occured replicating group {} cleanup records {}",
              _id,
              result.error());
        }
    } catch (const std::exception& e) {
        vlog(
          klog.error,
          "Exception occured replicating group {} cleanup records {}",
          _id,
          e);
    }
}

std::ostream& operator<<(std::ostream& o, const group& g) {
    fmt::print(
      o,
      "id={} state={} gen={} proto_type={} proto={} leader={} "
      "empty={} ntp=",
      g.id(),
      g.state(),
      g.generation(),
      g.protocol_type(),
      g.protocol(),
      g.leader(),
      !g.has_members());
    if (g._partition) {
        o << g._partition->ntp();
    } else {
        o << "<none>";
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, group_state gs) {
    return o << group_state_to_kafka_name(gs);
}

ss::sstring group_state_to_kafka_name(group_state gs) {
    // State names are written in camel case to match the formatting of kafka
    // since these states are returned through the kafka describe groups api.
    switch (gs) {
    case group_state::empty:
        return "Empty";
    case group_state::preparing_rebalance:
        return "PreparingRebalance";
    case group_state::completing_rebalance:
        return "CompletingRebalance";
    case group_state::stable:
        return "Stable";
    case group_state::dead:
        return "Dead";
    default:
        std::terminate(); // make gcc happy
    }
}

} // namespace kafka
