// Copyright 2020 Redpanda Data, Inc.
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
#include "cluster/tx_utils.h"
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/protocol/schemata/describe_groups_response.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/logger.h"
#include "kafka/types.h"
#include "likely.h"
#include "model/fundamental.h"
#include "raft/errc.h"
#include "storage/record_batch_builder.h"
#include "utils/to_string.h"
#include "vassert.h"

#include <seastar/core/coroutine.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

namespace kafka {

using member_config = join_group_response_member;

using violation_recovery_policy = model::violation_recovery_policy;

group::group(
  kafka::group_id id,
  group_state s,
  config::configuration& conf,
  ss::lw_shared_ptr<cluster::partition> partition,
  group_metadata_serializer serializer,
  enable_group_metrics group_metrics)
  : _id(std::move(id))
  , _state(s)
  , _state_timestamp(model::timestamp::now())
  , _generation(0)
  , _num_members_joining(0)
  , _new_member_added(false)
  , _conf(conf)
  , _partition(std::move(partition))
  , _recovery_policy(
      config::shard_local_cfg().rm_violation_recovery_policy.value())
  , _ctxlog(klog, *this)
  , _ctx_txlog(cluster::txlog, *this)
  , _md_serializer(std::move(serializer))
  , _enable_group_metrics(group_metrics) {}

group::group(
  kafka::group_id id,
  group_metadata_value& md,
  config::configuration& conf,
  ss::lw_shared_ptr<cluster::partition> partition,
  group_metadata_serializer serializer,
  enable_group_metrics group_metrics)
  : _id(std::move(id))
  , _num_members_joining(0)
  , _new_member_added(false)
  , _conf(conf)
  , _partition(std::move(partition))
  , _recovery_policy(
      config::shard_local_cfg().rm_violation_recovery_policy.value())
  , _ctxlog(klog, *this)
  , _ctx_txlog(cluster::txlog, *this)
  , _md_serializer(std::move(serializer))
  , _enable_group_metrics(group_metrics) {
    _state = md.members.empty() ? group_state::empty : group_state::stable;
    _generation = md.generation;
    _protocol_type = md.protocol_type;
    _protocol = md.protocol;
    _leader = md.leader;
    _state_timestamp = md.state_timestamp;
    for (auto& m : md.members) {
        auto member = ss::make_lw_shared<group_member>(
          std::move(m),
          id,
          _protocol_type.value(),
          std::vector<kafka::member_protocol>{member_protocol{
            .name = _protocol.value_or(protocol_name("")),
            .metadata = iobuf_to_bytes(m.subscription),
          }});
        vlog(_ctxlog.trace, "Initializing group with member {}", member);
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
    }

    __builtin_unreachable();
}

template<typename T>
static model::record_batch make_tx_batch(
  model::record_batch_type type,
  int8_t version,
  const model::producer_identity& pid,
  T cmd) {
    iobuf key;
    reflection::serialize(key, type, pid.id);

    iobuf value;
    reflection::serialize(value, version);
    reflection::serialize(value, std::move(cmd));

    storage::record_batch_builder builder(type, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kv(std::move(key), std::move(value));

    return std::move(builder).build();
}

group_state group::set_state(group_state s) {
    vassert(
      valid_previous_state(s),
      "Group {} invalid state transition from {} to {}",
      _id,
      _state,
      s);
    vlog(_ctxlog.trace, "Changing state from {} to {}", _state, s);
    _state_timestamp = model::timestamp::now();
    return std::exchange(_state, s);
}

bool group::supports_protocols(const join_group_request& r) const {
    vlog(
      _ctxlog.trace,
      "Check protocol support type {} members {} req.type {} req.protos {} "
      "supported {}",
      _protocol_type,
      _members.size(),
      r.data.protocol_type,
      r.data.protocols,
      fmt::join(_supported_protocols, ", "));

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
        vlog(
          _ctxlog.trace,
          "Cannot add member with duplicate id {}",
          member->id());
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
    vlog(
      _ctxlog.trace,
      "Updating {}joining member {} with protocols {}",
      member->is_joining() ? "" : "non-",
      member,
      new_protocols);

    /*
     * before updating the member, subtract its existing protocols from
     * group-level aggregate tracking. finally, update the group to reflect the
     * new protocols.
     */
    for (auto& p : member->protocols()) {
        auto& count = _supported_protocols[p.name];
        --count;
        vassert(
          count >= 0,
          "Invalid protocol support count {} for group {}",
          count,
          *this);
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
        vlog(_ctxlog.trace, "Cannot compute rebalance timeout for empty group");
        throw std::runtime_error("no members in group");
    }
}

std::vector<member_config> group::member_metadata() const {
    if (
      in_state(group_state::dead)
      || in_state(group_state::preparing_rebalance)) {
        vlog(
          _ctxlog.trace,
          "Cannot collect member metadata in group state {}",
          _state);
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
    vlog(_ctxlog.trace, "Advanced generation with protocol {}", _protocol);
}

/*
 * TODO
 *   - it is reasonable to try and consolidate the vote collection and final
 *   result reduction for efficiency as long as its done without lots of string
 *   creation / destruction.
 */
kafka::protocol_name group::select_protocol() const {
    // index of protocols supported by all members
    absl::flat_hash_set<kafka::protocol_name> candidates;
    std::for_each(
      std::cbegin(_supported_protocols),
      std::cend(_supported_protocols),
      [this, &candidates](const protocol_support::value_type& p) mutable {
          if ((size_t)p.second == _members.size()) {
              candidates.insert(p.first);
          }
      });

    vlog(_ctxlog.trace, "Selecting protocol from candidates {}", candidates);

    // collect votes from members
    protocol_support votes;
    std::for_each(
      std::cbegin(_members),
      std::cend(_members),
      [this, &votes, &candidates](const member_map::value_type& m) mutable {
          auto& choice = m.second->vote_for_protocol(candidates);
          auto total = ++votes[choice];
          vlog(
            _ctxlog.trace,
            "Member {} voting for protocol {} (total {})",
            m.first,
            choice,
            total);
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
    vlog(_ctxlog.trace, "Selected protocol {}", winner->first);
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
              vlog(
                _ctxlog.trace,
                "Completed syncing member {} with reply {}",
                member,
                reply);
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
        return false;
    }

    auto leader = get_member(*_leader);
    if (leader->is_joining()) {
        vlog(_ctxlog.trace, "Leader {} has rejoined", *_leader);
        return true;
    }

    // look for a replacement
    auto it = std::find_if(
      std::cbegin(_members),
      std::cend(_members),
      [](const member_map::value_type& member) {
          return member.second->is_joining();
      });

    if (it == _members.end()) {
        vlog(_ctxlog.trace, "No replacement leader is available");
        return false;
    } else {
        _leader = it->first;
        vlog(_ctxlog.trace, "Selected new leader {}", *_leader);
        return true;
    }
}

ss::future<join_group_response>
group::handle_join_group(join_group_request&& r, bool is_new_group) {
    vlog(
      _ctxlog.trace,
      "Handling join request {} for {} group {}",
      r,
      (is_new_group ? "new" : "existing"),
      *this);

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
            vlog(_ctxlog.trace, "Finishing join with all members present");
            _join_timer.cancel();
            complete_join();
        }
    }

    return ret;
}

// DONE
ss::future<join_group_response>
group::join_group_unknown_member(join_group_request&& r) {
    if (in_state(group_state::dead)) {
        vlog(_ctxlog.trace, "Join rejected in state {}", _state);
        return make_join_error(
          unknown_member_id, error_code::coordinator_not_available);

    } else if (!supports_protocols(r)) {
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
        vlog(
          _ctxlog.trace,
          "Requesting rejoin for unknown member with new id {}",
          new_member_id);
        add_pending_member(new_member_id, r.data.session_timeout_ms);
        return make_join_error(new_member_id, error_code::member_id_required);
    } else {
        return add_member_and_rebalance(std::move(new_member_id), std::move(r));
    }
}

ss::future<join_group_response>
group::join_group_known_member(join_group_request&& r) {
    if (in_state(group_state::dead)) {
        vlog(
          _ctxlog.trace,
          "Join rejected in state {} for {}",
          _state,
          r.data.member_id);
        return make_join_error(
          r.data.member_id, error_code::coordinator_not_available);

    } else if (!supports_protocols(r)) {
        return make_join_error(
          r.data.member_id, error_code::inconsistent_group_protocol);

    } else if (contains_pending_member(r.data.member_id)) {
        kafka::member_id new_member_id = std::move(r.data.member_id);
        return add_member_and_rebalance(std::move(new_member_id), std::move(r));

    } else if (!contains_member(r.data.member_id)) {
        vlog(
          _ctxlog.trace,
          "Join rejected for unregistered member {}",
          r.data.member_id);
        return make_join_error(r.data.member_id, error_code::unknown_member_id);
    }

    auto member = get_member(r.data.member_id);

    vlog(
      _ctxlog.trace,
      "Handling join for {}leader member {}",
      is_leader(member->id()) ? "" : "non-",
      member);

    switch (state()) {
    case group_state::preparing_rebalance:
        return update_member_and_rebalance(member, std::move(r));

    case group_state::completing_rebalance:
        if (r.data.protocols == member->protocols()) {
            // <kafka>member is joining with the same metadata (which could be
            // because it failed to receive the initial JoinGroup response), so
            // just return current group information for the current
            // generation.</kafka>

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

            vlog(
              _ctxlog.trace,
              "Resending join response for member {} reply {}",
              member->id(),
              response);

            return ss::make_ready_future<join_group_response>(
              std::move(response));

        } else {
            // <kafka>member has changed metadata, so force a rebalance</kafka>
            vlog(_ctxlog.trace, "Rebalancing due to protocol change");
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
            vlog(_ctxlog.trace, "Rebalancing due to leader or protocol change");
            return update_member_and_rebalance(member, std::move(r));

        } else {
            // <kafka>for followers with no actual change to their metadata,
            // just return group information for the current generation which
            // will allow them to issue SyncGroup</kafka>
            join_group_response response(
              error_code::none,
              generation(),
              protocol().value_or(protocol_name("")),
              leader().value_or(member_id("")),
              std::move(r.data.member_id));

            vlog(_ctxlog.trace, "Handling idemponent group join {}", response);

            return ss::make_ready_future<join_group_response>(
              std::move(response));
        }

    case group_state::empty:
        [[fallthrough]];

    case group_state::dead:
        return make_join_error(r.data.member_id, error_code::unknown_member_id);
    }

    __builtin_unreachable();
}

ss::future<join_group_response> group::add_member_and_rebalance(
  kafka::member_id member_id, join_group_request&& r) {
    auto member = ss::make_lw_shared<group_member>(
      std::move(member_id),
      id(),
      std::move(r.data.group_instance_id),
      r.client_id.value_or(client_id("")),
      r.client_host,
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

    vlog(
      _ctxlog.trace,
      "Added member {} with join timeout {} ms to group {}",
      member,
      _conf.group_new_member_join_timeout(),
      *this);

    try_prepare_rebalance();
    return response;
}

ss::future<join_group_response>
group::update_member_and_rebalance(member_ptr member, join_group_request&& r) {
    auto response = update_member(
      std::move(member), r.native_member_protocols());
    try_prepare_rebalance();
    return response;
}

void group::try_prepare_rebalance() {
    if (!valid_previous_state(group_state::preparing_rebalance)) {
        vlog(_ctxlog.trace, "Cannot prepare rebalance in state {}", _state);
        return;
    }

    // <kafka>if any members are awaiting sync, cancel their request and have
    // them rejoin.</kafka>
    if (in_state(group_state::completing_rebalance)) {
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
                  vlog(
                    _ctxlog.trace,
                    "Scheduling debounce join timer for {} ms remaining {} ms",
                    delay,
                    remaining);
                  _join_timer.arm(delay);
              } else {
                  complete_join();
              }
          });

        vlog(
          _ctxlog.trace,
          "Scheduling initial debounce join timer for {} ms",
          initial);

        _join_timer.arm(initial);

    } else if (all_members_joined()) {
        vlog(_ctxlog.trace, "All members have joined");
        complete_join();

    } else {
        auto timeout = rebalance_timeout();
        vlog(
          _ctxlog.trace,
          "Join completion scheduled in {} ms. Current members {} waiting {} "
          "pending {}",
          timeout,
          _members.size(),
          _num_members_joining,
          _pending_members.size());
        _join_timer.cancel();
        _join_timer.set_callback([this]() { complete_join(); });
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
    vlog(_ctxlog.trace, "Completing join for group {}", *this);

    // <kafka>remove dynamic members who haven't joined the group yet</kafka>
    // this is the old group->remove_unjoined_members();
    const auto prev_leader = _leader;
    for (auto it = _members.begin(); it != _members.end();) {
        if (!it->second->is_joining()) {
            vlog(_ctxlog.trace, "Removing unjoined member {}", it->first);

            // cancel the heartbeat timer
            it->second->expire_timer().cancel();

            // update supported protocols count
            for (auto& p : it->second->protocols()) {
                auto& count = _supported_protocols[p.name];
                --count;
                vassert(
                  count >= 0,
                  "Invalid supported protocols {} in {}",
                  count,
                  *this);
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

    if (_leader != prev_leader) {
        vlog(
          _ctxlog.trace,
          "Leadership changed to {} from {}",
          _leader,
          prev_leader);
    }

    if (in_state(group_state::dead)) {
        vlog(_ctxlog.trace, "Cancelling join completion in state {}", _state);

    } else if (!leader_rejoined() && has_members()) {
        // <kafka>If all members are not rejoining, we will postpone the
        // completion of rebalance preparing stage, and send out another
        // delayed operation until session timeout removes all the
        // non-responsive members.</kafka>
        //
        // the callback needs to be reset because we may have arrived here via
        // the initial delayed callback which implements debouncing.
        auto timeout = rebalance_timeout();
        vlog(
          _ctxlog.trace,
          "No members re-joined. Scheduling completion for {} ms",
          timeout);
        _join_timer.cancel();
        _join_timer.set_callback([this]() { complete_join(); });
        _join_timer.arm(timeout);

    } else {
        advance_generation();

        if (in_state(group_state::empty)) {
            vlog(_ctxlog.trace, "Checkpointing empty group {}", *this);
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

                  vlog(
                    _ctxlog.trace,
                    "Completing join for {} with reply {}",
                    member->id(),
                    reply);

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
        vlog(
          _ctxlog.trace,
          "Ignoring heartbeat expiration for group state {}",
          _state);

    } else if (contains_pending_member(member_id)) {
        vlog(
          _ctxlog.trace,
          "Handling expired heartbeat for pending member {}",
          member_id);
        remove_pending_member(member_id);

    } else if (!contains_member(member_id)) {
        vlog(
          _ctxlog.trace,
          "Ignoring heartbeat expiration for unregistered member {}",
          member_id);

    } else {
        auto member = get_member(member_id);
        const auto keep_alive = member->should_keep_alive(
          deadline, _conf.group_new_member_join_timeout());
        vlog(
          _ctxlog.trace,
          "Heartbeat expired for keep_alive={} member {}",
          keep_alive,
          member_id);
        if (!keep_alive) {
            remove_member(member);
        }
    }
}

void group::try_finish_joining_member(
  member_ptr member, join_group_response&& response) {
    if (member->is_joining()) {
        vlog(
          _ctxlog.trace,
          "Finishing joining member {} with reply {}",
          member->id(),
          response);
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
    vlog(
      _ctxlog.trace,
      "Scheduling heartbeat expiration {} ms for {}",
      member->session_timeout(),
      member->id());
    member->expire_timer().arm(deadline);
}

void group::remove_pending_member(kafka::member_id member_id) {
    _pending_members.erase(member_id);
    vlog(_ctxlog.trace, "Removing pending member {}", member_id);
    if (in_state(group_state::preparing_rebalance)) {
        if (_join_timer.armed() && all_members_joined()) {
            _join_timer.cancel();
            complete_join();
        }
    }
}

void group::shutdown() {
    // cancel join timer
    _join_timer.cancel();
    // cancel pending members timers
    for (auto& [_, timer] : _pending_members) {
        timer.cancel();
    }

    for (auto& [member_id, member] : _members) {
        member->expire_timer().cancel();
        if (member->is_syncing()) {
            member->set_sync_response(sync_group_response(
              error_code::not_coordinator, member->assignment()));
        } else if (member->is_joining()) {
            try_finish_joining_member(
              member, _make_join_error(member_id, error_code::not_coordinator));
        }
    }
}

void group::remove_member(member_ptr member) {
    vlog(_ctxlog.trace, "Removing member {}", member->id());

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
        _members.erase(it);
    }

    const auto prev_leader = _leader;
    if (is_leader(member->id())) {
        if (!_members.empty()) {
            _leader = _members.begin()->first;
        } else {
            _leader = std::nullopt;
        }
        if (_leader != prev_leader) {
            vlog(
              _ctxlog.trace,
              "Leadership changed to {} from {}",
              _leader,
              prev_leader);
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
    vlog(_ctxlog.trace, "Handling sync group request {}", r);

    if (in_state(group_state::dead)) {
        vlog(_ctxlog.trace, "Sync rejected for group state {}", _state);
        return make_sync_error(error_code::coordinator_not_available);

    } else if (!contains_member(r.data.member_id)) {
        vlog(
          _ctxlog.trace,
          "Sync rejected for unregistered member {}",
          r.data.member_id);
        return make_sync_error(error_code::unknown_member_id);

    } else if (r.data.generation_id != generation()) {
        vlog(
          _ctxlog.trace,
          "Sync rejected with out-of-date generation {} != {}",
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
        vlog(_ctxlog.trace, "Sync rejected for group state {}", _state);
        return make_sync_error(error_code::unknown_member_id);

    case group_state::preparing_rebalance:
        vlog(_ctxlog.trace, "Sync rejected for group state {}", _state);
        return make_sync_error(error_code::rebalance_in_progress);

    case group_state::completing_rebalance: {
        auto member = get_member(r.data.member_id);
        return sync_group_completing_rebalance(member, std::move(r));
    }

    case group_state::stable: {
        // <kafka>if the group is stable, we just return the current
        // assignment</kafka>
        auto member = get_member(r.data.member_id);
        schedule_next_heartbeat_expiration(member);
        sync_group_response reply(error_code::none, member->assignment());
        vlog(
          _ctxlog.trace,
          "Handling idemponent group sync for member {} with reply {}",
          member,
          reply);
        return ss::make_ready_future<sync_group_response>(std::move(reply));
    }

    case group_state::dead:
        // checked above on method entry
        break;
    }

    __builtin_unreachable();
}

model::record_batch group::checkpoint(const assignments_type& assignments) {
    group_metadata_key key;
    key.group_id = _id;
    group_metadata_value metadata;

    metadata.protocol_type = protocol_type().value_or(kafka::protocol_type(""));
    metadata.generation = generation();
    metadata.protocol = protocol();
    metadata.leader = leader();
    metadata.state_timestamp = _state_timestamp;

    for (const auto& it : _members) {
        auto& member = it.second;
        auto state = it.second->state().copy();
        // this is not coming from the member itself because the checkpoint
        // occurs right before the members go live and get their assignments.
        state.assignment = bytes_to_iobuf(assignments.at(member->id()));
        state.subscription = bytes_to_iobuf(
          it.second->get_protocol_metadata(_protocol.value()));
        metadata.members.push_back(std::move(state));
    }

    cluster::simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    auto kv = _md_serializer.to_kv(
      group_metadata_kv{.key = std::move(key), .value = std::move(metadata)});
    builder.add_raw_kv(std::move(kv.key), std::move(kv.value));

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
        vlog(
          _ctxlog.trace,
          "Non-leader member waiting for assignment {}",
          member->id());
        return response;
    }

    vlog(_ctxlog.trace, "Completing group sync with leader {}", member->id());

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
              vlog(_ctxlog.trace, "Group state changed while completing sync");
              return std::move(response);
          }

          if (r) {
              // the group state was successfully persisted:
              //   - save the member assignments; clients may re-request
              //   - unblock any clients waiting on their assignment
              //   - transition the group to the stable state
              set_assignments(std::move(assignments));
              finish_syncing_members(error_code::none);
              set_state(group_state::stable);
              vlog(_ctxlog.trace, "Successfully completed group sync");
          } else {
              vlog(
                _ctxlog.trace,
                "An error occurred completing group sync {}",
                r.error());
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
    vlog(_ctxlog.trace, "Handling heartbeat request {}", r);

    if (in_state(group_state::dead)) {
        vlog(_ctxlog.trace, "Heartbeat rejected for group state {}", _state);
        return make_heartbeat_error(error_code::coordinator_not_available);

    } else if (!contains_member(r.data.member_id)) {
        vlog(
          _ctxlog.trace,
          "Heartbeat rejected for unregistered member {}",
          r.data.member_id);
        return make_heartbeat_error(error_code::unknown_member_id);

    } else if (r.data.generation_id != generation()) {
        vlog(
          _ctxlog.trace,
          "Heartbeat rejected with out-of-date generation {} != {}",
          r.data.generation_id,
          generation());
        return make_heartbeat_error(error_code::illegal_generation);
    }

    switch (state()) {
    case group_state::empty:
        vlog(_ctxlog.trace, "Heartbeat rejected for group state {}", _state);
        return make_heartbeat_error(error_code::unknown_member_id);

    case group_state::completing_rebalance:
        vlog(_ctxlog.trace, "Heartbeat rejected for group state {}", _state);
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
        // checked above on method entry
        break;
    }

    __builtin_unreachable();
}

ss::future<leave_group_response>
group::handle_leave_group(leave_group_request&& r) {
    vlog(_ctxlog.trace, "Handling leave group request {}", r);

    if (in_state(group_state::dead)) {
        vlog(_ctxlog.trace, "Leave rejected for group state {}", _state);
        return make_leave_error(error_code::coordinator_not_available);

    } else if (contains_pending_member(r.data.member_id)) {
        // <kafka>if a pending member is leaving, it needs to be removed
        // from the pending list, heartbeat cancelled and if necessary,
        // prompt a JoinGroup completion.</kafka>
        remove_pending_member(r.data.member_id);
        return make_leave_error(error_code::none);

    } else if (!contains_member(r.data.member_id)) {
        vlog(
          _ctxlog.trace,
          "Leave rejected for unregistered member {}",
          r.data.member_id);
        return make_leave_error(error_code::unknown_member_id);

    } else {
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
        try_upsert_offset(tp, md);

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

void group::reset_tx_state(model::term_id term) {
    _term = term;
    _volatile_txs.clear();
}

void group::insert_prepared(prepared_tx tx) {
    auto pid = tx.pid;
    _prepared_txs[pid] = std::move(tx);
}

ss::future<cluster::commit_group_tx_reply>
group::commit_tx(cluster::commit_group_tx_request r) {
    // doesn't make sense to fence off a commit because transaction
    // manager has already decided to commit and acked to a client

    if (_partition->term() != _term) {
        co_return make_commit_tx_reply(cluster::tx_errc::stale);
    }

    auto prepare_it = _prepared_txs.find(r.pid);
    if (prepare_it == _prepared_txs.end()) {
        vlog(
          _ctx_txlog.trace,
          "can't find a tx {}, probably already comitted",
          r.pid);
        co_return make_commit_tx_reply(cluster::tx_errc::none);
    }

    if (prepare_it->second.tx_seq > r.tx_seq) {
        // rare situation:
        //   * tm_stm prepares (tx_seq+1)
        //   * prepare on this group passed but tm_stm failed to write to disk
        //   * during recovery tm_stm recommits (tx_seq)
        // existence of {pid, tx_seq+1} implies {pid, tx_seq} is committed
        vlog(
          _ctx_txlog.trace,
          "prepare for pid:{} has higher tx_seq:{} than given: {} => replaying "
          "already comitted commit",
          r.pid,
          prepare_it->second.tx_seq,
          r.tx_seq);
        co_return make_commit_tx_reply(cluster::tx_errc::none);
    } else if (prepare_it->second.tx_seq < r.tx_seq) {
        if (_recovery_policy == violation_recovery_policy::best_effort) {
            vlog(
              _ctx_txlog.error,
              "Rejecting commit with tx_seq:{} since prepare with lesser "
              "tx_seq:{} exists",
              r.tx_seq,
              prepare_it->second.tx_seq);
            co_return make_commit_tx_reply(cluster::tx_errc::request_rejected);
        } else {
            vassert(
              false,
              "Received commit with tx_seq:{} while prepare with lesser "
              "tx_seq:{} exists",
              r.tx_seq,
              prepare_it->second.tx_seq);
        }
    }

    // we commit only if a provided tx_seq matches prepared tx_seq

    group_log_commit_tx commit_tx;
    commit_tx.group_id = r.group_id;
    // TODO: https://app.clubhouse.io/vectorized/story/2200
    // include producer_id+type into key to make it unique-ish
    // to prevent being GCed by the compaction
    auto batch = make_tx_batch(
      model::record_batch_type::group_commit_tx,
      commit_tx_record_version,
      r.pid,
      std::move(commit_tx));

    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto e = co_await _partition->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!e) {
        co_return make_commit_tx_reply(cluster::tx_errc::unknown_server_error);
    }

    prepare_it = _prepared_txs.find(r.pid);
    if (prepare_it == _prepared_txs.end()) {
        vlog(
          _ctx_txlog.error,
          "can't find already observed prepared tx pid:{}",
          r.pid);
        co_return make_commit_tx_reply(cluster::tx_errc::unknown_server_error);
    }

    for (const auto& [tp, md] : prepare_it->second.offsets) {
        try_upsert_offset(tp, md);
    }

    _prepared_txs.erase(prepare_it);

    co_return make_commit_tx_reply(cluster::tx_errc::none);
}

cluster::begin_group_tx_reply make_begin_tx_reply(cluster::tx_errc ec) {
    cluster::begin_group_tx_reply reply;
    reply.ec = ec;
    return reply;
}

cluster::prepare_group_tx_reply make_prepare_tx_reply(cluster::tx_errc ec) {
    cluster::prepare_group_tx_reply reply;
    reply.ec = ec;
    return reply;
}

cluster::commit_group_tx_reply make_commit_tx_reply(cluster::tx_errc ec) {
    cluster::commit_group_tx_reply reply;
    reply.ec = ec;
    return reply;
}

cluster::abort_group_tx_reply make_abort_tx_reply(cluster::tx_errc ec) {
    cluster::abort_group_tx_reply reply;
    reply.ec = ec;
    return reply;
}

ss::future<cluster::begin_group_tx_reply>
group::begin_tx(cluster::begin_group_tx_request r) {
    if (_partition->term() != _term) {
        co_return make_begin_tx_reply(cluster::tx_errc::stale);
    }

    auto fence_it = _fence_pid_epoch.find(r.pid.get_id());
    if (
      fence_it != _fence_pid_epoch.end()
      && r.pid.get_epoch() < fence_it->second) {
        vlog(
          _ctx_txlog.error,
          "pid {} fenced out by epoch {}",
          r.pid,
          fence_it->second);
        co_return make_begin_tx_reply(cluster::tx_errc::fenced);
    }

    if (
      fence_it == _fence_pid_epoch.end()
      || r.pid.get_epoch() > fence_it->second) {
        group_log_fencing fence{.group_id = id()};

        // TODO: https://app.clubhouse.io/vectorized/story/2200
        // include producer_id into key to make it unique-ish
        // to prevent being GCed by the compaction
        auto batch = make_tx_batch(
          model::record_batch_type::tx_fence,
          fence_control_record_version,
          r.pid,
          std::move(fence));
        auto reader = model::make_memory_record_batch_reader(std::move(batch));
        auto e = co_await _partition->replicate(
          _term,
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));

        if (!e) {
            vlog(
              _ctx_txlog.error,
              "Error \"{}\" on replicating pid:{} fencing batch",
              e.error(),
              r.pid);
            co_return make_begin_tx_reply(
              cluster::tx_errc::unknown_server_error);
        }

        // _fence_pid_epoch may change while the method waits for the
        //  replicate coroutine to finish so the fence_it may become
        //  invalidated and we need to grab it again
        fence_it = _fence_pid_epoch.find(r.pid.get_id());
        if (fence_it == _fence_pid_epoch.end()) {
            _fence_pid_epoch.emplace(r.pid.get_id(), r.pid.get_epoch());
        } else {
            fence_it->second = r.pid.get_epoch();
        }
    }

    // TODO: https://app.clubhouse.io/vectorized/story/2194
    // (auto-abort txes with the the same producer_id but older epoch)
    auto [_, inserted] = _volatile_txs.try_emplace(
      r.pid, volatile_tx{.tx_seq = r.tx_seq});

    if (!inserted) {
        // TODO: https://app.clubhouse.io/vectorized/story/2194
        // (auto-abort txes with the the same producer_id but older epoch)
        co_return make_begin_tx_reply(cluster::tx_errc::request_rejected);
    }

    cluster::begin_group_tx_reply reply;
    reply.etag = _term;
    reply.ec = cluster::tx_errc::none;
    co_return reply;
}

ss::future<cluster::prepare_group_tx_reply>
group::prepare_tx(cluster::prepare_group_tx_request r) {
    if (_partition->term() != _term) {
        co_return make_prepare_tx_reply(cluster::tx_errc::stale);
    }

    auto prepared_it = _prepared_txs.find(r.pid);
    if (prepared_it != _prepared_txs.end()) {
        if (prepared_it->second.tx_seq != r.tx_seq) {
            // current prepare_tx call is stale, rejecting
            co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
        }
        // a tx was already prepared
        co_return make_prepare_tx_reply(cluster::tx_errc::none);
    }

    // checking fencing
    auto fence_it = _fence_pid_epoch.find(r.pid.get_id());
    if (fence_it != _fence_pid_epoch.end()) {
        if (r.pid.get_epoch() < fence_it->second) {
            vlog(
              _ctx_txlog.trace,
              "Can't prepare pid:{} - fenced out by epoch {}",
              r.pid,
              fence_it->second);
            co_return make_prepare_tx_reply(cluster::tx_errc::fenced);
        }
    }

    if (r.etag != _term) {
        co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
    }

    auto tx_it = _volatile_txs.find(r.pid);
    if (tx_it == _volatile_txs.end()) {
        // impossible situation, a transaction coordinator tries
        // to prepare a transaction which wasn't started
        vlog(_ctx_txlog.error, "Can't prepare pid:{} - unknown session", r.pid);
        co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
    }

    if (tx_it->second.tx_seq != r.tx_seq) {
        // current prepare_tx call is stale, rejecting
        co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
    }

    auto tx_entry = group_log_prepared_tx{
      .group_id = r.group_id, .pid = r.pid, .tx_seq = r.tx_seq};

    for (const auto& [tp, offset] : tx_it->second.offsets) {
        group_log_prepared_tx_offset tx_offset;

        tx_offset.tp = tp;
        tx_offset.offset = offset.offset;
        tx_offset.leader_epoch = offset.leader_epoch;
        tx_offset.metadata = offset.metadata;
        tx_entry.offsets.push_back(tx_offset);
    }

    volatile_tx tx = tx_it->second;
    _volatile_txs.erase(tx_it);

    // TODO: https://app.clubhouse.io/vectorized/story/2200
    // include producer_id+type into key to make it unique-ish
    // to prevent being GCed by the compaction
    auto batch = make_tx_batch(
      model::record_batch_type::group_prepare_tx,
      prepared_tx_record_version,
      r.pid,
      std::move(tx_entry));
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto e = co_await _partition->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!e) {
        co_return make_prepare_tx_reply(cluster::tx_errc::unknown_server_error);
    }

    prepared_tx ptx;
    ptx.tx_seq = r.tx_seq;
    for (const auto& [tp, offset] : tx.offsets) {
        offset_metadata md{
          .log_offset = e.value().last_offset,
          .offset = offset.offset,
          .metadata = offset.metadata.value_or(""),
          .committed_leader_epoch = offset.leader_epoch};
        ptx.offsets[tp] = md;
    }
    _prepared_txs[r.pid] = ptx;
    co_return make_prepare_tx_reply(cluster::tx_errc::none);
}

cluster::abort_origin group::get_abort_origin(
  const model::producer_identity& pid, model::tx_seq tx_seq) const {
    auto expected_it = _volatile_txs.find(pid);
    if (expected_it != _volatile_txs.end()) {
        if (tx_seq < expected_it->second.tx_seq) {
            return cluster::abort_origin::past;
        }
        if (expected_it->second.tx_seq < tx_seq) {
            return cluster::abort_origin::future;
        }
    }

    auto prepared_it = _prepared_txs.find(pid);
    if (prepared_it != _prepared_txs.end()) {
        if (tx_seq < prepared_it->second.tx_seq) {
            return cluster::abort_origin::past;
        }
        if (prepared_it->second.tx_seq < tx_seq) {
            return cluster::abort_origin::future;
        }
    }

    return cluster::abort_origin::present;
}

ss::future<cluster::abort_group_tx_reply>
group::abort_tx(cluster::abort_group_tx_request r) {
    // doesn't make sense to fence off an abort because transaction
    // manager has already decided to abort and acked to a client

    if (_partition->term() != _term) {
        co_return make_abort_tx_reply(cluster::tx_errc::stale);
    }

    auto origin = get_abort_origin(r.pid, r.tx_seq);
    if (origin == cluster::abort_origin::past) {
        // rejecting a delayed abort command to prevent aborting
        // a wrong transaction
        co_return make_abort_tx_reply(cluster::tx_errc::request_rejected);
    }
    if (origin == cluster::abort_origin::future) {
        // impossible situation: before transactional coordinator may issue
        // abort of the current transaction it should begin it and abort all
        // previous transactions with the same pid
        vlog(
          _ctx_txlog.error,
          "Rejecting abort (pid:{}, tx_seq: {}) because it isn't consistent "
          "with the current ongoing transaction",
          r.pid,
          r.tx_seq);
        co_return make_abort_tx_reply(cluster::tx_errc::request_rejected);
    }

    // preventing prepare and replicte once we
    // know we're going to abort tx and abandon pid
    _volatile_txs.erase(r.pid);

    // TODO: https://app.clubhouse.io/vectorized/story/2197
    // (check for tx_seq to prevent old abort requests aborting
    // new transactions in the same session)

    auto tx = group_log_aborted_tx{.group_id = r.group_id, .tx_seq = r.tx_seq};

    // TODO: https://app.clubhouse.io/vectorized/story/2200
    // include producer_id+type into key to make it unique-ish
    // to prevent being GCed by the compaction
    auto batch = make_tx_batch(
      model::record_batch_type::group_abort_tx,
      aborted_tx_record_version,
      r.pid,
      std::move(tx));
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto e = co_await _partition->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!e) {
        co_return make_abort_tx_reply(cluster::tx_errc::unknown_server_error);
    }

    _prepared_txs.erase(r.pid);

    co_return make_abort_tx_reply(cluster::tx_errc::none);
}

ss::future<txn_offset_commit_response>
group::store_txn_offsets(txn_offset_commit_request r) {
    if (_partition->term() != _term) {
        co_return txn_offset_commit_response(
          r, error_code::unknown_server_error);
    }

    model::producer_identity pid{
      .id = r.data.producer_id, .epoch = r.data.producer_epoch};

    auto tx_it = _volatile_txs.find(pid);

    if (tx_it == _volatile_txs.end()) {
        co_return txn_offset_commit_response(
          r, error_code::unknown_server_error);
    }

    for (const auto& t : r.data.topics) {
        for (const auto& p : t.partitions) {
            model::topic_partition tp(t.name, p.partition_index);
            volatile_offset md{
              .offset = p.committed_offset,
              .leader_epoch = p.committed_leader_epoch,
              .metadata = p.committed_metadata};
            tx_it->second.offsets[tp] = md;
        }
    }

    co_return txn_offset_commit_response(r, error_code::none);
}

kafka::error_code map_store_offset_error_code(std::error_code ec) {
    if (ec.category() == raft::error_category()) {
        switch (raft::errc(ec.value())) {
        case raft::errc::success:
            return error_code::none;
        case raft::errc::timeout:
        case raft::errc::shutting_down:
            return error_code::request_timed_out;
        case raft::errc::not_leader:
            return error_code::not_coordinator;
        case raft::errc::disconnected_endpoint:
        case raft::errc::exponential_backoff:
        case raft::errc::non_majority_replication:
        case raft::errc::vote_dispatch_error:
        case raft::errc::append_entries_dispatch_error:
        case raft::errc::replicated_entry_truncated:
        case raft::errc::leader_flush_failed:
        case raft::errc::leader_append_failed:
        case raft::errc::configuration_change_in_progress:
        case raft::errc::node_does_not_exists:
        case raft::errc::leadership_transfer_in_progress:
        case raft::errc::node_already_exists:
        case raft::errc::invalid_configuration_update:
        case raft::errc::not_voter:
        case raft::errc::invalid_target_node:
        case raft::errc::replicate_batcher_cache_error:
        case raft::errc::group_not_exists:
        case raft::errc::replicate_first_stage_exception:
        case raft::errc::transfer_to_current_leader:
            return error_code::unknown_server_error;
        }
    }
    return error_code::unknown_server_error;
}

group::offset_commit_stages group::store_offsets(offset_commit_request&& r) {
    cluster::simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    std::vector<std::pair<model::topic_partition, offset_metadata>>
      offset_commits;

    for (const auto& t : r.data.topics) {
        for (const auto& p : t.partitions) {
            offset_metadata_key key{
              .group_id = _id, .topic = t.name, .partition = p.partition_index};

            offset_metadata_value value{
              .offset = p.committed_offset,
              .leader_epoch = p.committed_leader_epoch,
              .metadata = p.committed_metadata.value_or(""),
              .commit_timestamp = model::timestamp(p.commit_timestamp),
            };

            auto kv = _md_serializer.to_kv(offset_metadata_kv{
              .key = std::move(key), .value = std::move(value)});
            builder.add_raw_kv(std::move(kv.key), std::move(kv.value));

            model::topic_partition tp(t.name, p.partition_index);
            offset_metadata md{
              .offset = p.committed_offset,
              .metadata = p.committed_metadata.value_or(""),
              .committed_leader_epoch = p.committed_leader_epoch,
            };

            offset_commits.emplace_back(std::make_pair(tp, md));

            // record the offset commits as pending commits which will be
            // inspected after the append to catch concurrent updates.
            _pending_offset_commits[tp] = md;
        }
    }

    auto batch = std::move(builder).build();
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto replicate_stages = _partition->replicate_in_stages(
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    auto f = replicate_stages.replicate_finished.then(
      [this, req = std::move(r), commits = std::move(offset_commits)](
        result<raft::replicate_result> r) mutable {
          auto error = error_code::none;
          if (!r) {
              vlog(
                _ctxlog.info,
                "Storing committed offset failed - {}",
                r.error().message());
              error = map_store_offset_error_code(r.error());
          }
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
    return offset_commit_stages(
      std::move(replicate_stages.request_enqueued), std::move(f));
}

ss::future<cluster::commit_group_tx_reply>
group::handle_commit_tx(cluster::commit_group_tx_request r) {
    if (in_state(group_state::dead)) {
        co_return make_commit_tx_reply(
          cluster::tx_errc::coordinator_not_available);
    } else if (
      in_state(group_state::empty) || in_state(group_state::stable)
      || in_state(group_state::preparing_rebalance)) {
        auto id = r.pid.get_id();
        co_return co_await with_pid_lock(
          id, [this, r = std::move(r)]() mutable {
              return commit_tx(std::move(r));
          });
    } else if (in_state(group_state::completing_rebalance)) {
        co_return make_commit_tx_reply(cluster::tx_errc::rebalance_in_progress);
    } else {
        vlog(_ctx_txlog.error, "Unexpected group state");
        co_return make_commit_tx_reply(cluster::tx_errc::timeout);
    }
}

ss::future<txn_offset_commit_response>
group::handle_txn_offset_commit(txn_offset_commit_request r) {
    if (in_state(group_state::dead)) {
        co_return txn_offset_commit_response(
          r, error_code::coordinator_not_available);
    } else if (
      in_state(group_state::empty) || in_state(group_state::stable)
      || in_state(group_state::preparing_rebalance)) {
        auto id = model::producer_id(r.data.producer_id());
        co_return co_await with_pid_lock(
          id, [this, r = std::move(r)]() mutable {
              return store_txn_offsets(std::move(r));
          });
    } else if (in_state(group_state::completing_rebalance)) {
        co_return txn_offset_commit_response(
          r, error_code::rebalance_in_progress);
    } else {
        vlog(_ctx_txlog.error, "Unexpected group state");
        co_return txn_offset_commit_response(
          r, error_code::unknown_server_error);
    }
}

ss::future<cluster::begin_group_tx_reply>
group::handle_begin_tx(cluster::begin_group_tx_request r) {
    if (in_state(group_state::dead)) {
        cluster::begin_group_tx_reply reply;
        reply.ec = cluster::tx_errc::coordinator_not_available;
        co_return reply;
    } else if (
      in_state(group_state::empty) || in_state(group_state::stable)
      || in_state(group_state::preparing_rebalance)) {
        auto id = r.pid.get_id();
        co_return co_await with_pid_lock(
          id, [this, r = std::move(r)]() mutable {
              return begin_tx(std::move(r));
          });
    } else if (in_state(group_state::completing_rebalance)) {
        cluster::begin_group_tx_reply reply;
        reply.ec = cluster::tx_errc::rebalance_in_progress;
        co_return reply;
    } else {
        vlog(_ctx_txlog.error, "Unexpected group state");
        cluster::begin_group_tx_reply reply;
        reply.ec = cluster::tx_errc::timeout;
        co_return reply;
    }
}

ss::future<cluster::prepare_group_tx_reply>
group::handle_prepare_tx(cluster::prepare_group_tx_request r) {
    if (in_state(group_state::dead)) {
        cluster::prepare_group_tx_reply reply;
        reply.ec = cluster::tx_errc::coordinator_not_available;
        co_return reply;
    } else if (
      in_state(group_state::stable) || in_state(group_state::empty)
      || in_state(group_state::preparing_rebalance)) {
        auto id = r.pid.get_id();
        co_return co_await with_pid_lock(
          id, [this, r = std::move(r)]() mutable {
              return prepare_tx(std::move(r));
          });
    } else if (in_state(group_state::completing_rebalance)) {
        cluster::prepare_group_tx_reply reply;
        reply.ec = cluster::tx_errc::rebalance_in_progress;
        co_return reply;
    } else {
        vlog(_ctx_txlog.error, "Unexpected group state");
        cluster::prepare_group_tx_reply reply;
        reply.ec = cluster::tx_errc::timeout;
        co_return reply;
    }
}

ss::future<cluster::abort_group_tx_reply>
group::handle_abort_tx(cluster::abort_group_tx_request r) {
    if (in_state(group_state::dead)) {
        cluster::abort_group_tx_reply reply;
        reply.ec = cluster::tx_errc::coordinator_not_available;
        co_return reply;
    } else if (
      in_state(group_state::stable) || in_state(group_state::empty)
      || in_state(group_state::preparing_rebalance)) {
        auto id = r.pid.get_id();
        co_return co_await with_pid_lock(
          id, [this, r = std::move(r)]() mutable {
              return abort_tx(std::move(r));
          });
    } else if (in_state(group_state::completing_rebalance)) {
        cluster::abort_group_tx_reply reply;
        reply.ec = cluster::tx_errc::rebalance_in_progress;
        co_return reply;
    } else {
        vlog(_ctx_txlog.error, "Unexpected group state");
        cluster::abort_group_tx_reply reply;
        reply.ec = cluster::tx_errc::timeout;
        co_return reply;
    }
}

group::offset_commit_stages
group::handle_offset_commit(offset_commit_request&& r) {
    if (in_state(group_state::dead)) {
        return offset_commit_stages(
          offset_commit_response(r, error_code::coordinator_not_available));

    } else if (r.data.generation_id < 0 && in_state(group_state::empty)) {
        // <kafka>The group is only using Kafka to store offsets.</kafka>
        return store_offsets(std::move(r));

    } else if (!contains_member(r.data.member_id)) {
        return offset_commit_stages(
          offset_commit_response(r, error_code::unknown_member_id));

    } else if (r.data.generation_id != generation()) {
        return offset_commit_stages(
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
        return offset_commit_stages(
          offset_commit_response(r, error_code::rebalance_in_progress));
    } else {
        return offset_commit_stages(
          ss::now(),
          ss::make_exception_future<offset_commit_response>(std::runtime_error(
            fmt::format("Unexpected group state {} for {}", _state, *this))));
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
              .committed_offset = e.second->metadata.offset,
              .committed_leader_epoch
              = e.second->metadata.committed_leader_epoch,
              .metadata = e.second->metadata.metadata,
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
    auto cid = r.client_id ? *r.client_id : kafka::client_id("");
    auto id = r.data.group_instance_id ? (*r.data.group_instance_id)() : cid();
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return kafka::member_id(ssx::sformat("{}-{}", id, to_string(uuid)));
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

namespace {
void add_offset_tombstone_record(
  const kafka::group_id& group,
  const model::topic_partition& tp,
  group_metadata_serializer& serializer,
  storage::record_batch_builder& builder) {
    offset_metadata_key key{
      .group_id = group,
      .topic = tp.topic,
      .partition = tp.partition,
    };
    auto kv = serializer.to_kv(offset_metadata_kv{.key = std::move(key)});
    builder.add_raw_kv(reflection::to_iobuf(std::move(kv.key)), std::nullopt);
}

void add_group_tombstone_record(
  const kafka::group_id& group,
  group_metadata_serializer& serializer,
  storage::record_batch_builder& builder) {
    group_metadata_key key{
      .group_id = group,
    };
    auto kv = serializer.to_kv(group_metadata_kv{.key = std::move(key)});
    builder.add_raw_kv(reflection::to_iobuf(std::move(kv.key)), std::nullopt);
}
} // namespace

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
      model::record_batch_type::raft_data, model::offset(0));

    for (auto& offset : _offsets) {
        add_offset_tombstone_record(_id, offset.first, _md_serializer, builder);
    }

    // build group tombstone
    add_group_tombstone_record(_id, _md_serializer, builder);

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
              std::move(offset.key()), std::move(offset.mapped()->metadata));
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
      model::record_batch_type::raft_data, model::offset(0));

    // create deletion records for offsets from deleted partitions
    for (auto& offset : removed) {
        vlog(
          klog.trace, "Removing offset for group {} tp {}", _id, offset.first);
        add_offset_tombstone_record(_id, offset.first, _md_serializer, builder);
    }

    // gc the group?
    if (in_state(group_state::dead) && generation() > 0) {
        add_group_tombstone_record(_id, _md_serializer, builder);
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
    const auto ntp = [&g] {
        if (g._partition) {
            return fmt::format("{}", g._partition->ntp());
        } else {
            return std::string("<none>");
        }
    }();

    auto timer_expires =
      [](const auto& timer) -> std::optional<group::duration_type> {
        if (timer.armed()) {
            return timer.get_timeout() - group::clock_type::now();
        }
        return std::nullopt;
    };

    fmt::print(
      o,
      "id={} state={} gen={} proto_type={} proto={} leader={} "
      "empty={} ntp={} num_members_joining={} new_member_added={} "
      "join_timer={}",
      g.id(),
      g.state(),
      g.generation(),
      g.protocol_type(),
      g.protocol(),
      g.leader(),
      !g.has_members(),
      ntp,
      g._num_members_joining,
      g._new_member_added,
      timer_expires(g._join_timer));

    fmt::print(o, " pending members [");
    for (const auto& m : g._pending_members) {
        fmt::print(o, "{} expires={} ", m.first, timer_expires(m.second));
    }
    fmt::print(o, "] full members [");
    for (const auto& m : g._members) {
        fmt::print(o, "{} ", m.second);
    }
    fmt::print(o, "]");

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

void group::add_pending_member(
  const kafka::member_id& member_id, duration_type timeout) {
    auto res = _pending_members.try_emplace(member_id, [this, member_id] {
        vlog(
          _ctxlog.trace,
          "Handling expired heartbeat for pending member {}",
          member_id);
        remove_pending_member(member_id);
    });

    // let existing timer expire
    if (!res.second) {
        return;
    }

    vlog(
      _ctxlog.trace,
      "Scheduling heartbeat expiration {} ms for pending {}",
      timeout,
      member_id);

    res.first->second.arm(timeout);
}

} // namespace kafka
