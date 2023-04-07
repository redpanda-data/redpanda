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
#include "kafka/server/member.h"
#include "kafka/types.h"
#include "likely.h"
#include "model/fundamental.h"
#include "raft/errc.h"
#include "ssx/future-util.h"
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

group::group(
  kafka::group_id id,
  group_state s,
  config::configuration& conf,
  ss::lw_shared_ptr<cluster::partition> partition,
  ss::sharded<cluster::tx_gateway_frontend>& tx_frontend,
  ss::sharded<features::feature_table>& feature_table,
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
  , _probe(_members, _static_members, _offsets)
  , _ctxlog(klog, *this)
  , _ctx_txlog(cluster::txlog, *this)
  , _md_serializer(std::move(serializer))
  , _enable_group_metrics(group_metrics)
  , _abort_interval_ms(config::shard_local_cfg()
                         .abort_timed_out_transactions_interval_ms.value())
  , _tx_frontend(tx_frontend)
  , _feature_table(feature_table) {
    if (_enable_group_metrics) {
        _probe.setup_public_metrics(_id);
    }

    start_abort_timer();
}

group::group(
  kafka::group_id id,
  group_metadata_value& md,
  config::configuration& conf,
  ss::lw_shared_ptr<cluster::partition> partition,
  ss::sharded<cluster::tx_gateway_frontend>& tx_frontend,
  ss::sharded<features::feature_table>& feature_table,
  group_metadata_serializer serializer,
  enable_group_metrics group_metrics)
  : _id(std::move(id))
  , _state(md.members.empty() ? group_state::empty : group_state::stable)
  , _state_timestamp(md.state_timestamp)
  , _generation(md.generation)
  , _num_members_joining(0)
  , _protocol_type(md.protocol_type)
  , _protocol(md.protocol)
  , _leader(md.leader)
  , _new_member_added(false)
  , _conf(conf)
  , _partition(std::move(partition))
  , _probe(_members, _static_members, _offsets)
  , _ctxlog(klog, *this)
  , _ctx_txlog(cluster::txlog, *this)
  , _md_serializer(std::move(serializer))
  , _enable_group_metrics(group_metrics)
  , _abort_interval_ms(config::shard_local_cfg()
                         .abort_timed_out_transactions_interval_ms.value())
  , _tx_frontend(tx_frontend)
  , _feature_table(feature_table) {
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

    if (_enable_group_metrics) {
        _probe.setup_public_metrics(_id);
    }

    start_abort_timer();
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

static model::record_batch make_tx_fence_batch(
  const model::producer_identity& pid, group_log_fencing_v0 cmd) {
    return make_tx_batch(
      model::record_batch_type::tx_fence,
      group::fence_control_record_v0_version,
      pid,
      cmd);
}

static model::record_batch make_tx_fence_batch(
  const model::producer_identity& pid, group_log_fencing cmd) {
    return make_tx_batch(
      model::record_batch_type::tx_fence,
      group::fence_control_record_version,
      pid,
      cmd);
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
    if (member->group_instance_id()) {
        auto [_, success] = _static_members.emplace(
          *member->group_instance_id(), member->id());
        if (!success) {
            throw std::runtime_error(fmt::format(
              "group already contains member with group instance id: {}, group "
              "state: {}",
              member,
              *this));
        }
    }
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

void group::update_member_no_join(
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
}

ss::future<join_group_response> group::update_member(
  member_ptr member, std::vector<member_protocol>&& new_protocols) {
    update_member_no_join(member, std::move(new_protocols));

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

group::join_group_stages
group::handle_join_group(join_group_request&& r, bool is_new_group) {
    vlog(
      _ctxlog.trace,
      "Handling join request {} for {} group {}",
      r,
      (is_new_group ? "new" : "existing"),
      *this);

    auto ret = join_group_stages(join_group_response(error_code::none));

    if (r.data.member_id == unknown_member_id) {
        ret = join_group_unknown_member(std::move(r));
    } else {
        ret = join_group_known_member(std::move(r));
    }

    // TODO: move the logic in this method up to group manager to make the
    // handling of is_new_group etc.. clearner rather than passing these flags
    // down into the group-level handler.
    if (
      !is_new_group && !_initial_join_in_progress
      && in_state(group_state::preparing_rebalance)) {
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

group::join_group_stages
group::join_group_unknown_member(join_group_request&& r) {
    if (in_state(group_state::dead)) {
        vlog(_ctxlog.trace, "Join rejected in state {}", _state);
        return join_group_stages(make_join_error(
          unknown_member_id, error_code::coordinator_not_available));

    } else if (!supports_protocols(r)) {
        return join_group_stages(make_join_error(
          unknown_member_id, error_code::inconsistent_group_protocol));
    }

    auto new_member_id = group::generate_member_id(r);
    if (r.data.group_instance_id) {
        return add_new_static_member(std::move(new_member_id), std::move(r));
    }
    return add_new_dynamic_member(std::move(new_member_id), std::move(r));
}

group::join_group_stages
group::add_new_static_member(member_id new_member_id, join_group_request&& r) {
    auto it = _static_members.find(r.data.group_instance_id.value());

    if (it != _static_members.end()) {
        vlog(
          _ctxlog.debug,
          "Static member with unknown member id and instance id {} joins the "
          "group, replacing previously mapped {} member id with new member_id "
          "{}",
          r.data.group_instance_id.value(),
          it->second,
          new_member_id);
        return update_static_member_and_rebalance(
          it->second, std::move(new_member_id), std::move(r));
    }
    vlog(
      _ctxlog.debug,
      "Static member with unknown member id and instance id {} joins the "
      "group new member id: {}",
      r.data.group_instance_id.value(),
      new_member_id);
    return add_member_and_rebalance(std::move(new_member_id), std::move(r));
}

group::join_group_stages group::update_static_member_and_rebalance(
  member_id old_member_id, member_id new_member_id, join_group_request&& r) {
    auto member = replace_static_member(
      *r.data.group_instance_id, old_member_id, new_member_id);
    /*
     * <kafka> Heartbeat of old member id will expire without effect since the
     * group no longer contains that member id. New heartbeat shall be scheduled
     * with new member id.</kafka>
     */
    schedule_next_heartbeat_expiration(member);
    auto f = update_member(member, r.native_member_protocols());
    auto old_protocols = _members.at(new_member_id)->protocols();
    switch (state()) {
    case group_state::stable: {
        auto next_gen_protocol = select_protocol();
        if (_protocol == next_gen_protocol) {
            vlog(
              _ctxlog.trace,
              "static member {} joins in stable state with the protocol that "
              "does not require group protocol update, will not trigger "
              "rebalance",
              r.data.group_instance_id);
            ssx::background
              = store_group(checkpoint())
                  .then([this,
                         member,
                         instance_id = *r.data.group_instance_id,
                         new_member_id = std::move(new_member_id),
                         old_member_id = std::move(old_member_id),
                         old_protocols = std::move(old_protocols)](
                          result<raft::replicate_result> result) mutable {
                      if (!result) {
                          vlog(
                            _ctxlog.warn,
                            "unable to store group checkpoint - {}",
                            result.error());
                          // <kafka>Failed to persist member.id of the
                          // given static member, revert the update of
                          // the static member in the group.</kafka>
                          auto member = replace_static_member(
                            instance_id, new_member_id, old_member_id);
                          update_member_no_join(
                            member, std::move(old_protocols));
                          schedule_next_heartbeat_expiration(member);
                          try_finish_joining_member(
                            member,
                            make_join_error(
                              unknown_member_id,
                              map_store_offset_error_code(result.error())));
                          return;
                      }
                      // leader    -> member metadata
                      // followers -> []
                      std::vector<member_config> md;
                      if (is_leader(new_member_id)) {
                          md = member_metadata();
                      }

                      auto reply = join_group_response(
                        error_code::none,
                        generation(),
                        protocol().value_or(kafka::protocol_name()),
                        leader().value_or(kafka::member_id()),
                        new_member_id,
                        std::move(md));
                      try_finish_joining_member(member, std::move(reply));
                  })
                  .finally([group = shared_from_this()] {
                      // keep the group alive while the background task runs
                  });
            return join_group_stages(std::move(f));
        } else {
            vlog(
              _ctxlog.trace,
              "static member {} joins in stable state with the protocol that "
              "requires group protocol update, trying to trigger rebalance",
              r.data.group_instance_id);
            try_prepare_rebalance();
        }
        return join_group_stages(std::move(f));
    }
    case group_state::preparing_rebalance:
        return join_group_stages(std::move(f));
    case group_state::completing_rebalance:
        vlog(
          _ctxlog.trace,
          "updating static member {} (member_id: {}) metadata",
          member->group_instance_id().value(),
          member->id());
        try_prepare_rebalance();
        return join_group_stages(std::move(f));
    case group_state::empty:
        [[fallthrough]];
    case group_state::dead:
        throw std::runtime_error(fmt::format(
          "group was not supposed to be in {} state when the unknown static "
          "member {} rejoins, group state: {}",
          state(),
          r.data.group_instance_id,
          *this));
    }
}

member_ptr group::replace_static_member(
  const group_instance_id& instance_id,
  const member_id& old_member_id,
  const member_id& new_member_id) {
    auto it = _members.find(old_member_id);
    if (it == _members.end()) {
        throw std::runtime_error(fmt::format(
          "can not replace not existing member with id {}, group state: {}",
          old_member_id,
          *this));
    }
    auto member = it->second;
    _members.erase(it);
    member->replace_id(new_member_id);
    _members.emplace(new_member_id, member);

    if (member->is_joining()) {
        try_finish_joining_member(
          member,
          make_join_error(old_member_id, error_code::fenced_instance_id));
    }
    if (member->is_syncing()) {
        member->set_sync_response(
          sync_group_response(error_code::fenced_instance_id));
    }
    if (is_leader(old_member_id)) {
        _leader = member->id();
    }

    auto static_it = _static_members.find(instance_id);
    vassert(
      static_it != _static_members.end(),
      "Static member that is going to be replaced must existing in group "
      "static members map. instance_id: {}, group_state: {}",
      instance_id,
      *this);
    static_it->second = new_member_id;

    return member;
}

group::join_group_stages
group::add_new_dynamic_member(member_id new_member_id, join_group_request&& r) {
    vassert(
      !r.data.group_instance_id,
      "Group instance id must be empty for dynamic members, request: {}",
      r.data);
    // <kafka>Only return MEMBER_ID_REQUIRED error if joinGroupRequest version
    // is >= 4 and groupInstanceId is configured to unknown.</kafka>
    if (r.version >= api_version(4)) {
        // <kafka>If member id required (dynamic membership), register the
        // member in the pending member list and send back a response to
        // call for another join group request with allocated member id.
        // </kafka>
        vlog(
          _ctxlog.trace,
          "Requesting rejoin for unknown member with new id {}",
          new_member_id);
        add_pending_member(new_member_id, r.data.session_timeout_ms);
        return join_group_stages(
          make_join_error(new_member_id, error_code::member_id_required));
    } else {
        return add_member_and_rebalance(std::move(new_member_id), std::move(r));
    }
}

group::join_group_stages
group::join_group_known_member(join_group_request&& r) {
    if (in_state(group_state::dead)) {
        vlog(
          _ctxlog.trace,
          "Join rejected in state {} for {}",
          _state,
          r.data.member_id);
        return join_group_stages(make_join_error(
          r.data.member_id, error_code::coordinator_not_available));

    } else if (!supports_protocols(r)) {
        return join_group_stages(make_join_error(
          r.data.member_id, error_code::inconsistent_group_protocol));

    } else if (contains_pending_member(r.data.member_id)) {
        kafka::member_id new_member_id = std::move(r.data.member_id);
        return add_member_and_rebalance(std::move(new_member_id), std::move(r));

    } else if (auto ec = validate_existing_member(
                 r.data.member_id, r.data.group_instance_id, "join");
               ec != error_code::none) {
        vlog(
          _ctxlog.trace,
          "Join rejected for invalid member {} - {}",
          r.data.member_id,
          ec);
        return join_group_stages(make_join_error(r.data.member_id, ec));
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

            return join_group_stages(std::move(response));

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

            return join_group_stages(std::move(response));
        }

    case group_state::empty:
        [[fallthrough]];

    case group_state::dead:
        return join_group_stages(
          make_join_error(r.data.member_id, error_code::unknown_member_id));
    }

    __builtin_unreachable();
}

group::join_group_stages group::add_member_and_rebalance(
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
    return join_group_stages(std::move(response));
}

group::join_group_stages
group::update_member_and_rebalance(member_ptr member, join_group_request&& r) {
    auto response = update_member(
      std::move(member), r.native_member_protocols());
    try_prepare_rebalance();
    return join_group_stages(std::move(response));
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
    _initial_join_in_progress = prev_state == group_state::empty;
    if (_initial_join_in_progress) {
        // debounce joins to an empty group. for a bounded delay, we'll avoid
        // competing the join phase as long as new members are arriving.
        auto rebalance = std::chrono::duration_cast<std::chrono::milliseconds>(
          rebalance_timeout());
        auto initial = _conf.group_initial_rebalance_delay();
        auto remaining = std::max(
          rebalance - initial, std::chrono::milliseconds(0));

        _join_timer.cancel();
        _join_timer.set_callback(
          [this, initial, delay = initial, remaining]() mutable {
              if (_new_member_added && remaining.count()) {
                  _new_member_added = false;
                  auto prev_delay = delay;
                  delay = std::min(initial, remaining);
                  remaining = std::max(
                    remaining - prev_delay, std::chrono::milliseconds(0));
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
    _initial_join_in_progress = false;

    // <kafka>remove dynamic members who haven't joined the group yet</kafka>
    // this is the old group->remove_unjoined_members();
    const auto prev_leader = _leader;
    for (auto it = _members.begin(); it != _members.end();) {
        if (!it->second->is_joining() && !it->second->is_static()) {
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
            ssx::background
              = store_group(checkpoint(assignments_type{}))
                  .then(
                    []([[maybe_unused]] result<raft::replicate_result> r) {})
                  .finally([this_group = shared_from_this()] {});
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

ss::future<> group::shutdown() {
    _auto_abort_timer.cancel();
    co_await _gate.close();
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
              member, make_join_error(member_id, error_code::not_coordinator));
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
      member, make_join_error(no_member, error_code::unknown_member_id));

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
        if (it->second->group_instance_id()) {
            _static_members.erase(it->second->group_instance_id().value());
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

group::sync_group_stages group::handle_sync_group(sync_group_request&& r) {
    vlog(_ctxlog.trace, "Handling sync group request {}", r);

    if (in_state(group_state::dead)) {
        vlog(_ctxlog.trace, "Sync rejected for group state {}", _state);
        return sync_group_stages(
          sync_group_response(error_code::coordinator_not_available));

    } else if (auto ec = validate_existing_member(
                 r.data.member_id, r.data.group_instance_id, "sync");
               ec != error_code::none) {
        vlog(
          _ctxlog.trace,
          "Sync rejected for invalid member {} - {}",
          r.data.member_id,
          ec);
        return sync_group_stages(sync_group_response(ec));

    } else if (r.data.generation_id != generation()) {
        vlog(
          _ctxlog.trace,
          "Sync rejected with out-of-date generation {} != {}",
          r.data.generation_id,
          generation());
        return sync_group_stages(
          sync_group_response(error_code::illegal_generation));
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
        return sync_group_stages(
          sync_group_response(error_code::unknown_member_id));

    case group_state::preparing_rebalance:
        vlog(_ctxlog.trace, "Sync rejected for group state {}", _state);
        return sync_group_stages(
          sync_group_response(error_code::rebalance_in_progress));

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
        return sync_group_stages(sync_group_response(std::move(reply)));
    }

    case group_state::dead:
        // checked above on method entry
        break;
    }

    __builtin_unreachable();
}

model::record_batch group::checkpoint(const assignments_type& assignments) {
    return do_checkpoint(
      [&assignments](const member_id& id) { return assignments.at(id); });
}

model::record_batch group::checkpoint() {
    return do_checkpoint([this](const member_id& id) {
        auto it = _members.find(id);
        vassert(
          it != _members.end(),
          "Checkpointed member {} must be part of the group {}",
          id,
          *this);
        return it->second->assignment();
    });
}

group::sync_group_stages group::sync_group_completing_rebalance(
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
        return sync_group_stages(std::move(response));
    }

    vlog(_ctxlog.trace, "Completing group sync with leader {}", member->id());

    // construct a member assignment structure that will be persisted to the
    // underlying metadata topic for group recovery. the mapping is the
    // assignments in the request plus any missing assignments for group
    // members.
    auto assignments = std::move(r).member_assignments();
    add_missing_assignments(assignments);

    auto f = store_group(checkpoint(assignments))
               .then([this,
                      response = std::move(response),
                      expected_generation = generation(),
                      assignments = std::move(assignments)](
                       result<raft::replicate_result> r) mutable {
                   /*
                    * the group's state has changed (e.g. another member
                    * joined). there's nothing to do now except have the client
                    * wait for an update.
                    */
                   if (
                     !in_state(group_state::completing_rebalance)
                     || expected_generation != generation()) {
                       vlog(
                         _ctxlog.trace,
                         "Group state changed while completing sync");
                       return std::move(response);
                   }

                   if (r) {
                       // the group state was successfully persisted:
                       //   - save the member assignments; clients may
                       //   re-request
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
    return sync_group_stages(std::move(f));
}

ss::future<heartbeat_response> group::handle_heartbeat(heartbeat_request&& r) {
    vlog(_ctxlog.trace, "Handling heartbeat request {}", r);

    if (in_state(group_state::dead)) {
        vlog(_ctxlog.trace, "Heartbeat rejected for group state {}", _state);
        return make_heartbeat_error(error_code::coordinator_not_available);

    } else if (auto ec = validate_existing_member(
                 r.data.member_id, r.data.group_instance_id, "heartbeat");
               ec != error_code::none) {
        vlog(
          _ctxlog.trace,
          "Heartbeat rejected for invalid member {} - {}",
          r.data.member_id,
          ec);
        return make_heartbeat_error(ec);

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
    }
    /**
     * Leave group prior to version 3 were requesting only a single member
     * leave.
     */
    if (r.version < api_version(3)) {
        auto err = member_leave_group(r.data.member_id, std::nullopt);
        return make_leave_error(err);
    }

    leave_group_response response(error_code::none);

    response.data.members.reserve(r.data.members.size());
    for (auto& m : r.data.members) {
        auto ec = member_leave_group(m.member_id, m.group_instance_id);
        response.data.members.push_back(member_response{
          .member_id = m.member_id,
          .group_instance_id = m.group_instance_id,
          .error_code = ec});
    }
    return ss::make_ready_future<leave_group_response>(std::move(response));
}

kafka::error_code group::member_leave_group(
  const member_id& member_id,
  const std::optional<group_instance_id>& group_instance_id) {
    // <kafka>The LeaveGroup API allows administrative removal of members by
    // GroupInstanceId in which case we expect the MemberId to be
    // undefined.</kafka>
    if (member_id == unknown_member_id) {
        if (!group_instance_id) {
            return error_code::unknown_member_id;
        }

        auto it = _static_members.find(*group_instance_id);
        if (it == _static_members.end()) {
            return error_code::unknown_member_id;
        }
        // recurse with found mapping
        return member_leave_group(it->second, group_instance_id);
    }

    if (contains_pending_member(member_id)) {
        // <kafka>if a pending member is leaving, it needs to be removed
        // from the pending list, heartbeat cancelled and if necessary,
        // prompt a JoinGroup completion.</kafka>
        remove_pending_member(member_id);
        return error_code::none;

    } else if (auto ec = validate_existing_member(
                 member_id, group_instance_id, "leave");
               ec != error_code::none) {
        vlog(
          _ctxlog.trace,
          "Leave rejected for invalid member {} - {}",
          member_id,
          ec);
        return ec;
    } else {
        auto member = get_member(member_id);
        member->expire_timer().cancel();
        remove_member(member);
        return error_code::none;
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

    auto [txseq_it, inserted] = _tx_seqs.try_emplace(pid.get_id(), tx.tx_seq);
    if (!inserted) {
        if (txseq_it->second != tx.tx_seq) {
            vlog(
              _ctx_txlog.warn,
              "prepared_tx of pid {} has tx_seq {} while {} expected",
              tx.pid,
              tx.tx_seq,
              txseq_it->second);
        }
    }

    _prepared_txs[pid] = std::move(tx);
}

ss::future<cluster::commit_group_tx_reply>
group::commit_tx(cluster::commit_group_tx_request r) {
    if (_partition->term() != _term) {
        co_return make_commit_tx_reply(cluster::tx_errc::stale);
    }

    auto fence_it = _fence_pid_epoch.find(r.pid.get_id());
    if (fence_it == _fence_pid_epoch.end()) {
        vlog(
          _ctx_txlog.warn,
          "Can't prepare tx: fence with pid {} isn't set",
          r.pid);
        co_return make_commit_tx_reply(cluster::tx_errc::request_rejected);
    }
    if (r.pid.get_epoch() != fence_it->second) {
        vlog(
          _ctx_txlog.trace,
          "Can't prepare tx with pid {} - the fence doesn't match {}",
          r.pid,
          fence_it->second);
        co_return make_commit_tx_reply(cluster::tx_errc::request_rejected);
    }

    auto txseq_it = _tx_seqs.find(r.pid.get_id());
    if (txseq_it == _tx_seqs.end()) {
        if (is_transaction_ga()) {
            vlog(
              _ctx_txlog.trace,
              "can't find a tx {}, probably already comitted",
              r.pid);
            co_return make_commit_tx_reply(cluster::tx_errc::none);
        }
    } else if (txseq_it->second > r.tx_seq) {
        // rare situation:
        //   * tm_stm begins (tx_seq+1)
        //   * request on this group passes but then tm_stm fails and forgets
        //   about this tx
        //   * during recovery tm_stm recommits previous tx (tx_seq)
        // existence of {pid, tx_seq+1} implies {pid, tx_seq} is committed
        vlog(
          _ctx_txlog.trace,
          "Already commited pid:{} tx_seq:{} - a higher tx_seq:{} was observed",
          r.pid,
          r.tx_seq,
          txseq_it->second);
        co_return make_commit_tx_reply(cluster::tx_errc::none);
    } else if (txseq_it->second != r.tx_seq) {
        vlog(
          _ctx_txlog.warn,
          "Can't commit pid {}: passed txseq {} doesn't match ongoing {}",
          r.pid,
          r.tx_seq,
          txseq_it->second);
        co_return make_commit_tx_reply(cluster::tx_errc::request_rejected);
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
        co_return make_commit_tx_reply(cluster::tx_errc::request_rejected);
    }

    // we commit only if a provided tx_seq matches prepared tx_seq
    co_return co_await do_commit(r.group_id, r.pid);
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
        vlog(
          _ctx_txlog.trace,
          "processing name:begin_tx pid:{} tx_seq:{} timeout:{} => stale "
          "leader",
          r.pid,
          r.tx_seq,
          r.timeout);
        co_return make_begin_tx_reply(cluster::tx_errc::stale);
    }

    vlog(
      _ctx_txlog.trace,
      "processing name:begin_tx pid:{} tx_seq:{} timeout:{} in term:{}",
      r.pid,
      r.tx_seq,
      r.timeout,
      _term);
    auto fence_it = _fence_pid_epoch.find(r.pid.get_id());
    if (fence_it == _fence_pid_epoch.end()) {
        // intentionally empty
    } else if (r.pid.get_epoch() < fence_it->second) {
        vlog(
          _ctx_txlog.error,
          "pid {} fenced out by epoch {}",
          r.pid,
          fence_it->second);
        co_return make_begin_tx_reply(cluster::tx_errc::fenced);
    } else if (r.pid.get_epoch() > fence_it->second) {
        // there is a fence, it might be that tm_stm failed, forget about
        // an ongoing transaction, assigned next pid for the same tx.id and
        // started a new transaction without aborting the previous one.
        //
        // at the same time it's possible that it already aborted the old
        // tx before starting this. do_abort_tx is idempotent so calling it
        // just in case to proactivly abort the tx instead of waiting for
        // the timeout

        auto old_pid = model::producer_identity{
          r.pid.get_id(), fence_it->second};
        auto ar = co_await do_try_abort_old_tx(old_pid);
        if (ar != cluster::tx_errc::none) {
            vlog(
              _ctx_txlog.trace,
              "can't begin tx {} because abort of a prev tx {} failed with {}; "
              "retrying",
              r.pid,
              old_pid,
              ar);
            co_return make_begin_tx_reply(cluster::tx_errc::stale);
        }
    }

    auto txseq_it = _tx_seqs.find(r.pid.get_id());
    if (txseq_it != _tx_seqs.end()) {
        if (r.tx_seq != txseq_it->second) {
            vlog(
              _ctx_txlog.warn,
              "can't begin a tx {} with tx_seq {}: a producer id is already "
              "involved in a tx with tx_seq {}",
              r.pid,
              r.tx_seq,
              txseq_it->second);
            co_return make_begin_tx_reply(
              cluster::tx_errc::unknown_server_error);
        }
        if (_prepared_txs.contains(r.pid)) {
            vlog(
              _ctx_txlog.warn,
              "can't begin a tx {} with tx_seq {}: it was already begun and it "
              "accepted writes",
              r.pid,
              r.tx_seq);
            co_return make_begin_tx_reply(
              cluster::tx_errc::unknown_server_error);
        }
        co_return cluster::begin_group_tx_reply(_term, cluster::tx_errc::none);
    }

    auto is_txn_ga = is_transaction_ga();
    std::optional<model::record_batch> batch{};
    if (is_txn_ga) {
        group_log_fencing fence{
          .group_id = id(),
          .tx_seq = r.tx_seq,
          .transaction_timeout_ms = r.timeout};
        batch = make_tx_fence_batch(r.pid, std::move(fence));
    } else {
        group_log_fencing_v0 fence{.group_id = id()};
        batch = make_tx_fence_batch(r.pid, std::move(fence));
    }

    auto reader = model::make_memory_record_batch_reader(
      std::move(batch.value()));
    auto e = co_await _partition->raft()->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!e) {
        vlog(
          _ctx_txlog.warn,
          "Error \"{}\" on replicating pid:{} fencing batch",
          e.error(),
          r.pid);
        co_return make_begin_tx_reply(cluster::tx_errc::leader_not_found);
    }

    _fence_pid_epoch[r.pid.get_id()] = r.pid.get_epoch();
    _tx_seqs[r.pid.get_id()] = r.tx_seq;

    if (!is_txn_ga) {
        _volatile_txs[r.pid] = volatile_tx{.tx_seq = r.tx_seq};
    }

    auto res = _expiration_info.insert_or_assign(
      r.pid, expiration_info(r.timeout));
    try_arm(res.first->second.deadline());

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

    auto is_txn_ga = is_transaction_ga();
    if (!is_txn_ga) {
        auto tx_it = _volatile_txs.find(r.pid);
        if (tx_it == _volatile_txs.end()) {
            // impossible situation, a transaction coordinator tries
            // to prepare a transaction which wasn't started
            vlog(
              _ctx_txlog.error,
              "Can't prepare pid:{} - unknown session",
              r.pid);
            co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
        }
        if (tx_it->second.tx_seq != r.tx_seq) {
            // current prepare_tx call is stale, rejecting
            co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
        }
        if (r.etag != _term) {
            co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
        }
    }

    // checking fencing
    auto fence_it = _fence_pid_epoch.find(r.pid.get_id());
    if (fence_it == _fence_pid_epoch.end()) {
        vlog(
          _ctx_txlog.warn,
          "Can't prepare tx: fence with pid {} isn't set",
          r.pid);
        co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
    }
    if (r.pid.get_epoch() != fence_it->second) {
        vlog(
          _ctx_txlog.trace,
          "Can't prepare tx with pid {} - the fence doesn't match {}",
          r.pid,
          fence_it->second);
        co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
    }

    auto txseq_it = _tx_seqs.find(r.pid.get_id());
    if (txseq_it != _tx_seqs.end() && txseq_it->second != r.tx_seq) {
        vlog(
          _ctx_txlog.warn,
          "Can't prepare pid {}: passed txseq {} doesn't match ongoing {}",
          r.pid,
          r.tx_seq,
          txseq_it->second);
        co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
    }

    auto prepared_it = _prepared_txs.find(r.pid);
    if (prepared_it == _prepared_txs.end()) {
        vlog(
          _ctx_txlog.warn, "Can't prepare tx with pid {}: unknown tx", r.pid);
        co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
    }
    if (prepared_it->second.tx_seq != r.tx_seq) {
        vlog(
          _ctx_txlog.warn,
          "Can't prepare pid {}: passed txseq {} doesn't match known {}",
          r.pid,
          r.tx_seq,
          prepared_it->second.tx_seq);
        co_return make_prepare_tx_reply(cluster::tx_errc::request_rejected);
    }

    if (!is_txn_ga) {
        _volatile_txs.erase(r.pid);
    }

    auto exp_it = _expiration_info.find(r.pid);
    if (exp_it != _expiration_info.end()) {
        exp_it->second.update_last_update_time();
    }

    co_return make_prepare_tx_reply(cluster::tx_errc::none);
}

cluster::abort_origin group::get_abort_origin(
  const model::producer_identity& pid, model::tx_seq tx_seq) const {
    if (is_transaction_ga()) {
        auto it = _tx_seqs.find(pid.get_id());
        if (it != _tx_seqs.end()) {
            if (tx_seq < it->second) {
                return cluster::abort_origin::past;
            }
            if (it->second < tx_seq) {
                return cluster::abort_origin::future;
            }
        }

        return cluster::abort_origin::present;
    }

    auto txseq_it = _tx_seqs.find(pid.get_id());
    if (txseq_it != _tx_seqs.end()) {
        if (tx_seq < txseq_it->second) {
            return cluster::abort_origin::past;
        }
        if (txseq_it->second < tx_seq) {
            return cluster::abort_origin::future;
        }
    }

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

    auto fence_it = _fence_pid_epoch.find(r.pid.get_id());
    if (fence_it == _fence_pid_epoch.end()) {
        vlog(
          _ctx_txlog.warn,
          "Can't abort tx: fence with pid {} isn't set",
          r.pid);
        co_return make_abort_tx_reply(cluster::tx_errc::request_rejected);
    }
    if (r.pid.get_epoch() != fence_it->second) {
        vlog(
          _ctx_txlog.trace,
          "Can't abort tx with pid {} - the fence doesn't match {}",
          r.pid,
          fence_it->second);
        co_return make_abort_tx_reply(cluster::tx_errc::request_rejected);
    }

    auto txseq_it = _tx_seqs.find(r.pid.get_id());
    if (txseq_it == _tx_seqs.end()) {
        if (is_transaction_ga()) {
            vlog(
              _ctx_txlog.trace,
              "can't find a tx {}, probably already aborted",
              r.pid);
            co_return make_abort_tx_reply(cluster::tx_errc::none);
        }
    } else if (txseq_it->second > r.tx_seq) {
        // rare situation:
        //   * tm_stm begins (tx_seq+1)
        //   * request on this group passes but then tm_stm fails and forgets
        //   about this tx
        //   * during recovery tm_stm reaborts previous tx (tx_seq)
        // existence of {pid, tx_seq+1} implies {pid, tx_seq} is aborted
        vlog(
          _ctx_txlog.trace,
          "Already aborted pid:{} tx_seq:{} - a higher tx_seq:{} was observed",
          r.pid,
          r.tx_seq,
          txseq_it->second);
        co_return make_abort_tx_reply(cluster::tx_errc::none);
    } else if (txseq_it->second != r.tx_seq) {
        vlog(
          _ctx_txlog.warn,
          "Can't abort pid {}: passed txseq {} doesn't match ongoing {}",
          r.pid,
          r.tx_seq,
          txseq_it->second);
        co_return make_abort_tx_reply(cluster::tx_errc::request_rejected);
    }

    auto origin = get_abort_origin(r.pid, r.tx_seq);
    if (origin == cluster::abort_origin::past) {
        // rejecting a delayed abort command to prevent aborting
        // a wrong transaction
        auto it = _expiration_info.find(r.pid);
        if (it != _expiration_info.end()) {
            it->second.is_expiration_requested = true;
        } else {
            vlog(
              _ctx_txlog.error,
              "pid({}) should be inside _expiration_info",
              r.pid);
        }
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

    co_return co_await do_abort(r.group_id, r.pid, r.tx_seq);
}

ss::future<txn_offset_commit_response>
group::store_txn_offsets(txn_offset_commit_request r) {
    // replaying the log, the term isn't set yet
    // we should use replay or not a leader error
    if (_partition->term() != _term) {
        vlog(
          _ctx_txlog.warn,
          "Last known term {} doesn't match partition term {}",
          _term,
          _partition->term());
        co_return txn_offset_commit_response(
          r, error_code::unknown_server_error);
    }

    model::producer_identity pid{r.data.producer_id, r.data.producer_epoch};

    // checking fencing
    auto fence_it = _fence_pid_epoch.find(pid.get_id());
    if (fence_it == _fence_pid_epoch.end()) {
        vlog(
          _ctx_txlog.warn,
          "Can't store txn offsets: fence with pid {} isn't set",
          pid);
        co_return txn_offset_commit_response(
          r, error_code::invalid_producer_epoch);
    }
    if (r.data.producer_epoch != fence_it->second) {
        vlog(
          _ctx_txlog.trace,
          "Can't store txn offsets with pid {} - the fence doesn't match {}",
          pid,
          fence_it->second);
        co_return txn_offset_commit_response(
          r, error_code::invalid_producer_epoch);
    }

    auto txseq_it = _tx_seqs.find(pid.get_id());
    if (txseq_it == _tx_seqs.end()) {
        vlog(
          _ctx_txlog.warn,
          "Can't store txn offsets: current tx with pid {} isn't ongoing",
          pid);
        co_return txn_offset_commit_response(
          r, error_code::invalid_producer_epoch);
    }
    auto tx_seq = txseq_it->second;

    if (!is_transaction_ga()) {
        auto tx_it = _volatile_txs.find(pid);
        if (tx_it == _volatile_txs.end()) {
            co_return txn_offset_commit_response(
              r, error_code::unknown_server_error);
        }
    }

    absl::node_hash_map<model::topic_partition, group_log_prepared_tx_offset>
      offsets;

    auto prepare_it = _prepared_txs.find(pid);
    if (prepare_it != _prepared_txs.end()) {
        for (const auto& [tp, offset] : prepare_it->second.offsets) {
            group_log_prepared_tx_offset md{
              .tp = tp,
              .offset = offset.offset,
              .leader_epoch = offset.committed_leader_epoch,
              .metadata = offset.metadata};
            offsets[tp] = md;
        }
    }

    for (const auto& t : r.data.topics) {
        for (const auto& p : t.partitions) {
            model::topic_partition tp(t.name, p.partition_index);
            group_log_prepared_tx_offset md{
              .tp = tp,
              .offset = p.committed_offset,
              .leader_epoch = p.committed_leader_epoch,
              .metadata = p.committed_metadata};
            offsets[tp] = md;
        }
    }

    auto tx_entry = group_log_prepared_tx{
      .group_id = r.data.group_id, .pid = pid, .tx_seq = tx_seq};

    for (const auto& [tp, offset] : offsets) {
        tx_entry.offsets.push_back(offset);
    }

    auto batch = make_tx_batch(
      model::record_batch_type::group_prepare_tx,
      prepared_tx_record_version,
      pid,
      std::move(tx_entry));
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto e = co_await _partition->raft()->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!e) {
        co_return txn_offset_commit_response(
          r, error_code::unknown_server_error);
    }

    prepared_tx ptx;
    ptx.tx_seq = tx_seq;
    for (const auto& [tp, offset] : offsets) {
        offset_metadata md{
          .log_offset = e.value().last_offset,
          .offset = offset.offset,
          .metadata = offset.metadata.value_or(""),
          .committed_leader_epoch = kafka::leader_epoch(offset.leader_epoch)};
        ptx.offsets[tp] = md;
    }
    _prepared_txs[pid] = ptx;

    auto it = _expiration_info.find(pid);
    if (it != _expiration_info.end()) {
        it->second.update_last_update_time();
    } else {
        vlog(_ctx_txlog.warn, "pid {} should be in _expiration_info", pid);
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
        case raft::errc::replicated_entry_truncated:
            return error_code::not_coordinator;
        case raft::errc::disconnected_endpoint:
        case raft::errc::exponential_backoff:
        case raft::errc::non_majority_replication:
        case raft::errc::vote_dispatch_error:
        case raft::errc::append_entries_dispatch_error:
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

void group::update_store_offset_builder(
  cluster::simple_batch_builder& builder,
  const model::topic& name,
  model::partition_id partition,
  model::offset committed_offset,
  leader_epoch committed_leader_epoch,
  const ss::sstring& metadata,
  model::timestamp commit_timestamp) {
    offset_metadata_key key{
      .group_id = _id, .topic = name, .partition = partition};

    offset_metadata_value value{
      .offset = committed_offset,
      .leader_epoch = committed_leader_epoch,
      .metadata = metadata,
      .commit_timestamp = commit_timestamp,
    };

    auto kv = _md_serializer.to_kv(
      offset_metadata_kv{.key = std::move(key), .value = std::move(value)});
    builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
}

group::offset_commit_stages group::store_offsets(offset_commit_request&& r) {
    cluster::simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    std::vector<std::pair<model::topic_partition, offset_metadata>>
      offset_commits;

    for (const auto& t : r.data.topics) {
        for (const auto& p : t.partitions) {
            update_store_offset_builder(
              builder,
              t.name,
              p.partition_index,
              p.committed_offset,
              p.committed_leader_epoch,
              p.committed_metadata.value_or(""),
              model::timestamp(p.commit_timestamp));

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

    auto replicate_stages = _partition->raft()->replicate_in_stages(
      _term,
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

error_code
group::validate_expected_group(const txn_offset_commit_request& r) const {
    if (r.data.group_instance_id.has_value()) {
        auto static_member_it = _static_members.find(
          r.data.group_instance_id.value());
        if (
          static_member_it != _static_members.end()
          && static_member_it->second != r.data.member_id) {
            return error_code::fenced_instance_id;
        }
    }

    if (
      r.data.member_id != unknown_member_id
      && !_members.contains(r.data.member_id)) {
        return error_code::unknown_member_id;
    }

    if (r.data.generation_id >= 0 && r.data.generation_id != _generation) {
        return error_code::illegal_generation;
    }

    return error_code::none;
}

ss::future<txn_offset_commit_response>
group::handle_txn_offset_commit(txn_offset_commit_request r) {
    if (in_state(group_state::dead)) {
        co_return txn_offset_commit_response(
          r, error_code::coordinator_not_available);
    } else if (
      in_state(group_state::empty) || in_state(group_state::stable)
      || in_state(group_state::preparing_rebalance)) {
        auto check_res = validate_expected_group(r);
        if (check_res != error_code::none) {
            co_return txn_offset_commit_response(r, check_res);
        }

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

    } else if (auto ec = validate_existing_member(
                 r.data.member_id, r.data.group_instance_id, "offset-commit");
               ec != error_code::none) {
        return offset_commit_stages(offset_commit_response(r, ec));

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
              .committed_offset = model::offset(-1),
              .metadata = "",
              .error_code = error_code::none,
            };

            if (r.data.require_stable && has_pending_transaction(e.first)) {
                p.error_code = error_code::unstable_offset_commit;
            } else {
                p.committed_offset = e.second->metadata.offset;
                p.committed_leader_epoch
                  = e.second->metadata.committed_leader_epoch;
                p.metadata = e.second->metadata.metadata;
            }
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

            offset_fetch_response_partition p = {
              .partition_index = id,
              .committed_offset = model::offset(-1),
              .metadata = "",
              .error_code = error_code::none,
            };

            if (r.data.require_stable && has_pending_transaction(tp)) {
                p.error_code = error_code::unstable_offset_commit;
            } else {
                auto res = offset(tp);
                if (res) {
                    p.partition_index = id;
                    p.committed_offset = res->offset;
                    p.metadata = res->metadata;
                    p.error_code = error_code::none;
                }
            }
            t.partitions.push_back(std::move(p));
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
    builder.add_raw_kv(std::move(kv.key), std::nullopt);
}

void add_group_tombstone_record(
  const kafka::group_id& group,
  group_metadata_serializer& serializer,
  storage::record_batch_builder& builder) {
    group_metadata_key key{
      .group_id = group,
    };
    auto kv = serializer.to_kv(group_metadata_kv{.key = std::move(key)});
    builder.add_raw_kv(std::move(kv.key), std::nullopt);
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
        auto result = co_await _partition->raft()->replicate(
          _term,
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        if (result) {
            vlog(
              klog.trace,
              "Replicated group delete record {} at offset {}",
              _id,
              result.value().last_offset);
        } else if (result.error() == raft::errc::shutting_down) {
            vlog(
              klog.debug,
              "Cannot replicate group {} delete records due to shutdown",
              _id);
        } else {
            vlog(
              klog.error,
              "Error occured replicating group {} delete records {} ({})",
              _id,
              result.error().message(),
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
        auto result = co_await _partition->raft()->replicate(
          _term,
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        if (result) {
            vlog(
              klog.trace,
              "Replicated group cleanup record {} at offset {}",
              _id,
              result.value().last_offset);
        } else if (result.error() == raft::errc::shutting_down) {
            vlog(
              klog.debug,
              "Cannot replicate group {} cleanup records due to shutdown",
              _id);
        } else {
            vlog(
              klog.error,
              "Error occured replicating group {} cleanup records {} ({})",
              _id,
              result.error().message(),
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

ss::future<result<raft::replicate_result>>
group::store_group(model::record_batch batch) {
    return _partition->raft()->replicate(
      _term,
      model::make_memory_record_batch_reader(std::move(batch)),
      raft::replicate_options(raft::consistency_level::quorum_ack));
}

error_code group::validate_existing_member(
  const member_id& member_id,
  const std::optional<group_instance_id>& instance_id,
  const ss::sstring& ctx) const {
    if (instance_id) {
        auto it = _static_members.find(*instance_id);
        if (it == _static_members.end()) {
            return error_code::unknown_member_id;
        } else if (it->second != member_id) {
            vlog(
              _ctxlog.info,
              "operation: {} - static member with instance id {} has different "
              "member_id assigned, current: {}, requested: {}",
              ctx,
              *instance_id,
              it->second,
              member_id);
            return error_code::fenced_instance_id;
        }
    } else {
        if (!_members.contains(member_id)) {
            return error_code::unknown_member_id;
        }
    }
    return error_code::none;
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

ss::future<cluster::abort_group_tx_reply> group::do_abort(
  kafka::group_id group_id,
  model::producer_identity pid,
  model::tx_seq tx_seq) {
    // preventing prepare and replicate once we
    // know we're going to abort tx and abandon pid
    _volatile_txs.erase(pid);

    auto tx = group_log_aborted_tx{.group_id = group_id, .tx_seq = tx_seq};

    auto batch = make_tx_batch(
      model::record_batch_type::group_abort_tx,
      aborted_tx_record_version,
      pid,
      std::move(tx));
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto e = co_await _partition->raft()->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!e) {
        vlog(
          _ctx_txlog.warn,
          "Error \"{}\" on replicating pid:{} abort batch",
          e.error(),
          pid);
        if (
          _partition->raft()->is_leader()
          && _partition->raft()->term() == _term) {
            co_await _partition->raft()->step_down("group do abort failed");
        }
        co_return make_abort_tx_reply(cluster::tx_errc::timeout);
    }

    _prepared_txs.erase(pid);
    _tx_seqs.erase(pid.get_id());
    _expiration_info.erase(pid);

    co_return make_abort_tx_reply(cluster::tx_errc::none);
}

ss::future<cluster::commit_group_tx_reply>
group::do_commit(kafka::group_id group_id, model::producer_identity pid) {
    auto prepare_it = _prepared_txs.find(pid);
    if (prepare_it == _prepared_txs.end()) {
        // Impossible situation
        vlog(_ctx_txlog.error, "Can not find prepared tx for pid: {}", pid);
        co_return make_commit_tx_reply(cluster::tx_errc::unknown_server_error);
    }

    // It is fix for https://github.com/redpanda-data/redpanda/issues/5163.
    // Problem is group_*_tx contains only producer_id in key, so compaction
    // save only last records for this events. We need only save in logs
    // consumed offset after commit_txs. For this we can write store_offset
    // event together with group_commit_tx.
    //
    // Problem: this 2 events contains different record batch type. So we can
    // not put it in one batch and write on disk atomically. But it is not a
    // problem, because client got ok for commit_request (see
    // tx_gateway_frontend). So redpanda will eventually finish commit and
    // complete write for both this events.
    model::record_batch_reader::data_t batches;
    batches.reserve(2);

    cluster::simple_batch_builder store_offset_builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (const auto& [tp, metadata] : prepare_it->second.offsets) {
        update_store_offset_builder(
          store_offset_builder,
          tp.topic,
          tp.partition,
          metadata.offset,
          metadata.committed_leader_epoch,
          metadata.metadata,
          model::timestamp{-1});
    }

    batches.push_back(std::move(store_offset_builder).build());

    group_log_commit_tx commit_tx;
    commit_tx.group_id = group_id;
    auto batch = make_tx_batch(
      model::record_batch_type::group_commit_tx,
      commit_tx_record_version,
      pid,
      std::move(commit_tx));

    batches.push_back(std::move(batch));

    auto reader = model::make_memory_record_batch_reader(std::move(batches));

    auto e = co_await _partition->raft()->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!e) {
        vlog(
          _ctx_txlog.warn,
          "Error \"{}\" on replicating pid:{} commit batch",
          e.error(),
          pid);
        if (
          _partition->raft()->is_leader()
          && _partition->raft()->term() == _term) {
            co_await _partition->raft()->step_down("group tx commit failed");
        }
        co_return make_commit_tx_reply(cluster::tx_errc::timeout);
    }

    prepare_it = _prepared_txs.find(pid);
    if (prepare_it == _prepared_txs.end()) {
        vlog(
          _ctx_txlog.error,
          "can't find already observed prepared tx pid:{}",
          pid);
        co_return make_commit_tx_reply(cluster::tx_errc::unknown_server_error);
    }

    for (const auto& [tp, md] : prepare_it->second.offsets) {
        try_upsert_offset(tp, md);
    }

    _prepared_txs.erase(prepare_it);
    _tx_seqs.erase(pid.get_id());
    _expiration_info.erase(pid);

    co_return make_commit_tx_reply(cluster::tx_errc::none);
}

void group::abort_old_txes() {
    ssx::spawn_with_gate(_gate, [this] {
        return do_abort_old_txes().finally(
          [this] { try_arm(clock_type::now() + _abort_interval_ms); });
    });
}

void group::maybe_rearm_timer() {
    std::optional<time_point_type> earliest_deadline;
    for (auto& [pid, expiration] : _expiration_info) {
        auto candidate = expiration.deadline();
        if (earliest_deadline) {
            earliest_deadline = std::min(earliest_deadline.value(), candidate);
        } else {
            earliest_deadline = candidate;
        }
    }

    if (earliest_deadline) {
        auto deadline = std::min(
          earliest_deadline.value(), clock_type::now() + _abort_interval_ms);
        try_arm(deadline);
    }
}

ss::future<> group::do_abort_old_txes() {
    std::vector<model::producer_identity> pids;
    for (auto& [id, _] : _prepared_txs) {
        pids.push_back(id);
    }
    for (auto& [id, _] : _volatile_txs) {
        pids.push_back(id);
    }
    for (auto& [id, _] : _tx_seqs) {
        auto it = _fence_pid_epoch.find(id);
        if (it != _fence_pid_epoch.end()) {
            pids.push_back(model::producer_identity(id(), it->second));
        }
    }

    absl::btree_set<model::producer_identity> expired;
    for (auto pid : pids) {
        auto expiration_it = _expiration_info.find(pid);
        if (expiration_it != _expiration_info.end()) {
            if (!expiration_it->second.is_expired()) {
                continue;
            }
        }
        expired.insert(pid);
    }

    for (auto pid : expired) {
        co_await try_abort_old_tx(pid);
    }

    maybe_rearm_timer();
}

ss::future<> group::try_abort_old_tx(model::producer_identity pid) {
    return get_tx_lock(pid.get_id())->with([this, pid]() {
        vlog(_ctx_txlog.trace, "attempting to expire pid:{}", pid);

        auto expiration_it = _expiration_info.find(pid);
        if (expiration_it != _expiration_info.end()) {
            if (!expiration_it->second.is_expired()) {
                vlog(_ctx_txlog.trace, "pid:{} isn't expired, skipping", pid);
                return ss::now();
            }
        }

        return do_try_abort_old_tx(pid).discard_result();
    });
}

ss::future<cluster::tx_errc>
group::do_try_abort_old_tx(model::producer_identity pid) {
    vlog(_ctx_txlog.trace, "aborting pid:{}", pid);

    auto p_it = _prepared_txs.find(pid);
    if (p_it != _prepared_txs.end()) {
        auto tx_seq = p_it->second.tx_seq;
        auto r = co_await _tx_frontend.local().try_abort(
          model::partition_id(
            0), // TODO: Pass partition_id to prepare request and use it here.
                // https://github.com/redpanda-data/redpanda/issues/6137
          pid,
          tx_seq,
          config::shard_local_cfg().rm_sync_timeout_ms.value());
        if (r.ec != cluster::tx_errc::none) {
            co_return r.ec;
        }
        if (r.commited) {
            auto res = co_await do_commit(_id, pid);
            if (res.ec != cluster::tx_errc::none) {
                vlog(
                  _ctxlog.warn,
                  "commit of prepared tx pid:{} failed with ec:{}",
                  pid,
                  res.ec);
            }
            co_return res.ec;
        } else if (r.aborted) {
            auto res = co_await do_abort(_id, pid, tx_seq);
            if (res.ec != cluster::tx_errc::none) {
                vlog(
                  _ctxlog.warn,
                  "abort of prepared tx pid:{} failed with ec:{}",
                  pid,
                  res.ec);
            }
            co_return res.ec;
        }

        co_return cluster::tx_errc::stale;
    } else {
        model::tx_seq tx_seq;
        if (is_transaction_ga()) {
            auto txseq_it = _tx_seqs.find(pid.get_id());
            if (txseq_it == _tx_seqs.end()) {
                vlog(
                  _ctx_txlog.trace, "skipping pid:{} (can't find tx_seq)", pid);
                co_return cluster::tx_errc::none;
            }
            tx_seq = txseq_it->second;
        } else {
            auto v_it = _volatile_txs.find(pid);
            if (v_it == _volatile_txs.end()) {
                vlog(
                  _ctx_txlog.trace,
                  "skipping pid:{} (can't find volatile)",
                  pid);
                co_return cluster::tx_errc::none;
            }
            tx_seq = v_it->second.tx_seq;
        }

        auto res = co_await do_abort(_id, pid, tx_seq);
        if (res.ec != cluster::tx_errc::none) {
            vlog(
              _ctxlog.warn,
              "abort of pid:{} tx_seq:{} failed with {}",
              pid,
              tx_seq,
              res.ec);
        }
        co_return res.ec;
    }
}

void group::try_arm(time_point_type deadline) {
    if (
      _auto_abort_timer.armed() && _auto_abort_timer.get_timeout() > deadline) {
        _auto_abort_timer.cancel();
        _auto_abort_timer.arm(deadline);
    } else if (!_auto_abort_timer.armed()) {
        _auto_abort_timer.arm(deadline);
    }
}

std::ostream& operator<<(std::ostream& o, const group::offset_metadata& md) {
    fmt::print(
      o,
      "{{log_offset:{}, offset:{}, metadata:{}, committed_leader_epoch:{}}}",
      md.log_offset,
      md.offset,
      md.metadata,
      md.committed_leader_epoch);
    return o;
}

} // namespace kafka
