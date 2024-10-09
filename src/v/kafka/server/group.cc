// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/group.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "bytes/bytes.h"
#include "cluster/partition.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/tx_utils.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/schemata/describe_groups_response.h"
#include "kafka/protocol/schemata/leave_group_response.h"
#include "kafka/protocol/schemata/offset_fetch_response.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/protocol/txn_offset_commit.h"
#include "kafka/protocol/wire.h"
#include "kafka/server/errors.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/logger.h"
#include "kafka/server/member.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "raft/errc.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"
#include "strings/string_switch.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

namespace kafka {

namespace {

/**
 * Convert the request member protocol list into the type used internally to
 * group membership. We maintain two different types because the internal
 * type is also the type stored on disk and we do not want it to be tied to
 * the type produced by code generation.
 */
chunked_vector<member_protocol>
native_member_protocols(const join_group_request& request) {
    chunked_vector<member_protocol> res;
    res.reserve(request.data.protocols.size());
    std::transform(
      request.data.protocols.cbegin(),
      request.data.protocols.cend(),
      std::back_inserter(res),
      [](const join_group_request_protocol& p) {
          return member_protocol{p.name, p.metadata};
      });
    return res;
}

// group membership helper to compare a protocol set from the wire with our
// internal type without doing a full type conversion.
bool operator==(
  const chunked_vector<join_group_request_protocol>& a,
  const chunked_vector<member_protocol>& b) {
    return std::equal(
      a.cbegin(),
      a.cend(),
      b.cbegin(),
      b.cend(),
      [](const join_group_request_protocol& a, const member_protocol& b) {
          return a.name == b.name && a.metadata == b.metadata;
      });
}

assignments_type member_assignments(sync_group_request request) {
    assignments_type res;
    res.reserve(request.data.assignments.size());
    std::for_each(
      std::begin(request.data.assignments),
      std::end(request.data.assignments),
      [&res](sync_group_request_assignment& a) mutable {
          res.emplace(std::move(a.member_id), std::move(a.assignment));
      });
    return res;
}

} // namespace

using member_config = join_group_response_member;

group::group(
  kafka::group_id id,
  group_state s,
  config::configuration& conf,
  ss::lw_shared_ptr<ssx::rwlock> catchup_lock,
  ss::lw_shared_ptr<cluster::partition> partition,
  model::term_id term,
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
  , _catchup_lock(std::move(catchup_lock))
  , _partition(std::move(partition))
  , _probe(_members, _static_members, _offsets)
  , _ctxlog(klog, *this)
  , _ctx_txlog(cluster::txlog, *this)
  , _md_serializer(std::move(serializer))
  , _term(term)
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
  ss::lw_shared_ptr<ssx::rwlock> catchup_lock,
  ss::lw_shared_ptr<cluster::partition> partition,
  model::term_id term,
  ss::sharded<cluster::tx_gateway_frontend>& tx_frontend,
  ss::sharded<features::feature_table>& feature_table,
  group_metadata_serializer serializer,
  enable_group_metrics group_metrics)
  : _id(std::move(id))
  , _state(md.members.empty() ? group_state::empty : group_state::stable)
  , _state_timestamp(
      md.state_timestamp == model::timestamp(-1)
        ? std::optional<model::timestamp>(std::nullopt)
        : md.state_timestamp)
  , _generation(md.generation)
  , _num_members_joining(0)
  , _protocol_type(md.protocol_type)
  , _protocol(md.protocol)
  , _leader(md.leader)
  , _new_member_added(false)
  , _conf(conf)
  , _catchup_lock(std::move(catchup_lock))
  , _partition(std::move(partition))
  , _probe(_members, _static_members, _offsets)
  , _ctxlog(klog, *this)
  , _ctx_txlog(cluster::txlog, *this)
  , _md_serializer(std::move(serializer))
  , _term(term)
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
          chunked_vector<kafka::member_protocol>{member_protocol{
            .name = _protocol.value_or(protocol_name("")),
            .metadata = iobuf_to_bytes(m.subscription),
          }});
        vlog(_ctxlog.trace, "Initializing group with member {}", member);
        add_member_no_join(member);
    }

    // update when restoring from metadata value
    update_subscriptions();

    if (_enable_group_metrics) {
        _probe.setup_public_metrics(_id);
    }

    start_abort_timer();
}

group::~group() noexcept = default;

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

group::ongoing_transaction::ongoing_transaction(
  model::tx_seq tx_seq,
  model::partition_id coordinator_partition,
  model::timeout_clock::duration tx_timeout)
  : tx_seq(tx_seq)
  , coordinator_partition(coordinator_partition)
  , timeout(tx_timeout)
  , last_update(model::timeout_clock::now()) {}

group::tx_producer::tx_producer(model::producer_epoch epoch)
  : epoch(epoch) {}

namespace {
template<typename T>
model::record_batch make_tx_batch(
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

model::record_batch make_tx_fence_batch(
  const model::producer_identity& pid,
  group_tx::fence_metadata cmd,
  bool use_dedicated_batch_type_for_fence) {
    auto batch_type = use_dedicated_batch_type_for_fence
                        ? model::record_batch_type::group_fence_tx
                        : model::record_batch_type::tx_fence;
    return make_tx_batch(
      batch_type, group::fence_control_record_version, pid, std::move(cmd));
}
} // namespace

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
      fmt::join(
        std::views::transform(
          _supported_protocols,
          [](const auto& p) {
              return fmt::format("({}, {})", p.first, p.second);
          }),
        ","));

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
  member_ptr member,
  chunked_vector<member_protocol>&& new_protocols,
  const std::optional<kafka::client_id>& new_client_id,
  const kafka::client_host& new_client_host,
  std::chrono::milliseconds new_session_timeout,
  std::chrono::milliseconds new_rebalance_timeout) {
    vlog(
      _ctxlog.trace,
      "Updating {}joining member {} with protocols {} and timeouts {}/{}",
      member->is_joining() ? "" : "non-",
      member,
      new_protocols,
      new_session_timeout,
      new_rebalance_timeout);

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

    if (new_client_id) {
        member->replace_client_id(*new_client_id);
    }
    member->replace_client_host(new_client_host);
    member->replace_session_timeout(new_session_timeout);
    member->replace_rebalance_timeout(new_rebalance_timeout);
}

ss::future<join_group_response> group::update_member(
  member_ptr member,
  chunked_vector<member_protocol>&& new_protocols,
  const std::optional<kafka::client_id>& new_client_id,
  const kafka::client_host& new_client_host,
  std::chrono::milliseconds new_session_timeout,
  std::chrono::milliseconds new_rebalance_timeout) {
    update_member_no_join(
      member,
      std::move(new_protocols),
      new_client_id,
      new_client_host,
      new_session_timeout,
      new_rebalance_timeout);

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

chunked_vector<member_config> group::member_metadata() const {
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

    chunked_vector<member_config> out;
    std::transform(
      std::cbegin(_members),
      std::cend(_members),
      std::back_inserter(out),
      [this](const member_map::value_type& m) {
          auto& group_inst = m.second->group_instance_id();
          auto metadata = m.second->get_protocol_metadata(_protocol.value());
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
    update_subscriptions(); // call after protocol is set
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
    // handling of is_new_group etc.. clearer rather than passing these flags
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
         * try_complete_join where we return immediately rather than completing
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

    kafka::client_id old_client_id = member->client_id();
    kafka::client_host old_client_host = member->client_host();
    auto old_session_timeout
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        member->session_timeout());
    auto old_rebalance_timeout
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        member->rebalance_timeout());

    auto f = update_member(
      member,
      native_member_protocols(r),
      r.client_id,
      r.client_host,
      r.data.session_timeout_ms,
      r.data.rebalance_timeout_ms);
    auto old_protocols = _members.at(new_member_id)->protocols().copy();
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
                         old_protocols = std::move(old_protocols),
                         old_client_id = std::move(old_client_id),
                         old_client_host = std::move(old_client_host),
                         old_session_timeout = old_session_timeout,
                         old_rebalance_timeout = old_rebalance_timeout](
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
                            member,
                            std::move(old_protocols),
                            old_client_id,
                            old_client_host,
                            old_session_timeout,
                            old_rebalance_timeout);
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
                      chunked_vector<member_config> md;
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
            chunked_vector<member_config> members;
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

            vlog(_ctxlog.trace, "Handling idempotent group join {}", response);

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
      native_member_protocols(r));

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
      std::move(member),
      native_member_protocols(r),
      r.client_id,
      r.client_host,
      r.data.session_timeout_ms,
      r.data.rebalance_timeout_ms);
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
                  chunked_vector<member_config> md;
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
          "Handling idempotent group sync for member {} with reply {}",
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
    auto assignments = member_assignments(std::move(r));
    add_missing_assignments(assignments);

    // clang-tidy 16.0.4 is reporting an erroneous 'use-after-move' error when
    // calling `then` after `store_group`.
    auto replicate_result = store_group(checkpoint(assignments));
    auto f = replicate_result.then([this,
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
            vlog(_ctxlog.trace, "Group state changed while completing sync");
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
            //   - propagate error back to waiting clients
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
    _producers.clear();
}

void group::insert_ongoing_tx(
  model::producer_identity pid, ongoing_transaction tx) {
    auto [it, inserted] = _producers.try_emplace(pid.get_id(), pid.get_epoch());
    it->second.epoch = pid.get_epoch();
    it->second.transaction = std::make_unique<ongoing_transaction>(
      std::move(tx));
}

ss::future<cluster::commit_group_tx_reply>
group::commit_tx(cluster::commit_group_tx_request r) {
    vlog(_ctx_txlog.trace, "processing commit_tx request: {}", r);
    co_return co_await do_commit(r.group_id, r.pid, r.tx_seq);
}

cluster::begin_group_tx_reply make_begin_tx_reply(cluster::tx::errc ec) {
    cluster::begin_group_tx_reply reply;
    reply.ec = ec;
    return reply;
}

cluster::commit_group_tx_reply make_commit_tx_reply(cluster::tx::errc ec) {
    cluster::commit_group_tx_reply reply;
    reply.ec = ec;
    return reply;
}

cluster::abort_group_tx_reply make_abort_tx_reply(cluster::tx::errc ec) {
    cluster::abort_group_tx_reply reply;
    reply.ec = ec;
    return reply;
}

ss::future<cluster::begin_group_tx_reply>
group::begin_tx(cluster::begin_group_tx_request r) {
    vlog(_ctx_txlog.trace, "processing begin tx request: {}", r);
    if (_partition->term() != _term) {
        vlog(
          _ctx_txlog.debug,
          "begin tx request {} failed - leadership changed. Expected term: {}, "
          "current term: {}",
          r,
          _term,
          _partition->term());
        co_return make_begin_tx_reply(cluster::tx::errc::stale);
    }

    auto it = _producers.find(r.pid.get_id());

    if (it != _producers.end()) {
        auto& producer = it->second;
        if (r.pid.get_epoch() < producer.epoch) {
            vlog(
              _ctx_txlog.warn,
              "begin tx request failed. Producer {} epoch is lower than "
              "current fence epoch: {}",
              r.pid,
              producer.epoch);
            co_return make_begin_tx_reply(cluster::tx::errc::fenced);
        } else if (r.pid.get_epoch() > producer.epoch) {
            // there is a fence, it might be that tm_stm failed, forget about
            // an ongoing transaction, assigned next pid for the same tx.id and
            // started a new transaction without aborting the previous one.
            //
            // at the same time it's possible that it already aborted the old
            // tx before starting this. do_abort_tx is idempotent so calling it
            // just in case to proactively abort the tx instead of waiting for
            // the timeout

            auto old_pid = model::producer_identity{
              r.pid.get_id(), producer.epoch};
            auto ar = co_await do_try_abort_old_tx(old_pid);
            if (ar != cluster::tx::errc::none) {
                vlog(
                  _ctx_txlog.warn,
                  "begin tx request {} failed, can not abort old transaction: "
                  "{} - {}",
                  r,
                  old_pid,
                  ar);
                co_return make_begin_tx_reply(cluster::tx::errc::stale);
            }
        }
        if (producer.transaction) {
            auto& producer_tx = *producer.transaction;
            if (r.tx_seq != producer_tx.tx_seq) {
                vlog(
                  _ctx_txlog.warn,
                  "begin tx request {} failed - produced has already ongoing "
                  "transaction with different sequence number: {}",
                  r,
                  producer_tx.tx_seq);
                co_return make_begin_tx_reply(
                  cluster::tx::errc::unknown_server_error);
            }

            if (!producer_tx.offsets.empty()) {
                vlog(
                  _ctx_txlog.warn,
                  "begin tx request {} failed - transaction is already ongoing "
                  "and accepted offset commits",
                  r);
                co_return make_begin_tx_reply(
                  cluster::tx::errc::unknown_server_error);
            }
            // begin_tx request is idempotent, return success
            co_return cluster::begin_group_tx_reply(
              _term, cluster::tx::errc::none);
        }
    }

    group_tx::fence_metadata fence{
      .group_id = id(),
      .tx_seq = r.tx_seq,
      .transaction_timeout_ms = r.timeout,
      .tm_partition = r.tm_partition};
    // replicate fence batch - this is a transaction boundary
    model::record_batch batch = make_tx_fence_batch(
      r.pid, std::move(fence), use_dedicated_batch_type_for_fence());
    auto reader = model::make_memory_record_batch_reader(std::move(batch));
    auto result = co_await _partition->raft()->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!result) {
        vlog(
          _ctx_txlog.warn,
          "begin tx request {} failed - error replicating fencing batch - {}",
          r,
          result.error().message());
        if (
          _partition->raft()->is_leader()
          && _partition->raft()->term() == _term) {
            co_await _partition->raft()->step_down("group begin_tx failed");
        }
        co_return map_tx_replication_error(result.error());
    }
    auto [producer_it, _] = _producers.try_emplace(
      r.pid.get_id(), r.pid.get_epoch());
    producer_it->second.epoch = r.pid.get_epoch();
    producer_it->second.transaction = std::make_unique<ongoing_transaction>(
      ongoing_transaction(r.tx_seq, r.tm_partition, r.timeout));

    try_arm(producer_it->second.transaction->deadline());

    co_return cluster::begin_group_tx_reply(_term, cluster::tx::errc::none);
}

ss::future<cluster::abort_group_tx_reply>
group::abort_tx(cluster::abort_group_tx_request r) {
    vlog(_ctxlog.trace, "processing abort_tx request: {}", r);
    co_return co_await do_abort(r.group_id, r.pid, r.tx_seq);
}

cluster::tx::errc group::map_tx_replication_error(std::error_code ec) {
    auto result_ec = cluster::tx::errc::none;
    // All generic errors are mapped to not coordinator to force the client to
    // retry, the errors like timeout and shutdown are mapped to timeout to
    // indicate the uncertainty of the operation outcome
    if (ec.category() == raft::error_category()) {
        switch (static_cast<raft::errc>(ec.value())) {
        case raft::errc::shutting_down:
        case raft::errc::timeout:
            result_ec = cluster::tx::errc::timeout;
            break;
        default:
            result_ec = cluster::tx::errc::not_coordinator;
        }
    } else if (ec.category() == cluster::error_category()) {
        switch (static_cast<cluster::errc>(ec.value())) {
        case cluster::errc::shutting_down:
        case cluster::errc::timeout:
            result_ec = cluster::tx::errc::timeout;
            break;
        default:
            result_ec = cluster::tx::errc::not_coordinator;
        }
    } else {
        vlog(_ctx_txlog.warn, "unexpected replication error: {}", ec);
        result_ec = cluster::tx::errc::not_coordinator;
    }

    vlog(
      _ctx_txlog.info,
      "transactional batch replication error: {}, mapped to: {}",
      ec,
      result_ec);
    return result_ec;
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
    auto it = _producers.find(pid.get_id());
    if (it == _producers.end()) {
        vlog(
          _ctx_txlog.warn,
          "Can't store txn offsets: fence with pid {} isn't set",
          pid);
        co_return txn_offset_commit_response(
          r, error_code::invalid_producer_epoch);
    }
    auto& producer = it->second;
    if (r.data.producer_epoch != producer.epoch) {
        vlog(
          _ctx_txlog.trace,
          "Can't store txn offsets with pid {} - the fence doesn't match {}",
          pid,
          producer.epoch);
        co_return txn_offset_commit_response(
          r, error_code::invalid_producer_epoch);
    }

    if (producer.transaction == nullptr) {
        vlog(
          _ctx_txlog.warn,
          "Can't store txn offsets: current tx with pid {} isn't ongoing",
          pid);
        co_return txn_offset_commit_response(
          r, error_code::invalid_producer_epoch);
    }

    auto& producer_tx = *producer.transaction;

    chunked_vector<group_tx::partition_offset> offsets;

    for (auto& t : r.data.topics) {
        for (const auto& p : t.partitions) {
            offsets.push_back(group_tx::partition_offset{
              .tp = model::topic_partition(t.name, p.partition_index),
              .offset = p.committed_offset,
              .leader_epoch = p.committed_leader_epoch,
              .metadata = p.committed_metadata,
            });
        }
    }

    group_tx::offsets_metadata tx_entry{
      .group_id = r.data.group_id,
      .pid = pid,
      .tx_seq = producer_tx.tx_seq,
      .offsets = {offsets.begin(), offsets.end()},
    };

    auto batch = make_tx_batch(
      model::record_batch_type::group_prepare_tx,
      prepared_tx_record_version,
      pid,
      std::move(tx_entry));
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto result = co_await _partition->raft()->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!result) {
        if (
          _partition->raft()->is_leader()
          && _partition->raft()->term() == _term) {
            co_await _partition->raft()->step_down(
              "group store_txn_offsets failed");
        }
        auto tx_ec = map_tx_replication_error(result.error());

        co_return txn_offset_commit_response(r, map_tx_errc(tx_ec));
    }

    it = _producers.find(pid.get_id());
    if (it == _producers.end() || it->second.transaction == nullptr) {
        vlog(
          _ctx_txlog.warn,
          "Can't store txn offsets: current tx with pid {} isn't ongoing",
          pid);
        co_return txn_offset_commit_response(
          r, error_code::invalid_producer_epoch);
    }
    auto& ongoing_tx = *it->second.transaction;
    for (auto& o : offsets) {
        ongoing_tx.offsets[o.tp] = pending_tx_offset{
          .offset_metadata = o,
          .log_offset = result.value().last_offset,
        };
    }
    ongoing_tx.update_last_update_time();

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
        case raft::errc::invalid_input_records:
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
  model::timestamp commit_timestamp,
  std::optional<model::timestamp> expiry_timestamp) {
    offset_metadata_key key{
      .group_id = _id, .topic = name, .partition = partition};

    offset_metadata_value value{
      .offset = committed_offset,
      .leader_epoch = committed_leader_epoch,
      .metadata = metadata,
      .commit_timestamp = commit_timestamp,
    };

    if (expiry_timestamp.has_value()) {
        value.expiry_timestamp = expiry_timestamp.value();
    }

    auto kv = _md_serializer.to_kv(
      offset_metadata_kv{.key = std::move(key), .value = std::move(value)});
    builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
}

group::offset_commit_stages group::store_offsets(offset_commit_request&& r) {
    cluster::simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    std::vector<std::pair<model::topic_partition, offset_metadata>>
      offset_commits;

    const auto expiry_timestamp = [&r]() -> std::optional<model::timestamp> {
        if (r.data.retention_time_ms == -1) {
            return std::nullopt;
        }
        return model::timestamp(
          model::timestamp::now().value() + r.data.retention_time_ms);
    }();

    const auto get_commit_timestamp =
      [](const offset_commit_request_partition& p) {
          if (p.commit_timestamp == -1) {
              return model::timestamp::now();
          }
          return model::timestamp(p.commit_timestamp);
      };

    for (const auto& t : r.data.topics) {
        for (const auto& p : t.partitions) {
            const auto commit_timestamp = get_commit_timestamp(p);
            update_store_offset_builder(
              builder,
              t.name,
              p.partition_index,
              p.committed_offset,
              p.committed_leader_epoch,
              p.committed_metadata.value_or(""),
              commit_timestamp,
              expiry_timestamp);

            model::topic_partition tp(t.name, p.partition_index);

            /*
             * .non_reclaimable defaults to false and this metadata will end up
             * replacing the metadata in existing registered offsets. this has
             * the effect that if a committed offset was recovered as
             * legacy/pre-v23 and is non-reclaimable that it then becomes
             * reclaimable, which is the behavior we want. it is effectively no
             * longer a legacy committed offset.
             */
            offset_metadata md{
              .offset = p.committed_offset,
              .metadata = p.committed_metadata.value_or(""),
              .committed_leader_epoch = p.committed_leader_epoch,
              .commit_timestamp = commit_timestamp,
              .expiry_timestamp = expiry_timestamp,
            };

            offset_commits.emplace_back(tp, md);

            // record the offset commits as pending commits which will be
            // inspected after the append to catch concurrent updates.
            _pending_offset_commits[tp] = md;
        }
    }
    if (builder.empty()) {
        vlog(_ctxlog.debug, "Empty offsets committed request");
        return offset_commit_stages(
          offset_commit_response(r, error_code::none));
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
    return {std::move(replicate_stages.request_enqueued), std::move(f)};
}

ss::future<cluster::commit_group_tx_reply>
group::handle_commit_tx(cluster::commit_group_tx_request r) {
    if (in_state(group_state::dead)) {
        co_return make_commit_tx_reply(
          cluster::tx::errc::coordinator_not_available);
    } else if (
      in_state(group_state::empty) || in_state(group_state::stable)
      || in_state(group_state::preparing_rebalance)) {
        auto id = r.pid.get_id();
        co_return co_await with_pid_lock(
          id, [this, r = std::move(r)]() mutable {
              return commit_tx(std::move(r));
          });
    } else if (in_state(group_state::completing_rebalance)) {
        co_return make_commit_tx_reply(
          cluster::tx::errc::rebalance_in_progress);
    } else {
        vlog(_ctx_txlog.error, "Unexpected group state");
        co_return make_commit_tx_reply(cluster::tx::errc::timeout);
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
            vlog(
              _ctx_txlog.warn,
              "fenced producer {} with out of date group metadata "
              "{{generation: {}, group_instance_id: {}, member_id: {}}} - {}",
              r.data.producer_id,
              r.data.generation_id,
              r.data.group_instance_id,
              r.data.member_id,
              check_res);
            co_return txn_offset_commit_response(r, check_res);
        }

        auto id = model::producer_id(r.data.producer_id());
        co_return co_await with_pid_lock(
          id, [this, r = std::move(r)]() mutable {
              return store_txn_offsets(std::move(r));
          });
    } else if (in_state(group_state::completing_rebalance)) {
        co_return txn_offset_commit_response(
          r, error_code::concurrent_transactions);
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
        reply.ec = cluster::tx::errc::coordinator_not_available;
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
        /**
         * When group is completing rebalance it doesn't makes sense to
         * replicate the fence batch as the transaction may be fenced with group
         * generation change, in this case return an error instructing client to
         * retry.
         */
        cluster::begin_group_tx_reply reply;
        reply.ec = cluster::tx::errc::concurrent_transactions;
        co_return reply;
    } else {
        vlog(_ctx_txlog.error, "Unexpected group state");
        cluster::begin_group_tx_reply reply;
        reply.ec = cluster::tx::errc::timeout;
        co_return reply;
    }
}

ss::future<cluster::abort_group_tx_reply>
group::handle_abort_tx(cluster::abort_group_tx_request r) {
    if (in_state(group_state::dead)) {
        cluster::abort_group_tx_reply reply;
        reply.ec = cluster::tx::errc::coordinator_not_available;
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
        reply.ec = cluster::tx::errc::concurrent_transactions;
        co_return reply;
    } else {
        vlog(_ctx_txlog.error, "Unexpected group state");
        cluster::abort_group_tx_reply reply;
        reply.ec = cluster::tx::errc::timeout;
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
          small_fragment_vector<offset_fetch_response_partition>>
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
                    p.committed_leader_epoch = res->committed_leader_epoch;
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

void group::add_offset_tombstone_record(
  const kafka::group_id& group,
  const model::topic_partition& tp,
  storage::record_batch_builder& builder) {
    offset_metadata_key key{
      .group_id = group,
      .topic = tp.topic,
      .partition = tp.partition,
    };
    auto kv = _md_serializer.to_kv(offset_metadata_kv{.key = std::move(key)});
    builder.add_raw_kv(std::move(kv.key), std::nullopt);
}

void group::add_group_tombstone_record(
  const kafka::group_id& group, storage::record_batch_builder& builder) {
    group_metadata_key key{
      .group_id = group,
    };
    auto kv = _md_serializer.to_kv(group_metadata_kv{.key = std::move(key)});
    builder.add_raw_kv(std::move(kv.key), std::nullopt);
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
      model::record_batch_type::raft_data, model::offset(0));

    for (auto& offset : _offsets) {
        add_offset_tombstone_record(_id, offset.first, builder);
    }

    // build group tombstone
    add_group_tombstone_record(_id, builder);

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
              klog.warn,
              "Error occurred replicating group {} delete records {} ({})",
              _id,
              result.error().message(),
              result.error());
        }
    } catch (const std::exception& e) {
        vlog(
          klog.error,
          "Exception occurred replicating group {} delete records {}",
          _id,
          e);
    }

    // kafka chooses to report no error even if replication fails. the in-memory
    // state on this node is still correctly represented.
    co_return error_code::none;
}

ss::future<> group::remove_topic_partitions(
  const chunked_vector<model::topic_partition>& tps) {
    chunked_vector<std::pair<model::topic_partition, offset_metadata>> removed;
    for (const auto& tp : tps) {
        _pending_offset_commits.erase(tp);
        if (auto offset = _offsets.extract(tp); offset) {
            removed.emplace_back(
              std::move(offset->first), std::move(offset->second->metadata));
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
        add_offset_tombstone_record(_id, offset.first, builder);
    }

    // gc the group?
    if (in_state(group_state::dead) && generation() > 0) {
        add_group_tombstone_record(_id, builder);
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
            // TODO: consider adding retries in this case
            vlog(
              klog.warn,
              "Error occurred replicating group {} cleanup records {} ({})",
              _id,
              result.error().message(),
              result.error());
        }
    } catch (const std::exception& e) {
        vlog(
          klog.error,
          "Exception occurred replicating group {} cleanup records {}",
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
        return ss::sstring(group_state_name_empty);
    case group_state::preparing_rebalance:
        return ss::sstring(group_state_name_preparing_rebalance);
    case group_state::completing_rebalance:
        return ss::sstring(group_state_name_completing_rebalance);
    case group_state::stable:
        return ss::sstring(group_state_name_stable);
    case group_state::dead:
        return ss::sstring(group_state_name_dead);
    default:
        std::terminate(); // make gcc happy
    }
}

std::optional<group_state> group_state_from_kafka_name(std::string_view name) {
    return string_switch<std::optional<group_state>>(name)
      .match(group_state_to_kafka_name(group_state::empty), group_state::empty)
      .match(
        group_state_to_kafka_name(group_state::preparing_rebalance),
        group_state::preparing_rebalance)
      .match(
        group_state_to_kafka_name(group_state::completing_rebalance),
        group_state::completing_rebalance)
      .match(
        group_state_to_kafka_name(group_state::stable), group_state::stable)
      .match(group_state_to_kafka_name(group_state::dead), group_state::dead)
      .default_match(std::nullopt);
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
    vlog(
      _ctxlog.trace,
      "processing do_abort_tx request: producer: {}, sequence: {}",
      group_id,
      pid,
      tx_seq);
    if (_partition->term() != _term) {
        vlog(
          _ctxlog.debug,
          "do_abort_tx request: failed - leadership changed, expected term: "
          "{}, current term: {}, pid: {}, sequence: {}",
          _term,
          _partition->term(),
          pid,
          tx_seq);
        co_return make_abort_tx_reply(cluster::tx::errc::stale);
    }
    auto it = _producers.find(pid.get_id());
    if (it == _producers.end() || it->second.transaction == nullptr) {
        // It could be a replay request from the coordinator to roll back
        // the transaction. It is possible that the state got cleaned up
        // between the original and the current replay request. We assume
        // aborted because this request confirms that the coordinator sees a
        // tx abort in the log and the original request should have been a
        // abort too.
        vlog(
          _ctx_txlog.info,
          "do_abort_tx request:- producer/transaction {} not found, sequence: "
          "{}, assuming already aborted.",
          pid,
          tx_seq);
        co_return make_abort_tx_reply(cluster::tx::errc::none);
    }
    auto& producer = it->second;
    if (pid.get_epoch() != producer.epoch) {
        vlog(
          _ctx_txlog.warn,
          "do_abort_tx request: {} failed - fence epoch mismatch. Fence epoch: "
          "{}",
          pid,
          producer.epoch);
        co_return make_abort_tx_reply(cluster::tx::errc::request_rejected);
    }

    if (producer.transaction == nullptr) {
        vlog(
          _ctx_txlog.trace,
          "unable to find transaction for {}, probably already aborted",
          pid);
        co_return make_abort_tx_reply(cluster::tx::errc::none);
    }
    auto& producer_tx = *producer.transaction;
    if (producer_tx.tx_seq > tx_seq) {
        // rare situation:
        //   * tm_stm begins (tx_seq+1)
        //   * request on this group passes but then tm_stm fails and forgets
        //   about this tx
        //   * during recovery tm_stm reaborts previous tx (tx_seq)
        // existence of {pid, tx_seq+1} implies {pid, tx_seq} is aborted
        vlog(
          _ctx_txlog.trace,
          "producer transaction {} already aborted, ongoing tx sequence: {}, "
          "request tx sequence: {}",
          pid,
          producer_tx.tx_seq,
          tx_seq);
        co_return make_abort_tx_reply(cluster::tx::errc::none);
    }

    if (producer_tx.tx_seq != tx_seq) {
        vlog(
          _ctx_txlog.warn,
          "do_abort_tx request: {} failed - tx sequence mismatch. Ongoing tx "
          "sequence: {}, request tx sequence: {}",
          pid,
          producer_tx.tx_seq,
          tx_seq);
        co_return make_abort_tx_reply(cluster::tx::errc::request_rejected);
    }
    auto tx = group_tx::abort_metadata{.group_id = group_id, .tx_seq = tx_seq};

    auto batch = make_tx_batch(
      model::record_batch_type::group_abort_tx,
      aborted_tx_record_version,
      pid,
      std::move(tx));
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto result = co_await _partition->raft()->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!result) {
        vlog(
          _ctx_txlog.warn,
          "Error \"{}\" on replicating pid:{} abort batch",
          result.error(),
          pid);
        if (
          _partition->raft()->is_leader()
          && _partition->raft()->term() == _term) {
            co_await _partition->raft()->step_down("group do abort failed");
        }
        co_return map_tx_replication_error(result.error());
    }
    it = _producers.find(pid.get_id());
    if (it != _producers.end()) {
        it->second.transaction.reset();
    }
    co_return make_abort_tx_reply(cluster::tx::errc::none);
}

ss::future<cluster::commit_group_tx_reply> group::do_commit(
  kafka::group_id group_id,
  model::producer_identity pid,
  model::tx_seq sequence) {
    vlog(
      _ctx_txlog.trace,
      "processing do_commit_tx request: pid: {}",
      group_id,
      pid);
    if (_partition->term() != _term) {
        vlog(
          _ctx_txlog.warn,
          "do_commit_tx request: pid: {} failed - "
          "leadership_changed, expected term: "
          "{}, current_term: {}",
          pid,
          _term,
          _partition->term());
        co_return make_commit_tx_reply(cluster::tx::errc::stale);
    }
    auto it = _producers.find(pid.get_id());
    if (it == _producers.end() || it->second.transaction == nullptr) {
        // It could be a replay request from the coordinator to roll forward
        // the transaction. It is possible that the state got cleaned up
        // between the original and the current replay request. We assume
        // committed because this request confirms that the coordinator sees a
        // tx commit in the log and the original request should have been a
        // commit too.
        vlog(
          _ctx_txlog.info,
          "do_commit_tx request:- producer/transaction {} not found, sequence: "
          "{}, assuming already committed.",
          pid,
          sequence);
        co_return make_commit_tx_reply(cluster::tx::errc::none);
    }
    auto& producer = it->second;
    if (pid.get_epoch() != producer.epoch) {
        vlog(
          _ctx_txlog.warn,
          "do_commit_tx request: pid: {} failed - fenced, stored "
          "producer epoch: {}",
          pid,
          producer.epoch);
        co_return make_commit_tx_reply(cluster::tx::errc::request_rejected);
    }

    if (producer.transaction == nullptr) {
        vlog(
          _ctx_txlog.trace,
          "do_commit_tx request: producer: {} - can not find "
          "ongoing transaction, it was "
          "most likely already committed",
          pid);
        co_return make_commit_tx_reply(cluster::tx::errc::none);
    }
    auto& producer_tx = *producer.transaction;
    if (producer_tx.tx_seq > sequence) {
        // rare situation:
        //   * tm_stm begins (tx_seq+1)
        //   * request on this group passes but then tm_stm fails and forgets
        //   about this tx
        //   * during recovery tm_stm recommits previous tx (tx_seq)
        // existence of {pid, tx_seq+1} implies {pid, tx_seq} is committed
        vlog(
          _ctx_txlog.trace,
          "Already commited pid: {} tx_seq: {} - a higher tx_seq: {} was "
          "observed",
          pid,
          sequence,
          producer_tx.tx_seq);
        co_return make_commit_tx_reply(cluster::tx::errc::none);
    }
    if (producer_tx.tx_seq != sequence) {
        vlog(
          _ctx_txlog.warn,
          "commit_tx request: pid: {}, sequence: {} failed - tx_seq mismatch. "
          "Expected seq: {}",
          pid,
          sequence,
          producer_tx.tx_seq);
        co_return make_commit_tx_reply(cluster::tx::errc::request_rejected);
    }
    auto& ongoing_tx = *it->second.transaction;
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
    // if pending offsets are empty, (there was no store_txn_offsets call, do
    // not replicate the offsets update batch)
    if (!ongoing_tx.offsets.empty()) {
        cluster::simple_batch_builder store_offset_builder(
          model::record_batch_type::raft_data, model::offset(0));
        for (const auto& [tp, pending_offset] : ongoing_tx.offsets) {
            update_store_offset_builder(
              store_offset_builder,
              tp.topic,
              tp.partition,
              pending_offset.offset_metadata.offset,
              kafka::leader_epoch(pending_offset.offset_metadata.leader_epoch),
              pending_offset.offset_metadata.metadata.value_or(""),
              model::timestamp::now(),
              std::nullopt);
        }

        batches.push_back(std::move(store_offset_builder).build());
    }

    group_tx::commit_metadata commit_tx;
    commit_tx.group_id = group_id;
    auto batch = make_tx_batch(
      model::record_batch_type::group_commit_tx,
      commit_tx_record_version,
      pid,
      std::move(commit_tx));

    batches.push_back(std::move(batch));

    auto reader = model::make_memory_record_batch_reader(std::move(batches));

    auto result = co_await _partition->raft()->replicate(
      _term,
      std::move(reader),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (!result) {
        vlog(
          _ctx_txlog.warn,
          "error replicating transaction commit batch for pid: {} - {}",
          pid,
          result.error().message());
        if (
          _partition->raft()->is_leader()
          && _partition->raft()->term() == _term) {
            co_await _partition->raft()->step_down("group tx commit failed");
        }
        co_return map_tx_replication_error(result.error());
    }

    it = _producers.find(pid.get_id());
    if (it == _producers.end() || it->second.transaction == nullptr) {
        vlog(
          _ctx_txlog.error,
          "unable to find ongoing transaction for producer: {}",
          pid);
        co_return make_commit_tx_reply(cluster::tx::errc::unknown_server_error);
    }

    for (const auto& [tp, md] : it->second.transaction->offsets) {
        try_upsert_offset(
          tp,
          offset_metadata{
            .log_offset = md.log_offset,
            .offset = md.offset_metadata.offset,
            .metadata = md.offset_metadata.metadata.value_or(""),
            .committed_leader_epoch = kafka::leader_epoch(
              md.offset_metadata.leader_epoch),
            .commit_timestamp = model::timestamp::now(),
          });
    }

    it->second.transaction.reset();

    co_return make_commit_tx_reply(cluster::tx::errc::none);
}

void group::abort_old_txes() {
    ssx::spawn_with_gate(_gate, [this] {
        return do_abort_old_txes().finally(
          [this] { try_arm(clock_type::now() + _abort_interval_ms); });
    });
}

void group::maybe_rearm_timer() {
    std::optional<time_point_type> earliest_deadline;
    for (auto& [pid, producer] : _producers) {
        if (producer.transaction == nullptr) {
            continue;
        }
        auto candidate = producer.transaction->deadline();
        if (earliest_deadline) {
            earliest_deadline = std::min(earliest_deadline.value(), candidate);
        } else {
            earliest_deadline = candidate;
        }
    }

    if (earliest_deadline) {
        auto deadline = std::min(
          earliest_deadline.value(), clock_type::now() + _abort_interval_ms);
        // never arm the next timer to be earlier than 500ms from now to prevent
        // busy looping
        deadline = std::max(
          clock_type::now() + _abort_interval_ms / 10, deadline);
        try_arm(deadline);
    }
}

ss::future<> group::do_abort_old_txes() {
    auto unit = _catchup_lock->attempt_read_lock();
    if (!unit) {
        co_return;
    }

    absl::btree_set<model::producer_identity> expired;

    for (auto& [pid, producer] : _producers) {
        if (
          producer.transaction == nullptr
          || !producer.transaction->is_expired()) {
            continue;
        }

        expired.insert(model::producer_identity{pid, producer.epoch});
    }
    bool has_error = false;
    for (auto pid : expired) {
        auto ec = co_await try_abort_old_tx(pid);
        if (ec != cluster::tx::errc::none) {
            has_error = true;
        }
    }

    if (!has_error) {
        // if no error was triggered during abort of transaction we may try to
        // schedule a next expiration earlier if there are transactions pending
        // to be expired
        maybe_rearm_timer();
    }
}

ss::future<cluster::tx::errc>
group::try_abort_old_tx(model::producer_identity pid) {
    auto lock = get_tx_lock(pid.get_id());
    auto u = co_await lock->get_units();
    vlog(
      _ctx_txlog.info,
      "attempting expiration of producer: {} transaction",
      pid);

    auto result = co_await do_try_abort_old_tx(pid);
    vlogl(
      _ctx_txlog,
      result == cluster::tx::errc::none ? ss::log_level::trace
                                        : ss::log_level::warn,
      "producer {} transaction expiration result: {}",
      pid,
      result);
    co_return result;
}

ss::future<cluster::tx::errc>
group::do_try_abort_old_tx(model::producer_identity pid) {
    vlog(_ctx_txlog.trace, "aborting producer {} transaction", pid);

    auto it = _producers.find(pid.get_id());
    if (it == _producers.end() || it->second.transaction == nullptr) {
        co_return cluster::tx::errc::none;
    }
    auto& producer_tx = *it->second.transaction;

    vlog(
      _ctx_txlog.trace,
      "sending abort tx request for producer {} with tx_seq: {} to "
      "coordinator partition: {}",
      pid,
      producer_tx.tx_seq,
      producer_tx.coordinator_partition);
    auto tx_seq = producer_tx.tx_seq;
    auto r = co_await _tx_frontend.local().route_globally(
      cluster::try_abort_request(
        producer_tx.coordinator_partition,
        pid,
        producer_tx.tx_seq,
        config::shard_local_cfg().rm_sync_timeout_ms.value()));

    if (r.ec != cluster::tx::errc::none) {
        co_return r.ec;
    }
    vlog(
      _ctx_txlog.trace,
      "producer id {} abort request result: [committed: {}, aborted: {}]",
      pid,
      r.commited,
      r.aborted);

    if (r.commited) {
        auto res = co_await do_commit(_id, pid, tx_seq);
        if (res.ec != cluster::tx::errc::none) {
            vlog(
              _ctxlog.warn,
              "committing producer {} transaction failed - {}",
              pid,
              res.ec);
        }
        co_return res.ec;
    }

    if (r.aborted) {
        auto res = co_await do_abort(_id, pid, producer_tx.tx_seq);
        if (res.ec != cluster::tx::errc::none) {
            vlog(
              _ctxlog.warn,
              "aborting producer {} transaction failed - {}",
              pid,
              res.ec);
        }
        co_return res.ec;
    }

    co_return cluster::tx::errc::stale;
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
      "{{log_offset:{}, offset:{}, metadata:{}, "
      "committed_leader_epoch:{}}}",
      md.log_offset,
      md.offset,
      md.metadata,
      md.committed_leader_epoch);
    return o;
}

bool group::subscribed(const model::topic& topic) const {
    if (_subscriptions.has_value()) {
        return _subscriptions.value().contains(topic);
    }
    // answer conservatively
    return true;
}

/*
 * Currently only supports Version 0 which is a prefix version for higher
 * versions and contains all of the information that we need.
 */
absl::node_hash_set<model::topic>
group::decode_consumer_subscriptions(iobuf data) {
    constexpr auto max_topic_name_length = 32_KiB;

    protocol::decoder reader(std::move(data));

    /* version intentionally ignored */
    reader.read_int16();

    const auto count = reader.read_int32();
    if (count < 0) {
        throw std::out_of_range(fmt::format(
          "consumer metadata contains negative topic count {}", count));
    }

    // a simple heuristic to avoid large allocations
    if (static_cast<size_t>(count) > reader.bytes_left()) {
        throw std::out_of_range(fmt::format(
          "consumer metadata topic count too large {} > {}",
          count,
          reader.bytes_left()));
    }

    absl::node_hash_set<model::topic> topics;
    topics.reserve(count);

    while (topics.size() != static_cast<size_t>(count)) {
        const auto len = reader.read_int16();
        if (len < 0) {
            throw std::out_of_range(fmt::format(
              "consumer metadata contains negative topic name length {}", len));
        } else if (static_cast<size_t>(len) > max_topic_name_length) {
            throw std::out_of_range(fmt::format(
              "consumer metadata contains topic name that exceeds maximum size "
              "{} > {}",
              len,
              max_topic_name_length));
        }
        auto name = reader.read_string_unchecked(len);
        topics.insert(model::topic(std::move(name)));
    }

    // after the topic list is user data bytes, followed by extra bytes for
    // versions > 0, neither of which are needed.

    return topics;
}

void group::update_subscriptions() {
    if (_protocol_type != consumer_group_protocol_type) {
        _subscriptions.reset();
        return;
    }

    if (_members.empty()) {
        _subscriptions.emplace();
        return;
    }

    if (!_protocol.has_value()) {
        _subscriptions.reset();
        return;
    }

    absl::node_hash_set<model::topic> subs;
    for (auto& member : _members) {
        try {
            auto data = bytes_to_iobuf(
              member.second->get_protocol_metadata(_protocol.value()));
            subs.merge(decode_consumer_subscriptions(std::move(data)));
        } catch (const std::out_of_range& e) {
            vlog(
              klog.warn,
              "Parsing consumer:{} data for group {} member {} failed: {}",
              _protocol.value(),
              _id,
              member.first,
              e);
            _subscriptions.reset();
            return;
        }
    }

    _subscriptions = std::move(subs);
}

std::vector<model::topic_partition> group::filter_expired_offsets(
  std::chrono::seconds retention_period,
  const std::function<bool(const model::topic&)>& subscribed,
  const std::function<model::timestamp(const offset_metadata&)>&
    effective_expires) {
    /*
     * default retention duration
     */
    const auto retain_for = model::timestamp(
      std::chrono::duration_cast<std::chrono::milliseconds>(retention_period)
        .count());

    const auto now = model::timestamp::now();
    std::vector<model::topic_partition> offsets;
    for (const auto& offset : _offsets) {
        if (offset.second->metadata.non_reclaimable) {
            continue;
        }

        /*
         * an offset won't be removed if its topic has an active subscription or
         * there are pending offset commits for the offset's topic.
         */
        if (
          subscribed(offset.first.topic)
          || _pending_offset_commits.contains(offset.first)) {
            continue;
        }

        if (offset.second->metadata.expiry_timestamp.has_value()) {
            /*
             * the old way is explicit expiration time point per offset
             */
            const auto& expires
              = offset.second->metadata.expiry_timestamp.value();
            if (expires > now) {
                continue;
            }
        } else {
            /*
             * the new way is a configurable global retention duration
             */
            const auto expires = effective_expires(offset.second->metadata);
            if (model::timestamp(now() - expires()) < retain_for) {
                continue;
            }
        }

        offsets.push_back(offset.first);
    }

    return offsets;
}

std::vector<model::topic_partition>
group::get_expired_offsets(std::chrono::seconds retention_period) {
    const auto not_subscribed = [](const auto&) { return false; };

    if (_protocol_type.has_value()) {
        if (
          _protocol_type.value() == consumer_group_protocol_type
          && _subscriptions.has_value() && in_state(group_state::stable)) {
            /*
             * policy description from kafka source:
             *
             * <kafka>
             * the group is stable and consumers exist.
             *
             * if an offset's topic is subscribed to and retention period has
             * passed since the last commit timestamp, then expire the offset.
             * offsets with pending commit are not expired.
             * </kafka>
             */
            return filter_expired_offsets(
              retention_period,
              [this](const auto& topic) {
                  return _subscriptions.value().contains(topic);
              },
              [](const offset_metadata& md) { return md.commit_timestamp; });

        } else if (in_state(group_state::empty)) {
            /*
             * policy description from kafka source:
             *
             * <kafka>
             * the group contains no consumers.
             *
             * if current state timestamp exists and retention period has passed
             * since group became empty, expire all offsets with no pending
             * offset commit.
             *
             * if there is no current state timestamp (old group metadata
             * schema) and retention period has passed since the last commit
             * timestamp, expire the offset
             * </kafka>
             */
            return filter_expired_offsets(
              retention_period,
              not_subscribed,
              [this](const offset_metadata& md) {
                  if (_state_timestamp.has_value()) {
                      return _state_timestamp.value();
                  } else {
                      return md.commit_timestamp;
                  }
              });
        } else {
            return {};
        }
    } else {
        /*
         * policy description from kafka source:
         *
         * <kafka>
         * there is no configured protocol type, so this is a standalone/simple
         * consumer that Kafka uses for offset storage only.
         *
         * expire offsets with no pending offset commits for which the retention
         * period has passed since their last commit.
         * </kafka>
         */
        return filter_expired_offsets(
          retention_period, not_subscribed, [](const offset_metadata& md) {
              return md.commit_timestamp;
          });
    }
}

bool group::has_offsets() const {
    return !_offsets.empty() || !_pending_offset_commits.empty()
           || std::any_of(
             _producers.begin(),
             _producers.end(),
             [](const producers_map::value_type& p) {
                 return p.second.transaction != nullptr;
             });
}

std::vector<model::topic_partition>
group::delete_expired_offsets(std::chrono::seconds retention_period) {
    /*
     * collect and delete expired offsets
     */
    auto offsets = get_expired_offsets(retention_period);
    for (const auto& offset : offsets) {
        vlog(_ctxlog.debug, "Expiring group offset {}", offset);
        _offsets.erase(offset);
    }

    /*
     * maybe mark the group as dead
     */
    if (in_state(group_state::empty) && !has_offsets()) {
        set_state(group_state::dead);
    }

    return offsets;
}

std::vector<model::topic_partition>
group::delete_offsets(std::vector<model::topic_partition> offsets) {
    std::vector<model::topic_partition> deleted_offsets;
    /*
     * Delete the requested offsets, unless there is at least one active
     * subscription for an offset.
     */
    for (auto& offset : offsets) {
        if (!subscribed(offset.topic)) {
            vlog(_ctxlog.debug, "Deleting group offset {}", offset);
            _offsets.erase(offset);
            _pending_offset_commits.erase(offset);
            deleted_offsets.push_back(std::move(offset));
        }
    }

    /*
     * maybe mark the group as dead
     */
    if (in_state(group_state::empty) && !has_offsets()) {
        set_state(group_state::dead);
    }

    return deleted_offsets;
}

} // namespace kafka
