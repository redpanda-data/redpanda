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

#include "base/vlog.h"
#include "kafka/server/logger.h"
#include "ssx/sformat.h"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <algorithm>
#include <utility>

namespace kafka {

namespace {

constexpr std::string_view group_state_name_848_empty = "Empty";
constexpr std::string_view group_state_name_848_reconciling = "Reconciling";
constexpr std::string_view group_state_name_848_stable = "Stable";

kafka::member_id generate_consumer_member_id() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return kafka::member_id(ssx::sformat("{}", to_string(uuid)));
}

bool valid_assignor(std::string_view name) {
    return name == consumer_group::uniform_assignor
           || name == consumer_group::range_assignor;
}

consumer_group::assignment_type owned_partitions_from_request(
  const chunked_vector<consumer_group_heartbeat_request_topic_partitions>&
    topic_partitions) {
    consumer_group::assignment_type owned;
    for (const auto& tp : topic_partitions) {
        auto& parts = owned[tp.topic_id];
        for (auto p : tp.partitions) {
            parts.insert(p);
        }
    }
    return owned;
}

bool assignment_contains(
  const consumer_group::assignment_type& assignment,
  const model::topic_id& id,
  model::partition_id p) {
    auto it = assignment.find(id);
    return it != assignment.end() && it->second.contains(p);
}

} // namespace

consumer_group::consumer_group(kafka::group_id id)
  : _id(std::move(id)) {}

consumer_group_heartbeat_response
consumer_group::make_error_response(error_code error, ss::sstring msg) {
    return {error, std::move(msg)};
}

consumer_group_heartbeat_response consumer_group::handle_heartbeat(
  consumer_group_heartbeat_request req,
  const consumer_group_topic_resolver& resolver,
  const consumer_group_settings& settings) {
    if (
      req.data.member_epoch == leave_epoch
      || req.data.member_epoch == static_leave_epoch) {
        return handle_leave(req.data);
    }
    if (req.data.member_epoch < 0) {
        return make_error_response(
          error_code::invalid_request,
          ssx::sformat("Invalid member epoch {}", req.data.member_epoch));
    }
    if (req.data.subscribed_topic_regex.has_value()) {
        return make_error_response(
          error_code::invalid_regular_expression,
          "Regular expression subscriptions are not supported");
    }
    if (
      req.data.server_assignor.has_value()
      && !valid_assignor(*req.data.server_assignor)) {
        return make_error_response(
          error_code::unsupported_assignor,
          ssx::sformat(
            "Server assignor {} is not supported. Supported assignors: "
            "uniform, range",
            *req.data.server_assignor));
    }
    if (req.data.member_epoch == join_epoch) {
        return handle_join(std::move(req), resolver, settings);
    }
    return handle_existing_member(std::move(req), resolver, settings);
}

consumer_group_heartbeat_response consumer_group::handle_leave(
  const consumer_group_heartbeat_request_data& data) {
    auto it = _members.find(data.member_id);
    if (it == _members.end()) {
        return make_error_response(
          error_code::unknown_member_id,
          ssx::sformat(
            "Member {} is not a member of group {}", data.member_id, _id));
    }

    if (data.member_epoch == static_leave_epoch) {
        auto& m = *it->second;
        if (!data.instance_id.has_value()) {
            return make_error_response(
              error_code::invalid_request,
              "InstanceId must be provided when leaving with a -2 member "
              "epoch");
        }
        if (m.instance_id != data.instance_id) {
            return make_error_response(
              error_code::fenced_instance_id,
              ssx::sformat(
                "Member {} does not own instance id {}",
                data.member_id,
                *data.instance_id));
        }

        vlog(
          cg_klog.debug,
          "[group: {}] static member {} (instance id {}) left temporarily",
          _id,
          data.member_id,
          *data.instance_id);

        // keep the member so that its assignment and instance id remain
        // reserved for the rejoining incarnation until the session expires.
        // Both epochs are set to -2 so that heartbeats from the departed
        // incarnation are fenced.
        m.member_epoch = static_leave_epoch;
        m.previous_member_epoch = static_leave_epoch;

        consumer_group_heartbeat_response resp;
        resp.data.member_id = data.member_id;
        resp.data.member_epoch = static_leave_epoch;
        return resp;
    }

    vlog(
      cg_klog.debug,
      "[group: {}] member {} left (epoch {})",
      _id,
      data.member_id,
      data.member_epoch);

    remove_member(data.member_id);

    consumer_group_heartbeat_response resp;
    resp.data.member_id = data.member_id;
    resp.data.member_epoch = data.member_epoch;
    return resp;
}

consumer_group_heartbeat_response consumer_group::handle_join(
  consumer_group_heartbeat_request req,
  const consumer_group_topic_resolver& resolver,
  const consumer_group_settings& settings) {
    auto member_id = req.data.member_id;
    if (member_id().empty()) {
        // version 0 clients may rely on the coordinator to generate the
        // member id on the initial heartbeat.
        member_id = generate_consumer_member_id();
    }

    if (req.data.instance_id.has_value()) {
        auto conflict = std::ranges::find_if(_members, [&](const auto& e) {
            return e.second->instance_id == req.data.instance_id
                   && e.first != member_id;
        });
        if (conflict != _members.end()) {
            if (conflict->second->member_epoch != static_leave_epoch) {
                return make_error_response(
                  error_code::unreleased_instance_id,
                  ssx::sformat(
                    "Static member with instance id {} is still active",
                    *req.data.instance_id));
            }
            // the previous incarnation of this static member left with a -2
            // epoch: the new incarnation takes over the member entry, and
            // with it the reserved (target) assignment, under its new
            // member id.
            vlog(
              cg_klog.debug,
              "[group: {}] static member {} (instance id {}) takes over "
              "from departed member {}",
              _id,
              member_id,
              *req.data.instance_id,
              conflict->first);
            auto replaced_id = conflict->first;
            auto node = _members.extract(conflict);
            node.key() = member_id;
            node.mapped()->id = member_id;
            node.mapped()->session_timer.cancel();
            node.mapped()->session_timer.set_callback(
              [this, member_id] { handle_session_expired(member_id); });
            _members.insert(std::move(node));
            if (auto target = _target.extract(replaced_id); !target.empty()) {
                target.key() = member_id;
                _target.insert(std::move(target));
            }
        }
    }

    auto it = _members.find(member_id);
    if (it == _members.end()) {
        if (!req.data.subscribed_topic_names.has_value()) {
            return make_error_response(
              error_code::invalid_request,
              "SubscribedTopicNames must be provided when joining");
        }
        auto m = std::make_unique<member>();
        m->id = member_id;
        m->session_timer.set_callback(
          [this, member_id] { handle_session_expired(member_id); });
        it = _members.emplace(member_id, std::move(m)).first;
        vlog(cg_klog.debug, "[group: {}] member {} joined", _id, member_id);
    } else {
        vlog(cg_klog.debug, "[group: {}] member {} rejoined", _id, member_id);
    }

    auto& m = *it->second;
    if (req.data.instance_id.has_value()) {
        m.instance_id = req.data.instance_id;
    }
    if (req.data.rack_id.has_value()) {
        m.rack_id = req.data.rack_id;
    }
    if (req.data.rebalance_timeout_ms.count() != -1) {
        m.rebalance_timeout = req.data.rebalance_timeout_ms;
    }
    if (req.data.subscribed_topic_names.has_value()) {
        m.subscribed_topics.assign(
          req.data.subscribed_topic_names->begin(),
          req.data.subscribed_topic_names->end());
    }
    m.client_id = req.client_id;
    m.client_host = req.client_host;
    // a joining member owns nothing: it either never had an assignment or
    // has revoked its partitions before rejoining with epoch 0.
    m.assigned.clear();
    m.revoking.clear();
    m.member_epoch = 0;
    m.previous_member_epoch = 0;
    m.state = reconcile_state::stable;

    if (req.data.server_assignor.has_value()) {
        _assignor = *req.data.server_assignor;
    }

    (void)refresh_subscription_metadata(resolver);
    on_group_updated();
    reconcile_member(m, std::nullopt);
    schedule_session_expiration(m, settings.session_timeout);

    return make_response(m, settings, true);
}

consumer_group_heartbeat_response consumer_group::handle_existing_member(
  consumer_group_heartbeat_request req,
  const consumer_group_topic_resolver& resolver,
  const consumer_group_settings& settings) {
    auto it = _members.find(req.data.member_id);
    if (it == _members.end()) {
        return make_error_response(
          error_code::unknown_member_id,
          ssx::sformat(
            "Member {} is not a member of group {}", req.data.member_id, _id));
    }
    auto& m = *it->second;

    if (
      req.data.member_epoch != m.member_epoch
      && req.data.member_epoch != m.previous_member_epoch) {
        return make_error_response(
          error_code::fenced_member_epoch,
          ssx::sformat(
            "Member epoch {} does not match the current member epoch {}",
            req.data.member_epoch,
            m.member_epoch));
    }

    bool group_updated = false;
    if (req.data.instance_id.has_value()) {
        m.instance_id = req.data.instance_id;
    }
    if (req.data.rack_id.has_value()) {
        m.rack_id = req.data.rack_id;
    }
    if (req.data.rebalance_timeout_ms.count() != -1) {
        m.rebalance_timeout = req.data.rebalance_timeout_ms;
    }
    if (req.data.subscribed_topic_names.has_value()) {
        std::vector<model::topic> subscribed(
          req.data.subscribed_topic_names->begin(),
          req.data.subscribed_topic_names->end());
        if (subscribed != m.subscribed_topics) {
            m.subscribed_topics = std::move(subscribed);
            group_updated = true;
        }
    }
    if (
      req.data.server_assignor.has_value()
      && *req.data.server_assignor != _assignor) {
        _assignor = *req.data.server_assignor;
        group_updated = true;
    }

    std::optional<assignment_type> owned;
    if (req.data.topic_partitions.has_value()) {
        owned = owned_partitions_from_request(*req.data.topic_partitions);
    }

    if (refresh_subscription_metadata(resolver) || group_updated) {
        on_group_updated();
    }

    const auto prior_epoch = m.member_epoch;
    const auto prior_state = m.state;
    reconcile_member(m, owned);
    schedule_session_expiration(m, settings.session_timeout);

    const bool include_assignment = m.member_epoch != prior_epoch
                                    || m.state != reconcile_state::stable
                                    || prior_state != reconcile_state::stable;
    return make_response(m, settings, include_assignment);
}

bool consumer_group::refresh_subscription_metadata(
  const consumer_group_topic_resolver& resolver) {
    absl::btree_map<model::topic, consumer_group_topic_metadata> refreshed;
    for (const auto& [id, m] : _members) {
        for (const auto& topic : m->subscribed_topics) {
            if (refreshed.contains(topic)) {
                continue;
            }
            if (auto md = resolver(topic); md.has_value()) {
                refreshed.emplace(topic, *md);
            }
        }
    }
    if (refreshed == _subscription_metadata) {
        return false;
    }
    _subscription_metadata = std::move(refreshed);
    return true;
}

void consumer_group::on_group_updated() {
    ++_group_epoch;
    compute_target_assignment();
    vlog(
      cg_klog.debug,
      "[group: {}] bumped group epoch to {} (members: {}, assignor: {})",
      _id,
      _group_epoch,
      _members.size(),
      _assignor);
}

void consumer_group::compute_target_assignment() {
    if (_assignor == range_assignor) {
        compute_range_assignment();
    } else {
        compute_uniform_assignment();
    }
    _assignment_epoch = _group_epoch;
}

void consumer_group::compute_range_assignment() {
    absl::node_hash_map<kafka::member_id, assignment_type> target;
    for (const auto& [id, _] : _members) {
        target.emplace(id, assignment_type{});
    }

    for (const auto& [topic, md] : _subscription_metadata) {
        std::vector<kafka::member_id> subscribers;
        for (const auto& [id, m] : _members) {
            if (
              std::ranges::find(m->subscribed_topics, topic)
              != m->subscribed_topics.end()) {
                subscribers.push_back(id);
            }
        }
        if (subscribers.empty()) {
            continue;
        }
        std::ranges::sort(subscribers);

        const auto num_partitions = md.partition_count;
        const auto num_subscribers = static_cast<int32_t>(subscribers.size());
        const auto per_member = num_partitions / num_subscribers;
        const auto extra = num_partitions % num_subscribers;

        int32_t next = 0;
        for (int32_t i = 0; i < num_subscribers; ++i) {
            auto count = per_member + (i < extra ? 1 : 0);
            auto& parts = target[subscribers[i]][md.id];
            for (int32_t p = 0; p < count; ++p) {
                parts.insert(model::partition_id(next++));
            }
        }
    }

    _target = std::move(target);
}

void consumer_group::compute_uniform_assignment() {
    absl::node_hash_map<kafka::member_id, assignment_type> target;
    absl::node_hash_map<kafka::member_id, size_t> load;
    for (const auto& [id, _] : _members) {
        target.emplace(id, assignment_type{});
        load.emplace(id, 0);
    }

    std::vector<kafka::member_id> member_ids;
    member_ids.reserve(_members.size());
    for (const auto& [id, _] : _members) {
        member_ids.push_back(id);
    }
    if (member_ids.empty()) {
        _target = std::move(target);
        return;
    }
    std::ranges::sort(member_ids);

    // reverse index of the previous target assignment for stickiness.
    absl::btree_map<
      std::pair<model::topic_id, model::partition_id>,
      kafka::member_id>
      previous_owner;
    for (const auto& [id, assignment] : _target) {
        for (const auto& [tid, parts] : assignment) {
            for (auto p : parts) {
                previous_owner.emplace(std::make_pair(tid, p), id);
            }
        }
    }

    // collect every assignable partition alongside the members eligible to
    // own it (the subscribers of its topic).
    struct assignable {
        model::topic_id tid;
        model::partition_id partition;
        std::vector<kafka::member_id> subscribers;
    };
    std::vector<assignable> partitions;
    for (const auto& [topic, md] : _subscription_metadata) {
        std::vector<kafka::member_id> subscribers;
        for (const auto& id : member_ids) {
            const auto& m = _members.at(id);
            if (
              std::ranges::find(m->subscribed_topics, topic)
              != m->subscribed_topics.end()) {
                subscribers.push_back(id);
            }
        }
        if (subscribers.empty()) {
            continue;
        }
        for (int32_t p = 0; p < md.partition_count; ++p) {
            partitions.push_back({md.id, model::partition_id(p), subscribers});
        }
    }

    // fair-share quota: spread the assignable partitions as evenly as
    // possible, giving the first `remainder` members (in id order) one extra.
    absl::node_hash_map<kafka::member_id, size_t> quota;
    const size_t base = partitions.size() / member_ids.size();
    size_t remainder = partitions.size() % member_ids.size();
    for (const auto& id : member_ids) {
        quota.emplace(id, base + (remainder > 0 ? 1 : 0));
        if (remainder > 0) {
            --remainder;
        }
    }

    // first pass: keep a partition with its previous owner only while that
    // owner is still eligible and below its quota. Sticky partitions beyond
    // the quota are released for redistribution so the load stays balanced.
    std::vector<const assignable*> unassigned;
    for (const auto& a : partitions) {
        auto prev = previous_owner.find(std::make_pair(a.tid, a.partition));
        if (
          prev != previous_owner.end()
          && std::ranges::find(a.subscribers, prev->second)
               != a.subscribers.end()
          && load[prev->second] < quota[prev->second]) {
            target[prev->second][a.tid].insert(a.partition);
            ++load[prev->second];
        } else {
            unassigned.push_back(&a);
        }
    }

    // second pass: hand each remaining partition to the least loaded eligible
    // subscriber still below its quota, falling back to the least loaded
    // subscriber when every eligible member has already reached its quota.
    for (const auto* a : unassigned) {
        kafka::member_id best;
        bool found = false;
        for (const auto& id : a->subscribers) {
            if (load[id] >= quota[id]) {
                continue;
            }
            if (!found || load[id] < load[best]) {
                best = id;
                found = true;
            }
        }
        if (!found) {
            best = a->subscribers.front();
            for (const auto& id : a->subscribers) {
                if (load[id] < load[best]) {
                    best = id;
                }
            }
        }
        target[best][a->tid].insert(a->partition);
        ++load[best];
    }

    _target = std::move(target);
}

void consumer_group::reconcile_member(
  member& m, const std::optional<assignment_type>& owned_partitions) {
    if (m.state == reconcile_state::unrevoked_partitions) {
        bool acknowledged = false;
        if (owned_partitions.has_value()) {
            acknowledged = true;
            for (const auto& [tid, parts] : m.revoking) {
                for (auto p : parts) {
                    if (assignment_contains(*owned_partitions, tid, p)) {
                        acknowledged = false;
                        break;
                    }
                }
                if (!acknowledged) {
                    break;
                }
            }
        }
        if (!acknowledged) {
            // wait for the member to report that it no longer owns the
            // partitions pending revocation.
            return;
        }
        m.revoking.clear();
    }

    static const assignment_type no_target;
    auto target_it = _target.find(m.id);
    const auto& target = target_it == _target.end() ? no_target
                                                    : target_it->second;

    // ask the member to revoke partitions it may currently own that are no
    // longer part of its target assignment. The member remains at its
    // current epoch until the revocation is acknowledged.
    assignment_type to_revoke;
    for (const auto& [tid, parts] : m.assigned) {
        for (auto p : parts) {
            if (!assignment_contains(target, tid, p)) {
                to_revoke[tid].insert(p);
            }
        }
    }
    if (!to_revoke.empty()) {
        for (const auto& [tid, parts] : to_revoke) {
            auto it = m.assigned.find(tid);
            for (auto p : parts) {
                it->second.erase(p);
            }
            if (it->second.empty()) {
                m.assigned.erase(it);
            }
        }
        m.revoking = std::move(to_revoke);
        m.state = reconcile_state::unrevoked_partitions;
        return;
    }

    // advance to the target epoch and grant the partitions of the target
    // assignment that are not still owned by other members.
    const auto others = partitions_owned_by_others(m.id);
    assignment_type granted;
    bool complete = true;
    for (const auto& [tid, parts] : target) {
        for (auto p : parts) {
            if (assignment_contains(others, tid, p)) {
                complete = false;
            } else {
                granted[tid].insert(p);
            }
        }
    }
    m.assigned = std::move(granted);
    m.previous_member_epoch = m.member_epoch;
    m.member_epoch = _assignment_epoch;
    m.state = complete ? reconcile_state::stable
                       : reconcile_state::unreleased_partitions;
}

consumer_group::assignment_type consumer_group::partitions_owned_by_others(
  const kafka::member_id& except) const {
    assignment_type owned;
    for (const auto& [id, m] : _members) {
        if (id == except) {
            continue;
        }
        for (const auto* assignment : {&m->assigned, &m->revoking}) {
            for (const auto& [tid, parts] : *assignment) {
                owned[tid].insert(parts.begin(), parts.end());
            }
        }
    }
    return owned;
}

void consumer_group::schedule_session_expiration(
  member& m, std::chrono::milliseconds session_timeout) {
    m.session_timer.cancel();
    m.session_timer.arm(session_timeout);
}

void consumer_group::handle_session_expired(kafka::member_id id) {
    vlog(
      cg_klog.info,
      "[group: {}] member {} session timed out, removing member",
      _id,
      id);
    remove_member(id);
}

void consumer_group::remove_member(const kafka::member_id& id) {
    _members.erase(id);
    _target.erase(id);
    on_group_updated();
}

consumer_group_heartbeat_response consumer_group::make_response(
  const member& m,
  const consumer_group_settings& settings,
  bool include_assignment) const {
    consumer_group_heartbeat_response resp;
    resp.data.member_id = m.id;
    resp.data.member_epoch = m.member_epoch;
    resp.data.heartbeat_interval_ms = settings.heartbeat_interval;
    if (include_assignment) {
        consumer_group_heartbeat_response_assignment assignment;
        for (const auto& [tid, parts] : m.assigned) {
            consumer_group_heartbeat_response_topic_partitions tp;
            tp.topic_id = tid;
            tp.partitions.assign(parts.begin(), parts.end());
            assignment.topic_partitions.push_back(std::move(tp));
        }
        resp.data.assignment = std::move(assignment);
    }
    return resp;
}

ss::sstring consumer_group::state_name() const {
    if (_members.empty()) {
        return ss::sstring(group_state_name_848_empty);
    }
    for (const auto& [_, m] : _members) {
        if (
          m->state != reconcile_state::stable
          || m->member_epoch != _assignment_epoch) {
            return ss::sstring(group_state_name_848_reconciling);
        }
    }
    return ss::sstring(group_state_name_848_stable);
}

std::optional<model::topic>
consumer_group::topic_name(const model::topic_id& id) const {
    for (const auto& [topic, md] : _subscription_metadata) {
        if (md.id == id) {
            return topic;
        }
    }
    return std::nullopt;
}

consumer_group_described_group consumer_group::describe() const {
    consumer_group_described_group group;
    group.group_id = _id;
    group.group_state = state_name();
    group.group_epoch = _group_epoch;
    group.assignment_epoch = _assignment_epoch;
    group.assignor_name = _assignor;

    for (const auto& [id, m] : _members) {
        consumer_group_describe_member member;
        member.member_id = id;
        member.instance_id = m->instance_id;
        member.rack_id = m->rack_id;
        member.member_epoch = m->member_epoch;
        member.client_id = m->client_id.value_or(kafka::client_id(""))();
        member.client_host = m->client_host();
        member.subscribed_topic_names.reserve(m->subscribed_topics.size());
        for (const auto& topic : m->subscribed_topics) {
            member.subscribed_topic_names.push_back(topic);
        }

        for (const auto& [tid, parts] : m->assigned) {
            consumer_group_describe_assigned_topic_partitions tp;
            tp.topic_id = tid;
            tp.topic_name = topic_name(tid).value_or(model::topic{});
            tp.partitions.assign(parts.begin(), parts.end());
            member.assignment.topic_partitions.push_back(std::move(tp));
        }

        auto target_it = _target.find(id);
        if (target_it != _target.end()) {
            for (const auto& [tid, parts] : target_it->second) {
                consumer_group_describe_target_topic_partitions tp;
                tp.topic_id = tid;
                tp.topic_name = topic_name(tid).value_or(model::topic{});
                tp.partitions.assign(parts.begin(), parts.end());
                member.target_assignment.topic_partitions.push_back(
                  std::move(tp));
            }
        }

        group.members.push_back(std::move(member));
    }
    return group;
}

error_code consumer_group::validate_offset_commit(
  const kafka::member_id& id, int32_t member_epoch) const {
    auto it = _members.find(id);
    if (it == _members.end()) {
        return error_code::unknown_member_id;
    }
    if (member_epoch != it->second->member_epoch) {
        return error_code::stale_member_epoch;
    }
    return error_code::none;
}

} // namespace kafka
