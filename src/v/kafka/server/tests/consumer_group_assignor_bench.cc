// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vassert.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "kafka/server/consumer_group_assignor.h"
#include "kafka/server/uniform_assignor.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

#include <absl/container/btree_map.h>
#include <fmt/format.h>

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace kafka {

namespace {

/// The coordinator-shard CPU cost of computing a target assignment:
///
///   1. the group_spec constructor: subscription type detection
///   2. uniform_assignor::assign() over the built spec
///
/// The assign_* cases measure step 2 alone, over a spec built up front. The
/// spec_ctor_* cases measure step 1 alone, and name a size with no scenario,
/// since the constructor reads only the subscriptions. The spec_and_assign_*
/// cases measure both steps together.
///
/// Case names read <scenario>_<members>m_<topics>t_<partitions per topic>p:
///   - fresh: no member holds an assignment, the group's first rebalance
///   - stable: every member holds the assignment a previous run produced
///   - join: one member with no assignment joins a stable group
///   - leave: one member left a stable group, so its partitions have no
///     owner

/// Topic metadata for the topics the bench registered.
class bench_describer final : public topic_describer {
public:
    void add(model::topic_id topic, int32_t partitions) {
        _partitions[topic] = partitions;
    }

    std::optional<int32_t> num_partitions(model::topic_id topic) const final {
        auto it = _partitions.find(topic);
        if (it == _partitions.end()) {
            return std::nullopt;
        }
        return it->second;
    }

private:
    absl::btree_map<model::topic_id, int32_t> _partitions;
};

/// A topic id whose byte pattern orders by `n`, so the subscription
/// builders can emit topic ids in increasing order.
model::topic_id make_topic_id(uint16_t n) {
    std::vector<uint8_t> bytes(uuid_t::length, 0);
    bytes[uuid_t::length - 2] = static_cast<uint8_t>(n >> 8U);
    bytes[uuid_t::length - 1] = static_cast<uint8_t>(n & 0xFFU);
    return model::topic_id{uuid_t{bytes}};
}

struct shape {
    size_t members;
    uint16_t topics;
    /// Partitions per topic.
    int32_t partitions;
    /// Operations per timed loop. Scaled down for the bigger shapes so a
    /// single call stays well under the run duration.
    size_t inner;
};

constexpr shape small_group{
  .members = 10, .topics = 1, .partitions = 100, .inner = 100};
constexpr shape medium_group{
  .members = 100, .topics = 10, .partitions = 100, .inner = 20};
constexpr shape large_group{
  .members = 1000, .topics = 100, .partitions = 100, .inner = 5};

enum class scenario { fresh, stable, join, leave };

/// The topics member `m` subscribes to, in order of increasing topic id.
using subscription_fn = subscribed_topic_ids (*)(size_t m, const shape&);

/// Every member subscribes to every topic: the homogeneous pattern.
subscribed_topic_ids subscribe_all(size_t, const shape& s) {
    subscribed_topic_ids ids;
    ids.reserve(s.topics);
    for (uint16_t t = 0; t < s.topics; ++t) {
        ids.push_back(make_topic_id(t));
    }
    return ids;
}

member_spec make_member(size_t m, const shape& s, subscription_fn subscribe) {
    return member_spec{
      .id = member_id{ss::sstring{fmt::format("member-{:05}", m)}},
      .subscribed_topics = subscribe(m, s)};
}

chunked_vector<member_spec>
make_members(size_t count, const shape& s, subscription_fn subscribe) {
    chunked_vector<member_spec> members;
    members.reserve(count);
    for (size_t m = 0; m < count; ++m) {
        members.push_back(make_member(m, s, subscribe));
    }
    return members;
}

chunked_vector<member_spec> copy(const chunked_vector<member_spec>& members) {
    chunked_vector<member_spec> out;
    out.reserve(members.size());
    for (const auto& member : members) {
        out.push_back(
          member_spec{
            .id = member.id,
            .subscribed_topics = member.subscribed_topics.copy(),
            .current_assignment = member.current_assignment.copy()});
    }
    return out;
}

struct assignor_bench {
    uniform_assignor impl;
    bench_describer metadata;
    std::optional<group_spec> prebuilt;
    std::optional<chunked_vector<member_spec>> proto;

    void register_topics(const shape& s) {
        for (uint16_t t = 0; t < s.topics; ++t) {
            metadata.add(make_topic_id(t), s.partitions);
        }
    }

    /// Assigns `members` once and hands each one the result back as its
    /// current assignment.
    chunked_vector<member_spec> seeded(chunked_vector<member_spec> members) {
        auto spec = group_spec{copy(members)};
        auto result = impl.assign(spec, metadata);
        vassert(
          result.has_value(),
          "the bench seeds itself with a valid assignment: {}",
          result.error());
        for (size_t m = 0; m < members.size(); ++m) {
            members[m].current_assignment = std::move((*result)[m]);
        }
        return members;
    }

    chunked_vector<member_spec>
    make_input(const shape& s, scenario kind, subscription_fn subscribe) {
        switch (kind) {
        case scenario::fresh:
            return make_members(s.members, s, subscribe);
        case scenario::stable:
            return seeded(make_members(s.members, s, subscribe));
        case scenario::join: {
            auto members = seeded(make_members(s.members - 1, s, subscribe));
            members.push_back(make_member(s.members - 1, s, subscribe));
            return members;
        }
        case scenario::leave: {
            auto members = seeded(make_members(s.members + 1, s, subscribe));
            members.pop_back();
            return members;
        }
        }
        __builtin_unreachable();
    }

    /// assign() alone, over a spec built once up front.
    size_t
    run_assign(const shape& s, scenario kind, subscription_fn subscribe) {
        if (!prebuilt) {
            register_topics(s);
            prebuilt.emplace(make_input(s, kind, subscribe));
        }
        perf_tests::start_measuring_time();
        for (size_t i = 0; i < s.inner; ++i) {
            auto result = impl.assign(*prebuilt, metadata);
            perf_tests::do_not_optimize(result);
        }
        perf_tests::stop_measuring_time();
        return s.inner;
    }

    /// The group_spec constructor alone, which reads only the subscriptions.
    /// The constructed specs outlive the timed loop, so it times the
    /// constructor and not the teardown of the inputs.
    size_t run_spec_ctor(const shape& s, subscription_fn subscribe) {
        if (!proto) {
            register_topics(s);
            proto.emplace(make_members(s.members, s, subscribe));
        }
        std::vector<chunked_vector<member_spec>> inputs;
        inputs.reserve(s.inner);
        for (size_t i = 0; i < s.inner; ++i) {
            inputs.push_back(copy(*proto));
        }
        std::vector<group_spec> specs;
        specs.reserve(s.inner);
        perf_tests::start_measuring_time();
        for (auto& input : inputs) {
            specs.emplace_back(std::move(input));
        }
        perf_tests::stop_measuring_time();
        perf_tests::do_not_optimize(specs);
        return s.inner;
    }

    /// Constructor plus assign(): the whole per-rebalance cost.
    size_t run_spec_and_assign(
      const shape& s, scenario kind, subscription_fn subscribe) {
        if (!proto) {
            register_topics(s);
            proto.emplace(make_input(s, kind, subscribe));
        }
        std::vector<chunked_vector<member_spec>> inputs;
        inputs.reserve(s.inner);
        for (size_t i = 0; i < s.inner; ++i) {
            inputs.push_back(copy(*proto));
        }
        perf_tests::start_measuring_time();
        for (auto& input : inputs) {
            auto spec = group_spec{std::move(input)};
            auto result = impl.assign(spec, metadata);
            perf_tests::do_not_optimize(result);
        }
        perf_tests::stop_measuring_time();
        return s.inner;
    }
};

} // namespace

PERF_TEST_F(assignor_bench, assign_fresh_10m_1t_100p) {
    return run_assign(small_group, scenario::fresh, subscribe_all);
}
PERF_TEST_F(assignor_bench, assign_fresh_100m_10t_100p) {
    return run_assign(medium_group, scenario::fresh, subscribe_all);
}
PERF_TEST_F(assignor_bench, assign_fresh_1000m_100t_100p) {
    return run_assign(large_group, scenario::fresh, subscribe_all);
}

PERF_TEST_F(assignor_bench, assign_stable_10m_1t_100p) {
    return run_assign(small_group, scenario::stable, subscribe_all);
}
PERF_TEST_F(assignor_bench, assign_stable_100m_10t_100p) {
    return run_assign(medium_group, scenario::stable, subscribe_all);
}
PERF_TEST_F(assignor_bench, assign_stable_1000m_100t_100p) {
    return run_assign(large_group, scenario::stable, subscribe_all);
}

PERF_TEST_F(assignor_bench, assign_join_100m_10t_100p) {
    return run_assign(medium_group, scenario::join, subscribe_all);
}
PERF_TEST_F(assignor_bench, assign_join_1000m_100t_100p) {
    return run_assign(large_group, scenario::join, subscribe_all);
}

PERF_TEST_F(assignor_bench, assign_leave_100m_10t_100p) {
    return run_assign(medium_group, scenario::leave, subscribe_all);
}
PERF_TEST_F(assignor_bench, assign_leave_1000m_100t_100p) {
    return run_assign(large_group, scenario::leave, subscribe_all);
}

PERF_TEST_F(assignor_bench, spec_ctor_100m_10t_100p) {
    return run_spec_ctor(medium_group, subscribe_all);
}
PERF_TEST_F(assignor_bench, spec_ctor_1000m_100t_100p) {
    return run_spec_ctor(large_group, subscribe_all);
}

PERF_TEST_F(assignor_bench, spec_and_assign_stable_100m_10t_100p) {
    return run_spec_and_assign(medium_group, scenario::stable, subscribe_all);
}
PERF_TEST_F(assignor_bench, spec_and_assign_stable_1000m_100t_100p) {
    return run_spec_and_assign(large_group, scenario::stable, subscribe_all);
}

} // namespace kafka
