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

#include "kafka/server/uniform_assignor.h"

#include "base/vassert.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"

#include <algorithm>
#include <bit>
#include <functional>
#include <limits>
#include <optional>
#include <ranges>
#include <utility>

namespace kafka {

namespace {

/// One topic's partition count from the metadata. Fails on a topic the
/// metadata does not recognize, and on a negative count.
std::expected<int32_t, assignor_error>
partition_count(const topic_describer& metadata, model::topic_id topic) {
    auto num_partitions = metadata.num_partitions(topic);
    if (!num_partitions.has_value()) {
        return std::unexpected(
          assignor_error{.errc = assignor_errc::unknown_topic, .topic = topic});
    }
    if (*num_partitions < 0) {
        return std::unexpected(
          assignor_error{
            .errc = assignor_errc::invalid_partition_count, .topic = topic});
    }
    return *num_partitions;
}

/// Checks one topic's assigned partitions, which must not be empty:
///
/// - they are sorted, with no duplicate partition ids
/// - the lowest is at least zero
/// - the highest is below `num_partitions`
std::expected<void, assignor_error> check_assigned(
  model::topic_id topic,
  const partition_set& assigned,
  int32_t num_partitions) {
    if (
      auto repeat = std::ranges::adjacent_find(
        assigned, std::ranges::greater_equal{});
      repeat != assigned.end()) {
        return std::unexpected(
          assignor_error{
            .errc = assignor_errc::malformed_assignment,
            .topic = topic,
            .partition = *std::ranges::next(repeat)});
    }
    if (assigned.front()() < 0) {
        return std::unexpected(
          assignor_error{
            .errc = assignor_errc::malformed_assignment,
            .topic = topic,
            .partition = assigned.front()});
    }
    if (assigned.back()() >= num_partitions) {
        return std::unexpected(
          assignor_error{
            .errc = assignor_errc::unknown_partition,
            .topic = topic,
            .partition = assigned.back()});
    }
    return {};
}

/// A forward-only position in a sequence ordered by increasing topic id, where
/// `proj` gets the topic of an element. The position never moves backwards, so
/// one pass over the sequence serves any number of lookups when they arrive in
/// increasing topic order, and a lookup below the position misses.
template<typename Range, typename Proj = std::identity>
class topic_cursor {
public:
    using iterator = std::ranges::iterator_t<Range>;

    /// The sequence must outlive the cursor, so we only accept an lvalue
    /// reference here.
    explicit topic_cursor(Range& sequence, Proj proj = {})
      : _next(std::ranges::begin(sequence))
      , _end(std::ranges::end(sequence))
      , _proj(std::move(proj)) {}

    /// Advances to `topic` if present in the tail of the sequence:
    ///
    /// - an iterator to its element when the sequence holds `topic`
    /// - `std::nullopt` on a miss, where a later lookup can still hit
    /// - `end()` once the sequence is exhausted
    std::optional<iterator> advance_to(model::topic_id topic) {
        while (_next != _end && std::invoke(_proj, *_next) < topic) {
            ++_next;
        }
        if (_next == _end) {
            return _end;
        }
        if (std::invoke(_proj, *_next) != topic) {
            // A miss leaves the cursor as it was: `_next` already sits on the
            // first element past `topic`, ready for the next, higher lookup.
            return std::nullopt;
        }
        return _next++;
    }

    iterator end() const { return _end; }

private:
    iterator _next;
    iterator _end;
    Proj _proj;
};

/// Homogeneous assignment means every member subscribes to the same topics,
/// so any member can take any partition. Three phases:
///
/// - count the partitions of every subscribed topic, and collect the ones that
///   no member holds
/// - every member keeps the partitions it already holds, lowest first, up to
///   its target size, and releases the rest
/// - the collected partitions go to the members below their target size
///
/// A member's target size is the total number of subscribed partitions divided
/// by the member count, and the remainder goes to that many members, one extra
/// partition each. So a member above its target size loses partitions it
/// already holds: an even split takes priority over stickiness.
///
/// This class uses the same algorithm as Kafka's
/// UniformHomogeneousAssignmentBuilder:
/// https://github.com/apache/kafka/blob/fce22525f74bbd7feeb4f8b34c79a215db9a74c3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/UniformHomogeneousAssignmentBuilder.java#L50
class homogeneous_assignment {
public:
    /// The group must have at least one member, since they all share its
    /// subscription.
    homogeneous_assignment(
      const group_spec& group, const topic_describer& metadata)
      : _group(group)
      , _metadata(metadata)
      , _target(group.members().size()) {
        vassert(
          !group.members().empty(),
          "tried to produce an assignment for a group with no members");
    }

    assignment_result build() && {
        if (subscription().empty()) {
            return std::move(_target);
        }
        auto counted = index_subscriptions();
        if (!counted.has_value()) {
            return std::unexpected(counted.error());
        }
        auto target_sizes = keep_within_target(*counted);
        if (!target_sizes.has_value()) {
            return std::unexpected(target_sizes.error());
        }
        auto dealt = deal_what_is_left(*target_sizes, counted->unassigned);
        if (!dealt.has_value()) {
            return std::unexpected(dealt.error());
        }
        _target.sort();
        return std::move(_target);
    }

private:
    /// The subscription, counted out for the phases that follow.
    struct subscription_index {
        /// How many partitions each subscribed topic has.
        chunked_hash_map<model::topic_id, int32_t> partition_counts;
        /// Across every subscribed topic.
        size_t total_partitions{0};
        /// The partitions no member holds, plus the ones members released to
        /// stay within a target size, grouped by topic as they are collected.
        /// A topic can appear more than once: once for the partitions nobody
        /// held, and once per member that released some. The last phase hands
        /// them out in this order.
        chunked_vector<topic_partitions> unassigned;
    };

    /// A position in one member's current assignment.
    using assignment_cursor = topic_cursor<
      const member_assignment,
      decltype(&topic_partitions::topic)>;

    /// What every member of the group subscribes to.
    const subscribed_topic_ids& subscription() const {
        return _group.members().front().subscribed_topics;
    }

    /// A dynamically sized bitset for partition_ids. One bit per id.
    class partition_bitmap {
    public:
        /// Empties the set and makes room for `capacity` ids.
        void reset(size_t capacity) {
            _capacity = capacity;
            _count = 0;
            auto in_use = words_for(capacity);
            while (_words.size() < in_use) {
                _words.push_back(0);
            }
            std::fill_n(_words.begin(), in_use, 0);
        }

        /// Ignores an id outside the capacity.
        void insert(model::partition_id partition) {
            if (!in_range(partition)) {
                return;
            }
            auto& word = _words[word_of(partition)];
            auto before = word;
            word |= bit_of(partition);
            if (word != before) {
                ++_count;
            }
        }

        bool contains(model::partition_id partition) const {
            return in_range(partition)
                   && (_words[word_of(partition)] & bit_of(partition)) != 0;
        }

        /// The ids inside the capacity that the set does not hold, in order
        /// of increasing partition id. Walks words rather than ids, so a full
        /// set costs one compare and a sparse one is linear in the words plus
        /// the ids it returns.
        partition_set missing() const {
            partition_set out;
            if (_count == _capacity) {
                return out;
            }
            out.reserve(_capacity - _count);
            for (size_t word = 0; word < words_for(_capacity); ++word) {
                auto absent = ~_words[word];
                while (absent != 0) {
                    auto id = word * word_bits
                              + static_cast<size_t>(std::countr_zero(absent));
                    if (id >= _capacity) {
                        break;
                    }
                    out.emplace_back(static_cast<int32_t>(id));
                    absent &= absent - 1;
                }
            }
            return out;
        }

    private:
        static constexpr size_t word_bits
          = std::numeric_limits<uint64_t>::digits;

        bool in_range(model::partition_id partition) const {
            return partition() >= 0
                   && static_cast<size_t>(partition()) < _capacity;
        }

        static size_t words_for(size_t capacity) {
            return (capacity + word_bits - 1) / word_bits;
        }

        static size_t word_of(model::partition_id partition) {
            return static_cast<uint64_t>(partition()) / word_bits;
        }

        static uint64_t bit_of(model::partition_id partition) {
            return uint64_t{1}
                   << (static_cast<uint64_t>(partition()) % word_bits);
        }

        chunked_vector<uint64_t> _words;
        size_t _capacity{0};
        /// How many ids the set holds.
        size_t _count{0};
    };

    /// Counts each subscribed topic's partitions and collects the ones that no
    /// member holds. Fails if any subscribed topic is unrecognized by the
    /// assignor shard.
    std::expected<subscription_index, assignor_error> index_subscriptions() {
        if (
          auto repeat = std::ranges::adjacent_find(
            subscription(), std::ranges::greater_equal{});
          repeat != subscription().end()) {
            return std::unexpected(
              assignor_error{
                .errc = assignor_errc::malformed_subscription,
                .topic = *std::ranges::next(repeat)});
        }

        subscription_index result;
        result.partition_counts.reserve(subscription().size());

        // One position per member, moving forward with the subscription.
        chunked_vector<assignment_cursor> cursors;
        cursors.reserve(_group.members().size());
        for (const auto& member : _group.members()) {
            cursors.emplace_back(
              member.current_assignment, &topic_partitions::topic);
        }
        // The partitions of the topic in hand that some member already holds.
        partition_bitmap claimed;

        // For each topic in the subscription, determine which partitions do
        // not appear in any member's current target assignment.
        for (const auto& topic : subscription()) {
            auto num_partitions = partition_count(_metadata, topic);
            if (!num_partitions.has_value()) {
                return std::unexpected(num_partitions.error());
            }
            result.partition_counts.emplace(topic, *num_partitions);
            result.total_partitions += static_cast<size_t>(*num_partitions);

            claimed.reset(static_cast<size_t>(*num_partitions));

            for (auto& member : cursors) {
                auto assigned = member.advance_to(topic);
                if (!assigned.has_value() || *assigned == member.end()) {
                    continue;
                }
                for (auto partition : (*assigned)->partitions) {
                    claimed.insert(partition);
                }
            }

            auto unowned = claimed.missing();
            if (!unowned.empty()) {
                result.unassigned.push_back(
                  {.topic = topic, .partitions = std::move(unowned)});
            }
        }

        return result;
    }

    /// How much of one member's target size is still unspent, deducted as the
    /// member keeps partitions topic by topic.
    class keep_budget {
    public:
        explicit keep_budget(size_t target_size)
          : _target_size(target_size)
          , _room(target_size) {}

        struct split {
            size_t to_keep;
            size_t to_release;
        };

        /// Splits `held` partitions into the ones the budget still covers,
        /// which it deducts, and the rest.
        split fit(size_t held) {
            auto to_keep = std::min(_room, held);
            _room -= to_keep;
            return {.to_keep = to_keep, .to_release = held - to_keep};
        }

        /// How many partitions the budget has covered so far.
        size_t kept() const { return _target_size - _room; }

        /// Whether the whole target size was spent.
        bool filled() const { return _room == 0; }

    private:
        size_t _target_size;
        size_t _room;
    };

    /// Every member keeps the partitions it already holds, up to its target
    /// size, and releases the rest. Fails on an assignment that is out of
    /// order or that the topic metadata cannot account for. Returns each
    /// member's target size, by member index.
    std::expected<chunked_vector<size_t>, assignor_error>
    keep_within_target(subscription_index& counted) {
        const auto num_members = _group.members().size();
        const auto min_size = counted.total_partitions / num_members;
        // How many members still get one partition more than the minimum.
        auto extras_left = counted.total_partitions % num_members;

        chunked_vector<size_t> target_sizes;
        target_sizes.reserve(num_members);

        for (auto index : std::views::iota(size_t{0}, num_members)) {
            const auto& member = _group.members()[index];
            if (
              auto repeat = std::ranges::adjacent_find(
                member.current_assignment,
                std::ranges::greater_equal{},
                &topic_partitions::topic);
              repeat != member.current_assignment.end()) {
                return std::unexpected(
                  assignor_error{
                    .errc = assignor_errc::malformed_assignment,
                    .topic = std::ranges::next(repeat)->topic});
            }

            auto target_size = min_size;
            bool takes_extra = extras_left > 0;
            if (takes_extra) {
                ++target_size;
            }

            auto& assignment = _target[index];
            keep_budget budget{target_size};
            for (const auto& [topic, curr_assigned] :
                 member.current_assignment) {
                auto counted_topic = counted.partition_counts.find(topic);
                if (counted_topic == counted.partition_counts.end()) {
                    // Nobody in the group subscribes to this topic any more,
                    // so we don't carry its partitions forward into the new
                    // assignment.
                    continue;
                }
                if (curr_assigned.empty()) {
                    continue;
                }
                if (
                  auto checked = check_assigned(
                    topic, curr_assigned, counted_topic->second);
                  !checked.has_value()) {
                    return std::unexpected(checked.error());
                }

                // Keep as much as the budget allows, lowest partitions
                // first, and release the rest for a member with room to pick
                // up.
                auto [to_keep, to_release] = budget.fit(curr_assigned.size());
                if (to_release > 0) {
                    counted.unassigned.push_back(
                      {.topic = topic,
                       .partitions = partition_set(
                         std::from_range,
                         curr_assigned | std::views::drop(to_keep))});
                }
                if (to_keep > 0) {
                    assignment.assign(
                      topic,
                      partition_set(
                        std::from_range,
                        curr_assigned | std::views::take(to_keep)));
                }
            }
            vassert(
              budget.kept() == assignment.num_partitions(),
              "{} kept {} partitions but is assigned {}",
              member.id,
              budget.kept(),
              assignment.num_partitions());

            // Any `extras_left` members can take the odd partitions and the
            // split stays even, so the extras are spent on stickiness: a
            // member that did not fill its target would have its extra filled
            // by a moved partition, while a later member may keep an extra
            // partition it already holds. The extra passes on, as long as
            // enough members remain to take every extra still unplaced.
            bool enough_members_left = num_members - index > extras_left;
            if (takes_extra && !budget.filled() && enough_members_left) {
                --target_size;
                takes_extra = false;
            }
            if (takes_extra) {
                --extras_left;
            }
            target_sizes.push_back(target_size);
        }
        return target_sizes;
    }

    /// Hands the collected partitions to the members below their target size,
    /// in collection order.
    std::expected<void, assignor_error> deal_what_is_left(
      const chunked_vector<size_t>& target_sizes,
      const chunked_vector<topic_partitions>& unassigned) {
        size_t member = 0;
        size_t room = 0;
        // Finds the next member below its target size and consumes one slot
        // of its room, or reports that no member has room left.
        auto take_room = [&] {
            while (room == 0 && member < target_sizes.size()) {
                auto assigned = _target[member].num_partitions();
                vassert(
                  assigned <= target_sizes[member],
                  "member {} is assigned {} partitions of a target size of {}",
                  member,
                  assigned,
                  target_sizes[member]);
                room = target_sizes[member] - assigned;
                if (room == 0) {
                    ++member;
                }
            }
            if (room == 0) {
                return false;
            }
            --room;
            return true;
        };

        for (const auto& [topic, partitions] : unassigned) {
            for (auto partition : partitions) {
                if (!take_room()) {
                    // The target sizes add up to the partition count exactly,
                    // so a partition left over means the current assignment
                    // had a partition assigned to two members, or the
                    // arithmetic above is wrong.
                    return std::unexpected(
                      assignor_error{
                        .errc = assignor_errc::partitions_left_unassigned,
                        .topic = topic,
                        .partition = partition});
                }
                _target[member].assign(topic, partition);
            }
        }
        return {};
    }

    const group_spec& _group;
    const topic_describer& _metadata;
    group_assignment _target;
};

} // namespace

assignment_result uniform_assignor::assign(
  const group_spec& group, const topic_describer& topics) const {
    if (group.members().empty()) {
        return group_assignment{};
    }
    switch (group.type()) {
    case subscription_type::homogeneous:
        return homogeneous_assignment{group, topics}.build();
    case subscription_type::heterogeneous:
        vassert(false, "not implemented");
    }
    std::unreachable();
}

} // namespace kafka
