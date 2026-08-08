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
#include <compare>
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

/// Narrows a container size to the int32_t that partition counts use. Safe for
/// every caller here: a topic's partition count is int32 on the wire, and a
/// group's membership is far below that.
int32_t to_count(size_t size) {
    vassert(
      size <= static_cast<size_t>(std::numeric_limits<int32_t>::max()),
      "size does not fit in int32_t: {}",
      size);
    return static_cast<int32_t>(size);
}

/// Nobody owns the partition.
constexpr int32_t no_member = -1;

/// Caps the balancing sweeps over a group's topics. Subscribers that all share
/// one subscription balance in a single sweep, plus a second that finds nothing
/// left to move. Partly overlapping subscriptions can take more, so the loop
/// stops at a bound instead of a fixpoint.
constexpr int32_t max_balance_sweeps = 16;

/// A topic ordered against the group's others by how little room it has for
/// placement: the most partitions per subscriber first. That leaves the topics
/// with more subscribers until last, so they can even out the imbalance the
/// earlier ones create.
struct topic_by_load {
    /// Index into the group's subscribed topics.
    size_t topic;
    int32_t num_partitions;
    size_t num_subscribers;

    /// Partitions per subscriber, decreasing; then subscriber count,
    /// increasing; then the topic's own order, so that the order is total.
    /// Cross-multiplied rather than divided, to keep the comparison exact.
    friend std::strong_ordering
    operator<=>(const topic_by_load& lhs, const topic_by_load& rhs) {
        auto load = static_cast<int64_t>(lhs.num_partitions)
                    * static_cast<int64_t>(rhs.num_subscribers);
        auto other_load = static_cast<int64_t>(rhs.num_partitions)
                          * static_cast<int64_t>(lhs.num_subscribers);
        if (auto order = other_load <=> load; order != 0) {
            return order;
        }
        if (
          auto order = lhs.num_subscribers <=> rhs.num_subscribers;
          order != 0) {
            return order;
        }
        return lhs.topic <=> rhs.topic;
    }
};

/// Hands out the subscribers of one topic by load: the least loaded of them to
/// take a partition, the most loaded to give one up.
///
/// Keeps two ranges over those subscribers, sorted by how many partitions each
/// are assigned across the whole group. Each range covers the members sitting
/// at one load level, the least loaded range growing rightwards from the start
/// and the most loaded range growing leftwards from the end:
///
///          |                                     #
///     load |    [least ->]##         ##[<- most]##
///          |    [########]##         ##[#######]##
///          +--------------------------------------->
///                 subscribers, by increasing load
///
/// Every member inside a range holds the same number of partitions, so a range
/// hands out round robin. Once a range runs out it moves up or down a level and
/// takes in the members already sitting there. The two ranges balance once
/// their levels differ by one or less, and balancing the topic stops there.
///
/// Loads belong to the caller, which moves the partitions and keeps the loads
/// current. A member this class hands out already counts as having taken or
/// given up a partition, so the next call moves on rather than return it again.
///
/// This class is a port of Kafka's MemberAssignmentBalancer:
/// https://github.com/apache/kafka/blob/fce22525f74bbd7feeb4f8b34c79a215db9a74c3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/UniformHeterogeneousAssignmentBuilder.java#L326
class member_balancer {
public:
    explicit member_balancer(const chunked_vector<int32_t>& loads)
      : _loads(loads) {}

    /// Restarts on one topic's subscribers, which must not be empty. Returns
    /// the difference in load between the most and the least loaded of them.
    int32_t restart(const chunked_vector<int32_t>& subscribers) {
        // Overwritten in place rather than cleared: `chunked_vector::clear()`
        // releases the buffer, and this runs once per topic per sweep.
        while (_sorted.size() < subscribers.size()) {
            _sorted.push_back(0);
        }
        _count = to_count(subscribers.size());
        std::ranges::copy(subscribers, _sorted.begin());
        std::ranges::sort(
          std::ranges::subrange(_sorted.begin(), _sorted.begin() + _count),
          {},
          [this](int32_t member) { return std::pair{load(member), member}; });

        _least_end = 0;
        _least_level = load(at(0)) - 1;
        _next_least = 0;

        _most_start = count();
        _most_end = count();
        _most_level = load(at(count() - 1)) + 1;
        _next_most = count() - 1;

        return load(at(count() - 1)) - load(at(0));
    }

    /// The least loaded subscriber, which afterwards counts as holding one
    /// more.
    int32_t next_least_loaded() {
        if (_next_least >= _least_end) {
            // The range is spent, so raise its level and take in the members
            // already sitting there.
            ++_least_level;
            while (_least_end < count()
                   && load(at(_least_end)) == _least_level) {
                ++_least_end;
            }
            _next_least = 0;
        }
        auto member = at(_next_least);
        ++_next_least;
        return member;
    }

    /// The most loaded subscriber, which afterwards counts as holding one less,
    /// or `no_member` once all of them have been excluded.
    int32_t next_most_loaded() {
        if (_next_most < _most_start) {
            if (_most_end <= _most_start && _most_start > 0) {
                // Exclusions emptied the range. Put its level on the next
                // member down, so that expanding below takes that member in
                // rather than handing out one from outside the range.
                _most_level = load(at(_most_start - 1));
            } else {
                --_most_level;
            }
            while (_most_start > 0
                   && load(at(_most_start - 1)) == _most_level) {
                --_most_start;
            }
            _next_most = _most_end - 1;
        }
        if (_next_most < 0) {
            return no_member;
        }
        auto member = at(_next_most);
        --_next_most;
        return member;
    }

    /// Drops the member `next_most_loaded` returned last from the most loaded
    /// range, because it has nothing of this topic left to give up.
    void exclude_most_loaded() {
        std::swap(
          _sorted[static_cast<size_t>(_next_most + 1)],
          _sorted[static_cast<size_t>(_most_end - 1)]);
        --_most_end;
    }

    bool is_balanced() const { return _most_level - _least_level <= 1; }

private:
    int32_t count() const { return _count; }

    int32_t at(int32_t position) const {
        return _sorted[static_cast<size_t>(position)];
    }

    int32_t load(int32_t member) const {
        return _loads[static_cast<size_t>(member)];
    }

    const chunked_vector<int32_t>& _loads;
    /// The subscribers this class hands out, in `[0, _count)`. Grown to the
    /// largest subscriber list so far and reused.
    chunked_vector<int32_t> _sorted;
    int32_t _count{0};
    /// [0, _least_end) are the least loaded members.
    int32_t _least_end{0};
    int32_t _least_level{0};
    int32_t _next_least{0};
    /// [_most_start, _most_end) are the most loaded members that still have
    /// something to give up.
    int32_t _most_start{0};
    int32_t _most_end{0};
    int32_t _most_level{0};
    int32_t _next_most{0};
};

/// One of the group's subscribed topics.
struct subscribed_topic {
    model::topic_id topic;
    /// The members that subscribe to it, as indices into the group's members.
    chunked_vector<int32_t> subscribers;
    int32_t num_partitions{0};
    /// The member each partition is assigned to, or `no_member`, by partition
    /// id.
    chunked_vector<int32_t> owners;

    int32_t& owner(int32_t partition) {
        return owners[static_cast<size_t>(partition)];
    }
    int32_t owner(int32_t partition) const {
        return owners[static_cast<size_t>(partition)];
    }
};

/// The partitions of one topic, laid out grouped by owner so that the
/// partitions of any one member are a contiguous range to take from.
///
/// Counted rather than sorted: the owners are dense member indices, so one
/// counting pass is linear in the partitions and produces each member's range
/// as it goes. The buffers are reused from topic to topic, so a rebuild does
/// not reallocate.
class topic_holdings {
public:
    /// Rebuilds the layout for one topic. The topic's owner indices must be
    /// below `num_members`.
    void rebuild(const subscribed_topic& subscribed, size_t num_members) {
        while (_start.size() < num_members) {
            _start.push_back(0);
            _end.push_back(0);
        }
        // Only the counts need clearing; the prefix pass below writes every
        // start.
        std::ranges::fill(_end, 0);

        for (auto owner : subscribed.owners) {
            if (owner == no_member) {
                continue;
            }
            ++_end[static_cast<size_t>(owner)];
        }

        // Turn the counts into ranges, leaving the end of each as a cursor to
        // fill from.
        int32_t next = 0;
        for (size_t member = 0; member < _end.size(); ++member) {
            _start[member] = next;
            next += _end[member];
            _end[member] = _start[member];
        }

        // Placed in increasing partition order, so every member's range comes
        // out sorted too. Grown rather than cleared, since
        // `chunked_vector::clear()` releases the buffer, and the cursors below
        // overwrite every slot they use.
        while (to_count(_grouped.size()) < next) {
            _grouped.push_back(0);
        }
        for (auto id : std::views::iota(0, subscribed.num_partitions)) {
            auto owner = subscribed.owner(id);
            if (owner == no_member) {
                continue;
            }
            auto& cursor = _end[static_cast<size_t>(owner)];
            _grouped[static_cast<size_t>(cursor)] = id;
            ++cursor;
        }
    }

    /// Whether the member still holds a partition of the topic.
    bool has_any(int32_t member) const {
        return _end[static_cast<size_t>(member)]
               > _start[static_cast<size_t>(member)];
    }

    /// Takes the member's highest partition of the topic. The member must hold
    /// one.
    int32_t pop_highest(int32_t member) {
        auto& end = _end[static_cast<size_t>(member)];
        --end;
        return _grouped[static_cast<size_t>(end)];
    }

private:
    /// The topic's partitions, grouped by owner.
    chunked_vector<int32_t> _grouped;
    /// [_start, _end) is a member's range in `_grouped`, by member index.
    chunked_vector<int32_t> _start;
    chunked_vector<int32_t> _end;
};

/// The members subscribe to different topics, so there is no single pool of
/// partitions to split evenly. A partition can only move between the members
/// that subscribe to its topic, while how loaded those members are depends on
/// every topic they subscribe to, so the group is evened out one topic at a
/// time, repeatedly.
///
/// Each member first keeps its current assignment of the topics it still
/// subscribes to. The partitions assigned to nobody go to the least loaded
/// subscriber of their topic. The topics are then swept, each moving partitions
/// from its most loaded subscriber to its least loaded while that improves the
/// balance, and the sweeps repeat until one moves nothing or
/// `max_balance_sweeps` is reached.
///
/// Balance is exact when every subscriber of a topic subscribes to the same
/// topics. Otherwise it can settle short of even, because evening out one topic
/// at a time cannot see a chain of moves across topics: three members and two
/// topics subscribed {1,2} and {2,3} can settle at 9, 10 and 11 partitions with
/// each topic balanced within itself.
///
/// This class is a port of Kafka's UniformHeterogeneousAssignmentBuilder:
/// https://github.com/apache/kafka/blob/fce22525f74bbd7feeb4f8b34c79a215db9a74c3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/UniformHeterogeneousAssignmentBuilder.java#L53
class heterogeneous_assignment {
public:
    heterogeneous_assignment(
      const group_spec& group, const topic_describer& metadata)
      : _group(group)
      , _metadata(metadata)
      , _balancer(_loads) {
        for (size_t member = 0; member < group.members().size(); ++member) {
            _loads.push_back(0);
        }
    }

    assignment_result build() && {
        if (auto indexed = index_subscriptions(); !indexed.has_value()) {
            return std::unexpected(indexed.error());
        }
        if (auto kept = keep_what_is_still_subscribed(); !kept.has_value()) {
            return std::unexpected(kept.error());
        }
        // Both phases walk the topics in the same order, so the list is built
        // and sorted once.
        auto by_load = topics_by_load();
        assign_unowned(by_load);
        balance(by_load);
        return materialize();
    }

private:
    /// Collects the group's subscribed topics and who subscribes to each,
    /// failing if one is absent from the metadata or a member's subscription
    /// is out of order.
    std::expected<void, assignor_error> index_subscriptions() {
        chunked_vector<model::topic_id> subscribed_ids;
        for (const auto& member : _group.members()) {
            for (const auto& topic : member.subscribed_topics) {
                subscribed_ids.push_back(topic);
            }
        }
        std::ranges::sort(subscribed_ids);
        subscribed_ids.erase_to_end(
          std::ranges::unique(subscribed_ids).begin());

        _subscribed.reserve(subscribed_ids.size());
        for (auto topic : subscribed_ids) {
            auto num_partitions = partition_count(_metadata, topic);
            if (!num_partitions.has_value()) {
                return std::unexpected(num_partitions.error());
            }
            _subscribed.push_back(
              {.topic = topic,
               .num_partitions = *num_partitions,
               .owners = chunked_vector<int32_t>(
                 std::from_range,
                 std::views::repeat(no_member, *num_partitions))});
        }

        const auto num_members = to_count(_group.members().size());
        for (auto index : std::views::iota(int32_t{0}, num_members)) {
            const auto& member = member_at(index);
            topic_cursor group_topics{_subscribed, &subscribed_topic::topic};
            for (const auto& topic : member.subscribed_topics) {
                auto found = group_topics.advance_to(topic);
                if (!found.has_value() || *found == group_topics.end()) {
                    // `_subscribed` holds every member's topics, so a miss
                    // means this member's subscription is out of order or
                    // repeats a topic.
                    return std::unexpected(
                      assignor_error{
                        .errc = assignor_errc::malformed_subscription,
                        .topic = topic});
                }
                (*found)->subscribers.push_back(index);
            }
        }
        return {};
    }

    /// Every member keeps its current assignment of the topics it still
    /// subscribes to. The rest is left unowned, for a member that does
    /// subscribe to it to pick up. Fails on an assignment that is out of
    /// order, or one with a partition its topic does not have.
    std::expected<void, assignor_error> keep_what_is_still_subscribed() {
        const auto num_members = to_count(_group.members().size());
        for (auto index : std::views::iota(int32_t{0}, num_members)) {
            const auto& member = member_at(index);
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
            topic_cursor member_topics{member.subscribed_topics};
            topic_cursor group_topics{_subscribed, &subscribed_topic::topic};
            for (const auto& [topic, curr_assigned] :
                 member.current_assignment) {
                if (
                  auto held = member_topics.advance_to(topic);
                  !held.has_value() || *held == member_topics.end()) {
                    continue;
                }
                auto found = group_topics.advance_to(topic);
                vassert(
                  found.has_value() && *found != group_topics.end(),
                  "{} is not subscribed",
                  topic);
                if (curr_assigned.empty()) {
                    continue;
                }
                if (
                  auto checked = check_assigned(
                    topic, curr_assigned, (*found)->num_partitions);
                  !checked.has_value()) {
                    return std::unexpected(checked.error());
                }
                for (auto partition : curr_assigned) {
                    (*found)->owner(partition()) = index;
                }
                load(index) += to_count(curr_assigned.size());
            }
        }
        return {};
    }

    /// The partitions assigned to nobody go to the least loaded subscriber of
    /// their topic, the topics with the least room to place them first.
    void assign_unowned(const chunked_vector<topic_by_load>& by_load) {
        for (const auto& ordered : by_load) {
            auto& subscribed = _subscribed[ordered.topic];
            bool restarted = false;
            for (auto id : std::views::iota(0, subscribed.num_partitions)) {
                if (subscribed.owner(id) != no_member) {
                    continue;
                }
                if (!restarted) {
                    _balancer.restart(subscribed.subscribers);
                    restarted = true;
                }
                assign_partition(subscribed, id, _balancer.next_least_loaded());
            }
        }
    }

    /// Sweeps the topics whose partitions can move at all, until every one of
    /// them was tried since the last move or the sweeps run out.
    void balance(const chunked_vector<topic_by_load>& by_load) {
        chunked_vector<size_t> movable;
        for (const auto& ordered : by_load) {
            // A topic with one subscriber has nowhere to move a partition to.
            if (_subscribed[ordered.topic].subscribers.size() >= 2) {
                movable.push_back(ordered.topic);
            }
        }
        if (movable.empty()) {
            return;
        }

        size_t tried_since_move = 0;
        for (int32_t sweep = 0; sweep < max_balance_sweeps; ++sweep) {
            for (auto topic : movable) {
                if (balance_topic(topic) > 0) {
                    tried_since_move = 0;
                } else if (++tried_since_move == movable.size()) {
                    return;
                }
            }
        }
    }

    /// Moves partitions of one topic from its most loaded subscribers to its
    /// least loaded ones, returning how many moved.
    int32_t balance_topic(size_t topic) {
        auto& subscribed = _subscribed[topic];
        if (_balancer.restart(subscribed.subscribers) <= 1) {
            return 0;
        }
        _holdings.rebuild(subscribed, _group.members().size());

        int32_t moved = 0;
        // The loop stops on the checks inside rather than on this condition:
        // either nobody is left with a partition of this topic to give up, or
        // the giver and taker turn out to be within a partition of each other.
        while (!_balancer.is_balanced()) {
            auto giver = next_giver();
            if (giver == no_member) {
                break;
            }
            auto taker = _balancer.next_least_loaded();
            if (_balancer.is_balanced()) {
                // Giver and taker came from levels within one of each other, so
                // the move would not improve the balance.
                break;
            }
            assign_partition(subscribed, _holdings.pop_highest(giver), taker);
            ++moved;
        }
        return moved;
    }

    /// The most loaded subscriber still assigned a partition of the topic
    /// being balanced, or `no_member` if none does.
    int32_t next_giver() {
        while (true) {
            auto giver = _balancer.next_most_loaded();
            if (giver == no_member) {
                return no_member;
            }
            if (_holdings.has_any(giver)) {
                return giver;
            }
            _balancer.exclude_most_loaded();
        }
    }

    /// Hands the partition to `member`, off whichever member owns it now.
    void assign_partition(
      subscribed_topic& subscribed, int32_t partition, int32_t member) {
        auto& owner = subscribed.owner(partition);
        if (owner != no_member) {
            --load(owner);
        }
        owner = member;
        ++load(member);
    }

    /// The group's subscribed topics, the ones with the least room to place
    /// first.
    chunked_vector<topic_by_load> topics_by_load() const {
        chunked_vector<topic_by_load> ordered;
        ordered.reserve(_subscribed.size());
        for (size_t topic = 0; topic < _subscribed.size(); ++topic) {
            ordered.push_back(
              {.topic = topic,
               .num_partitions = _subscribed[topic].num_partitions,
               .num_subscribers = _subscribed[topic].subscribers.size()});
        }
        std::ranges::sort(ordered, std::less<>{});
        return ordered;
    }

    /// The owners, turned back into an assignment per member.
    ///
    /// The topics are walked in increasing order and each one's partitions with
    /// them, so every member's assignment comes out ordered without being
    /// sorted.
    group_assignment materialize() const {
        group_assignment target{_group.members().size()};
        for (const auto& subscribed : _subscribed) {
            for (auto id : std::views::iota(0, subscribed.num_partitions)) {
                auto owner = subscribed.owner(id);
                if (owner == no_member) {
                    continue;
                }
                target[static_cast<size_t>(owner)].assign(
                  subscribed.topic, model::partition_id{id});
            }
        }
        return target;
    }

    const member_spec& member_at(int32_t index) const {
        return _group.members()[static_cast<size_t>(index)];
    }

    int32_t& load(int32_t member) {
        return _loads[static_cast<size_t>(member)];
    }

    const group_spec& _group;
    const topic_describer& _metadata;
    /// The group's subscribed topics, in order of increasing id. An index into
    /// this is what the rest of this refers to a topic by.
    chunked_vector<subscribed_topic> _subscribed;
    /// How many partitions each member is assigned across every topic, by
    /// member index.
    chunked_vector<int32_t> _loads;
    member_balancer _balancer;
    /// The partitions of the topic being balanced, grouped by owner.
    topic_holdings _holdings;
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
        return heterogeneous_assignment{group, topics}.build();
    }
    std::unreachable();
}

} // namespace kafka
