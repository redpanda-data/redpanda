/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace cloud_topics::l1;

namespace {
const object_id oid1 = l1::create_object_id();
const object_id oid2 = l1::create_object_id();
const object_id oid3 = l1::create_object_id();
const object_id oid4 = l1::create_object_id();
const std::string_view tidp_a = "deadbeef-aaaa-0000-0000-000000000000/0";
const std::string_view tidp_b = "deadbeef-bbbb-0000-0000-000000000000/0";
const std::string_view tidp_c = "deadbeef-cccc-0000-0000-000000000000/0";

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}
model::term_id operator""_tm(unsigned long long t) {
    return model::term_id{static_cast<int64_t>(t)};
}

class new_obj_builder {
public:
    new_obj_builder(object_id oid, size_t footer_pos, size_t object_size) {
        out.oid = oid;
        out.footer_pos = footer_pos;
        out.object_size = object_size;
    }
    new_obj_builder(const new_obj_builder&) = delete;
    new_obj_builder(new_obj_builder&&) = default;
    new_object build() { return std::move(out); }

    new_obj_builder& add(
      std::string_view tpr_str,
      kafka::offset base_o,
      kafka::offset last_o,
      model::timestamp last_t,
      size_t first_pos,
      size_t last_pos) {
        auto tpr = model::topic_id_partition::from(tpr_str);
        out.extent_metas[tpr.topic_id][tpr.partition] = new_object::metadata{
          .base_offset = base_o,
          .last_offset = last_o,
          .max_timestamp = last_t,
          .filepos = first_pos,
          .len = last_pos - first_pos,
        };
        return *this;
    }

private:
    new_object out;
};

struct add_objects_builder {
public:
    add_objects_builder& add(new_object o) {
        out.new_objects.emplace_back(std::move(o));
        return *this;
    }
    add_objects_builder&
    add_term_start(std::string_view tp_str, model::term_id t, kafka::offset o) {
        auto tp = model::topic_id_partition::from(tp_str);
        out.new_terms[tp].emplace_back(
          term_start{.term_id = t, .start_offset = o});
        return *this;
    }
    add_objects_update build() { return std::move(out); }

private:
    add_objects_update out;
};

struct replace_objects_builder {
public:
    replace_objects_builder& add(new_object o) {
        out.new_objects.emplace_back(std::move(o));
        return *this;
    }
    replace_objects_builder& clean(
      std::string_view tp_str,
      struct compaction_state_update::cleaned_range r,
      model::timestamp cleaned_at) {
        auto tp = model::topic_id_partition::from(tp_str);
        auto& c_state = out.compaction_updates[tp.topic_id][tp.partition];
        c_state.cleaned_at = cleaned_at;
        c_state.new_cleaned_range = r;
        return *this;
    }
    replace_objects_builder& clean_tombstones(
      std::string_view tp_str, kafka::offset base, kafka::offset last) {
        auto tp = model::topic_id_partition::from(tp_str);
        auto& c_state = out.compaction_updates[tp.topic_id][tp.partition];
        c_state.removed_tombstones_ranges.insert(base, last);
        return *this;
    }
    replace_objects_update build() { return std::move(out); }

private:
    replace_objects_update out;
};
} // namespace

TEST(StateUpdateTest, TestEmptyAdd) {
    state s;
    auto empty_update = add_objects_update::build(s, {}, {});
    EXPECT_FALSE(empty_update.has_value());
    EXPECT_EQ(empty_update.error(), "No objects requested");
}

TEST(StateUpdateTest, TestAddBasic) {
    state s;
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                               .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .add_term_start(tidp_b, 0_tm, 0_o)
                        .build();
        ASSERT_FALSE(update.new_objects.empty());
        auto res = update.apply(s);
        EXPECT_TRUE(res.has_value());
        EXPECT_EQ(2, s.topic_to_state.size());
    }
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid2, 100, 1100)
                               .add(tidp_c, 0_o, 10_o, 1999_t, 0, 99)
                               .add(tidp_b, 11_o, 20_o, 1999_t, 100, 199)
                               .build())
                        .add_term_start(tidp_c, 0_tm, 0_o)
                        .add_term_start(tidp_b, 0_tm, 11_o)
                        .build();
        ASSERT_FALSE(update.new_objects.empty());
        auto res = update.apply(s);
        EXPECT_TRUE(res.has_value());
        EXPECT_EQ(3, s.topic_to_state.size());
    }
}

TEST(StateUpdateTest, TestDuplicateAddSingleUpdate) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add(new_obj_builder(oid2, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    .add_term_start(tidp_b, 0_tm, 0_o)
                    .build();
    ASSERT_FALSE(update.new_objects.empty());
    auto res = update.can_apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()(), testing::HasSubstr("Input object breaks partition"));
}

TEST(StateUpdateTest, TestStartAfterZero) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 11_o)
                    .build();
    state s;
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());

    // The added object should be tracked as removed.
    EXPECT_EQ(0, s.topic_to_state.size());
    EXPECT_EQ(1, s.objects.size());
    auto& added_obj = s.objects.at(oid1);
    EXPECT_EQ(added_obj.removed_data_size, added_obj.total_data_size);
}

TEST(StateUpdateTest, TestDuplicateObject) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                           .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    .add_term_start(tidp_b, 0_tm, 0_o)
                    .add_term_start(tidp_c, 0_tm, 0_o)
                    .build();
    state s;
    ASSERT_FALSE(update.new_objects.empty());
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(3, s.topic_to_state.size());
    EXPECT_EQ(1, s.objects.size());

    // Apply the exact same add_object -- it should be deduped.
    auto dupe_res = update.can_apply(s);
    EXPECT_FALSE(dupe_res.has_value());
    EXPECT_THAT(dupe_res.error()(), testing::HasSubstr("already exists"))
      << dupe_res.error();
    EXPECT_EQ(1, s.objects.size());
}

TEST(StateUpdateTest, TestDuplicateAddMultipleUpdates) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                           .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    .add_term_start(tidp_b, 0_tm, 0_o)
                    .add_term_start(tidp_c, 0_tm, 0_o)
                    .build();
    state s;
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(3, s.topic_to_state.size());
    for (const auto& [t, p_states] : s.topic_to_state) {
        EXPECT_EQ(
          1, p_states.pid_to_state.at(model::partition_id{0}).extents.size());
    }
    EXPECT_EQ(1, s.objects.size());

    // Tweak the update to overlap with the one we just applied but with a
    // different object.
    update.new_objects[0].oid = oid2;
    auto dupe_res = update.apply(s);
    EXPECT_TRUE(dupe_res.has_value());
    EXPECT_EQ(3, s.topic_to_state.size());
    for (const auto& [t, p_states] : s.topic_to_state) {
        // The tracked extents shouldn't change.
        EXPECT_EQ(
          1, p_states.pid_to_state.at(model::partition_id{0}).extents.size());
    }
    EXPECT_EQ(2, s.objects.size());
    auto& dupe_obj = s.objects.at(oid2);
    EXPECT_EQ(dupe_obj.removed_data_size, dupe_obj.total_data_size);
}

TEST(StateUpdateTest, TestOverlapSomePartitions) {
    state s;
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                               .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .add_term_start(tidp_b, 0_tm, 0_o)
                        .build();
        auto res = update.apply(s);
        EXPECT_TRUE(res.has_value());
    }
    EXPECT_EQ(2, s.topic_to_state.size());
    for (const auto& [t, p_states] : s.topic_to_state) {
        EXPECT_EQ(
          1, p_states.pid_to_state.at(model::partition_id{0}).extents.size());
    }
    EXPECT_EQ(1, s.objects.size());

    // Now send a bogus range for one of the partitions, but a correct extent
    // for another.
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid2, 100, 1100)
                           .add(tidp_a, 1337_o, 1337_o, 1999_t, 0, 5)
                           .add(tidp_b, 11_o, 20_o, 1999_t, 100, 199)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 1337_o)
                    .add_term_start(tidp_b, 0_tm, 11_o)
                    .build();
    auto misaligned_res = update.apply(s);
    EXPECT_TRUE(misaligned_res.has_value());
    EXPECT_EQ(2, s.topic_to_state.size());

    // The bad update shouldn't be applied.
    auto p_a = s.partition_state(model::topic_id_partition::from(tidp_a));
    ASSERT_TRUE(p_a.has_value());
    EXPECT_EQ(1, p_a->get().extents.size());

    // But the good update should be there.
    auto p_b = s.partition_state(model::topic_id_partition::from(tidp_b));
    ASSERT_TRUE(p_b.has_value());
    EXPECT_EQ(2, p_b->get().extents.size());

    // The accounting for the object should reflect this.
    EXPECT_EQ(2, s.objects.size());
    auto& dupe_obj = s.objects.at(oid2);
    EXPECT_EQ(dupe_obj.removed_data_size, 5);
    EXPECT_EQ(dupe_obj.total_data_size, 104);
}

namespace {

MATCHER_P3(MatchesRange, oid, base, last, "") {
    return arg.oid == oid && arg.base_offset == base && arg.last_offset == last;
}
MATCHER_P2(MatchesRange, base, last, "") {
    return arg.base_offset == base && arg.last_offset == last;
}
MATCHER_P2(MatchesTermStart, term, offset, "") {
    return arg.term_id == term && arg.start_offset == offset;
}

} // namespace

TEST(StateUpdateTest, TestReplaceBasic) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                        .add(tidp_c, 11_o, 20_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .add_term_start(tidp_c, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    // Fully replace partition a, partially replace c.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid3, 100, 1100)
                            .add(tidp_a, 0_o, 20_o, 1999_t, 0, 99)
                            .add(tidp_c, 0_o, 10_o, 1999_t, 100, 199)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_TRUE(replace_res.has_value());

    // Fully replaced.
    const auto& prt_a
      = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid3, 0_o, 20_o)));

    // Not replaced.
    const auto& prt_b
      = s.partition_state(model::topic_id_partition::from(tidp_b))->get();
    EXPECT_THAT(prt_b.extents, ElementsAre(MatchesRange(oid1, 0_o, 10_o)));

    // Partially replaced.
    const auto& prt_c
      = s.partition_state(model::topic_id_partition::from(tidp_c))->get();
    EXPECT_THAT(
      prt_c.extents,
      ElementsAre(
        MatchesRange(oid3, 0_o, 10_o), MatchesRange(oid2, 11_o, 20_o)));

    EXPECT_EQ(s.objects.at(oid1).removed_data_size, 198);
    EXPECT_EQ(s.objects.at(oid2).removed_data_size, 99);
    EXPECT_EQ(s.objects.at(oid3).removed_data_size, 0);
}

TEST(StateUpdateTest, TestReplaceEmptyState) {
    state s;
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid1, 100, 1100)
                            .add(tidp_a, 0_o, 20_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex("Partition .+ not tracked by state"));
}

TEST(StateUpdateTest, TestReplaceDuplicate) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid1, 100, 1100)
                            .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex("Object .+ already exists"));
}

TEST(StateUpdateTest, TestReplaceMisaligned) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100, 1100)
                            .add(tidp_a, 0_o, 9_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex(
        "Partition .+ doesn't contain extents that span exactly"));
}

TEST(StateUpdateTest, TestReplaceBadOrdering) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    // Make the replacement overlap with itself.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100, 1100)
                            .add(tidp_a, 0_o, 7_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid3, 100, 1100)
                            .add(tidp_a, 5_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex("breaks partition .+ offset ordering"));
}

TEST(StateUpdateTest, TestEmptyReplace) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder().build();

    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::StrEq("No objects requested"));
}

TEST(StateUpdateTest, TestReplaceWithCompaction) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .add_term_start(tidp_c, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    // Fully replace partition a and clean part of it.
    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid2, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 10_o, .has_tombstones = true},
            1999_t)
          .build();

    auto replace_res = replace.apply(s);
    ASSERT_TRUE(replace_res.has_value());

    // Compact an extent, marking [5, 10] cleaned with tombstones.
    const auto& prt_a
      = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    ASSERT_TRUE(prt_a.compaction_state.has_value());
    EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid2, 0_o, 10_o)));
    EXPECT_THAT(
      prt_a.compaction_state->cleaned_ranges.to_vec(),
      ElementsAre(MatchesRange(5_o, 10_o)));
    EXPECT_THAT(
      prt_a.compaction_state->cleaned_ranges_with_tombstones,
      ElementsAre(MatchesRange(5_o, 10_o)));

    // Compact an extent, marking [3, 4] cleaned with tombstones.
    replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 3_o, .last_offset = 4_o, .has_tombstones = true},
            1999_t)
          .build();
    replace_res = replace.apply(s);
    ASSERT_TRUE(replace_res.has_value());

    EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid3, 0_o, 10_o)));
    EXPECT_THAT(
      prt_a.compaction_state->cleaned_ranges.to_vec(),
      ElementsAre(MatchesRange(3_o, 10_o)));
    EXPECT_THAT(
      prt_a.compaction_state->cleaned_ranges_with_tombstones,
      ElementsAre(MatchesRange(3_o, 4_o), MatchesRange(5_o, 10_o)));

    // Now mark [3, 8] as having removed tombstones.
    replace = replace_objects_builder()
                .add(new_obj_builder(oid4, 100, 1100)
                       .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                       .build())
                .clean_tombstones(tidp_a, 3_o, 8_o)
                .build();
    replace_res = replace.apply(s);
    ASSERT_TRUE(replace_res.has_value()) << replace_res.error();

    EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid4, 0_o, 10_o)));
    EXPECT_THAT(
      prt_a.compaction_state->cleaned_ranges.to_vec(),
      ElementsAre(MatchesRange(3_o, 10_o)));
    EXPECT_THAT(
      prt_a.compaction_state->cleaned_ranges_with_tombstones,
      ElementsAre(MatchesRange(9_o, 10_o)));
}

TEST(StateUpdateTest, TestCompactionMissingExtent) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    // Add a clean range for tidp_a but only supply an extent with tidp_b.
    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid2, 100, 1100)
                 .add(tidp_b, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 10_o, .has_tombstones = true},
            1999_t)
          .build();
    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex(
        "New cleaned range does not refer to partition with extent"));
}

TEST(StateUpdateTest, TestCompactionDoesntReplaceExtents) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 11_o, .has_tombstones = true},
            1999_t)
          .build();
    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex(
        "Cleaned range for .+ does not match requested "
        "new extents' last_offset"));
}

TEST(StateUpdateTest, TestCompactionDoesntReplaceExtentsStart) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());
    add = add_objects_builder()
            .add(new_obj_builder(oid2, 300, 1300)
                   .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                   .build())
            .add_term_start(tidp_a, 0_tm, 11_o)
            .build();
    add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    // Add a replacement extent and claim that it cleans a larger offset range.
    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 0_o, .last_offset = 20_o, .has_tombstones = true},
            1999_t)
          .build();
    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex(
        "Cleaned range start_offset for .+ is not covered by extents"));
}

TEST(StateUpdateTest, TestCompactionDoesntReplaceLogStart) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());
    add = add_objects_builder()
            .add(new_obj_builder(oid2, 300, 1300)
                   .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                   .build())
            .add_term_start(tidp_a, 0_tm, 11_o)
            .build();
    add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    // Add a replacement extent and claim that it makes a larger range clean.
    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 11_o, .last_offset = 20_o, .has_tombstones = true},
            1999_t)
          .build();
    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex(
        "Cleaned range .+ does not replace to the beginning of the log"));
}

TEST(StateUpdateTest, TestOverlappingTombstones) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid2, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 10_o, .has_tombstones = true},
            1999_t)
          .build();
    auto replace_res = replace.apply(s);
    ASSERT_TRUE(replace_res.has_value());

    replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 10_o, .last_offset = 10_o, .has_tombstones = true},
            1999_t)
          .build();
    replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex(
        "Cleaned range for .+ has tombstones and overlaps "
        "with an existing cleaned range with tombstones"));
}

TEST(StateUpdateTest, TestRemoveNonExistingTombstones) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    state s;
    auto add_res = add.apply(s);
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100, 1100)
                            .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .clean_tombstones(tidp_a, 5_o, 10_o)
                     .build();
    auto replace_res = replace.apply(s);
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_THAT(
      std::string(replace_res.error()()),
      testing::ContainsRegex(
        "Tombstone-removed range .+ for .+ is not tracked "
        "as having tombstones"));
}

TEST(StateUpdateTest, TestAddIncreasingTerms) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 1_tm, 0_o)
                    .add_term_start(tidp_a, 2_tm, 1_o)
                    .build();
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(1, s.topic_to_state.size());

    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 3_tm, 11_o)
               .add_term_start(tidp_a, 4_tm, 12_o)
               .build();
    res = update.apply(s);
    EXPECT_TRUE(res.has_value());

    auto p_state = s.partition_state(model::topic_id_partition::from(tidp_a));
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(2, p_state->get().extents.size());
    EXPECT_EQ(4, p_state->get().term_starts.size());
    EXPECT_THAT(
      p_state->get().term_starts,
      testing::ElementsAre(
        MatchesTermStart(1_tm, 0_o),
        MatchesTermStart(2_tm, 1_o),
        MatchesTermStart(3_tm, 11_o),
        MatchesTermStart(4_tm, 12_o)));
}

TEST(StateUpdateTest, TestAddSameSubsequentTerm) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 1_tm, 0_o)
                    .add_term_start(tidp_a, 2_tm, 1_o)
                    .build();
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(1, s.topic_to_state.size());
    auto p_state = s.partition_state(model::topic_id_partition::from(tidp_a));
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(1, p_state->get().extents.size());
    EXPECT_EQ(2, p_state->get().term_starts.size());
    EXPECT_THAT(
      p_state->get().term_starts,
      testing::ElementsAre(
        MatchesTermStart(1_tm, 0_o), MatchesTermStart(2_tm, 1_o)));

    // The start of term 2 shouldn't be changed, but term 3 should be added.
    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 2_tm, 11_o)
               .add_term_start(tidp_a, 3_tm, 12_o)
               .build();
    res = update.apply(s);
    EXPECT_TRUE(res.has_value());

    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(2, p_state->get().extents.size());
    EXPECT_EQ(3, p_state->get().term_starts.size());
    EXPECT_THAT(
      p_state->get().term_starts,
      testing::ElementsAre(
        MatchesTermStart(1_tm, 0_o),
        MatchesTermStart(2_tm, 1_o),
        MatchesTermStart(3_tm, 12_o)));
}

TEST(StateUpdateTest, TestAddNoTerms) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .build();

    auto res = update.can_apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()().c_str(),
      testing::HasSubstr("Missing term info in request"));
}

TEST(StateUpdateTest, TestAddMissingTermsForPartition) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    .build();

    auto res = update.can_apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()().c_str(), testing::HasSubstr("Missing term info for"));
}

TEST(StateUpdateTest, TestAddDecreasingTermInUpdate) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 2_tm, 0_o)
                    .add_term_start(tidp_a, 1_tm, 1_o)
                    .build();
    auto res = update.apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(res.error()().c_str(), testing::HasSubstr("Invalid term for"));
}

TEST(StateUpdateTest, TestAddDecreasingTerm) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 2_tm, 0_o)
                    .build();
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());

    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 1_tm, 11_o)
               .build();
    res = update.can_apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()().c_str(), testing::HasSubstr("must be >= last term"));
}

TEST(StateUpdateTest, TestAllowBogusTermWithBogusExtent) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 10_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 2_tm, 10_o)
                    .add_term_start(tidp_a, 1_tm, 10_o)
                    .build();
    // If there's a misaligned extent, we won't expect that its terms are valid
    // either, but we should expect the corrections to be populated.
    state s;
    chunked_hash_map<model::topic_id_partition, kafka::offset> corrections;
    auto res = update.can_apply(s, &corrections);
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(1, corrections.size());

    // When applying, the operation should succeed, but we should be left with
    // a dead object and no extents.
    res = update.apply(s);
    EXPECT_TRUE(res.has_value());
    EXPECT_TRUE(s.topic_to_state.empty());
    EXPECT_EQ(1, s.objects.size());
    auto& dead_obj = s.objects.begin()->second;
    EXPECT_EQ(dead_obj.removed_data_size, dead_obj.total_data_size);
}

TEST(StateUpdateTest, TestTermsWithNoExtent) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    // Add some terms for a missing partition.
                    .add_term_start(tidp_b, 0_tm, 0_o)
                    .build();
    state s;
    auto res = update.can_apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()().c_str(),
      testing::HasSubstr("Terms provided for a partition that has no extents"));
}

TEST(StateUpdateTest, TestAddMismatchedStartOffset) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 1_tm, 0_o)
                    .build();
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());

    // Add an update where the term's start offset doesn't match the extent.
    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 2_tm, 0_o)
               .build();
    res = update.can_apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()().c_str(),
      testing::HasSubstr("Extent start and term start do not match"));
}

TEST(StateUpdateTest, TestAddExtentEndsBelowLastTermStart) {
    state s;
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 1_tm, 0_o)
                    // We can add at the last offset.
                    .add_term_start(tidp_a, 2_tm, 10_o)
                    .build();
    auto res = update.apply(s);
    EXPECT_TRUE(res.has_value());

    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 3_tm, 11_o)
               // We cannot past the last offset.
               .add_term_start(tidp_a, 4_tm, 21_o)
               .build();
    res = update.apply(s);
    EXPECT_FALSE(res.has_value());
    EXPECT_THAT(
      res.error()().c_str(),
      testing::HasSubstr("Extents end below a requested new term for"));
}

TEST(StateUpdateTest, TestSetStartOffsetAlignedWithExtent) {
    state s;
    // Add some extents
    auto add_update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 10_o, 2000_t, 0, 99)
                               .build())
                        .add(new_obj_builder(oid2, 100, 1100)
                               .add(tidp_a, 11_o, 20_o, 2000_t, 0, 99)
                               .build())
                        .add(new_obj_builder(oid3, 100, 1100)
                               .add(tidp_a, 21_o, 30_o, 2000_t, 0, 99)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .build();
    auto res = add_update.apply(s);
    ASSERT_TRUE(res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(3, p_state->get().extents.size());

    // Set start offset to be aligned with the second extent (starts at 11)
    auto set_start_update = set_start_offset_update::build(s, tp, 11_o);
    ASSERT_TRUE(set_start_update.has_value());

    auto apply_res = set_start_update->apply(s);
    ASSERT_TRUE(apply_res.has_value());

    // Verify that the state has been updated correctly
    p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(11_o, p_state->get().start_offset);
    EXPECT_EQ(31_o, p_state->get().next_offset);

    // The first extent should be fully removed from accounting
    // Only extents 2 and 3 should remain
    EXPECT_EQ(2, p_state->get().extents.size());
}

TEST(StateUpdateTest, TestSetStartOffsetNotAlignedWithExtent) {
    state s;
    // Add some extents
    auto add_update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 10_o, 2000_t, 0, 99)
                               .build())
                        .add(new_obj_builder(oid2, 100, 1100)
                               .add(tidp_a, 11_o, 20_o, 2000_t, 0, 99)
                               .build())
                        .add(new_obj_builder(oid3, 100, 1100)
                               .add(tidp_a, 21_o, 30_o, 2000_t, 0, 99)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .build();
    auto res = add_update.apply(s);
    ASSERT_TRUE(res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(3, p_state->get().extents.size());

    // Set start offset to be not aligned with any extent (offset 15 is in the
    // middle of second extent)
    auto set_start_update = set_start_offset_update::build(s, tp, 15_o);
    ASSERT_TRUE(set_start_update.has_value());

    auto apply_res = set_start_update->apply(s);
    ASSERT_TRUE(apply_res.has_value());

    // Verify that the state has been updated correctly
    p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(15_o, p_state->get().start_offset);
    EXPECT_EQ(31_o, p_state->get().next_offset);

    // The first extent should be fully removed, but the second and third should
    // remain even though start is not aligned with the second extent's boundary
    EXPECT_EQ(2, p_state->get().extents.size());
}

TEST(StateUpdateTest, TestSetStartOffsetEmptyWithTerms) {
    state s;
    // Add extents with various terms
    auto add_update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 9_o, 2000_t, 0, 99)
                               .build())
                        .add_term_start(tidp_a, 1_tm, 0_o)
                        .add_term_start(tidp_a, 2_tm, 3_o)
                        .add_term_start(tidp_a, 5_tm, 7_o)
                        .build();
    auto res = add_update.apply(s);
    ASSERT_TRUE(res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(1, p_state->get().extents.size());
    EXPECT_EQ(3, p_state->get().term_starts.size());

    // Set start offset beyond the end of all extents to make log empty.
    auto set_start_update = set_start_offset_update::build(s, tp, 10_o);
    ASSERT_TRUE(set_start_update.has_value());

    auto apply_res = set_start_update->apply(s);
    ASSERT_TRUE(apply_res.has_value());

    p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(10_o, p_state->get().start_offset);
    EXPECT_EQ(10_o, p_state->get().next_offset);

    // All extents should be removed since start is beyond them.
    EXPECT_EQ(0, p_state->get().extents.size());

    // One term information should still be preserved to be able to serve term
    // queries at the next offset.
    EXPECT_EQ(1, p_state->get().term_starts.size());
}

TEST(StateUpdateTest, TestSetStartOffsetWithCompactionState) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;

    state s;
    // Add an extent and then compact it
    auto add_update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 20_o, 2000_t, 0, 99)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .build();
    auto res = add_update.apply(s);
    ASSERT_TRUE(res.has_value());

    // Compact part of the extent (clean offsets [5, 15])
    auto replace_update
      = replace_objects_builder()
          .add(new_obj_builder(oid2, 100, 1100)
                 .add(tidp_a, 0_o, 20_o, 2000_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 15_o, .has_tombstones = true},
            3000_t)
          .build();
    auto replace_res = replace_update.apply(s);
    ASSERT_TRUE(replace_res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    ASSERT_TRUE(p_state->get().compaction_state.has_value());

    // Verify initial compaction state
    EXPECT_THAT(
      p_state->get().compaction_state->cleaned_ranges.to_vec(),
      ElementsAre(MatchesRange(5_o, 15_o)));
    EXPECT_THAT(
      p_state->get().compaction_state->cleaned_ranges_with_tombstones,
      ElementsAre(MatchesRange(5_o, 15_o)));

    // Set start offset to fall within the cleaned range (offset 10)
    auto set_start_update = set_start_offset_update::build(s, tp, 10_o);
    ASSERT_TRUE(set_start_update.has_value());

    auto apply_res = set_start_update->apply(s);
    ASSERT_TRUE(apply_res.has_value());

    // Verify that the state has been updated correctly
    p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(10_o, p_state->get().start_offset);
    EXPECT_EQ(21_o, p_state->get().next_offset);

    // Check that compaction state reflects the new start
    // The cleaned ranges should be adjusted to reflect the new start
    ASSERT_TRUE(p_state->get().compaction_state.has_value());
    EXPECT_THAT(
      p_state->get().compaction_state->cleaned_ranges.to_vec(),
      ElementsAre(MatchesRange(10_o, 15_o)));
    EXPECT_THAT(
      p_state->get().compaction_state->cleaned_ranges_with_tombstones,
      ElementsAre(MatchesRange(10_o, 15_o)));
}

TEST(StateUpdateTest, TestRemoveObjectsBasic) {
    state s;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(add.apply(s).has_value());
    EXPECT_EQ(2, s.objects.size());

    // Replace objects to mark originals as unreferenced.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid3, 100, 1100)
                            .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_b, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .build();
    ASSERT_TRUE(replace.apply(s).has_value());
    EXPECT_EQ(4, s.objects.size());

    // Remove the unreferenced objects.
    auto remove_res = remove_objects_update::build(s, {oid1, oid2});
    ASSERT_TRUE(remove_res.has_value());
    ASSERT_TRUE(remove_res->apply(s).has_value());

    EXPECT_FALSE(s.objects.contains(oid1));
    EXPECT_FALSE(s.objects.contains(oid2));
    EXPECT_TRUE(s.objects.contains(oid3));
    EXPECT_TRUE(s.objects.contains(oid4));
    EXPECT_EQ(2, s.objects.size());

    // Move the start offset such that one of the partition's objects are fully
    // unreferenced.
    auto tp = model::topic_id_partition::from(tidp_a);
    auto set_start_update = set_start_offset_update::build(s, tp, 11_o);
    ASSERT_TRUE(set_start_update.has_value());
    auto apply_res = set_start_update->apply(s);
    ASSERT_TRUE(apply_res.has_value());

    // Remove the unreferenced objects.
    remove_res = remove_objects_update::build(s, {oid3});
    ASSERT_TRUE(remove_res.has_value());
    ASSERT_TRUE(remove_res->apply(s).has_value());

    EXPECT_FALSE(s.objects.contains(oid3));
    EXPECT_TRUE(s.objects.contains(oid4));
    EXPECT_EQ(1, s.objects.size());
}

TEST(StateUpdateTest, TestRemoveObjectsWithReferences) {
    state s;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 200, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(add.apply(s).has_value());
    EXPECT_EQ(1, s.objects.size());

    // Move the start offset such that one of the partitions is gone, but the
    // object is still referenced by the other.
    auto tp = model::topic_id_partition::from(tidp_a);
    auto set_start_update = set_start_offset_update::build(s, tp, 11_o);
    ASSERT_TRUE(set_start_update.has_value());
    auto apply_res = set_start_update->apply(s);
    ASSERT_TRUE(apply_res.has_value());

    auto remove_res = remove_objects_update::build(s, {oid1});
    ASSERT_FALSE(remove_res.has_value());
    EXPECT_THAT(
      remove_res.error()(), testing::ContainsRegex("is still referenced"));

    EXPECT_EQ(1, s.objects.size());
    EXPECT_TRUE(s.objects.contains(oid1));
}

TEST(StateUpdateTest, TestRemoveMissingObjects) {
    state s;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(add.apply(s).has_value());
    EXPECT_EQ(1, s.objects.size());

    // Remove references to oid1.
    auto tp = model::topic_id_partition::from(tidp_a);
    auto set_start_update = set_start_offset_update::build(s, tp, 11_o);
    ASSERT_TRUE(set_start_update.has_value());
    auto apply_res = set_start_update->apply(s);
    ASSERT_TRUE(apply_res.has_value());

    // Remove it, and objects that don't exist.
    auto remove_res = remove_objects_update::build(s, {oid1, oid2, oid3, oid4});
    ASSERT_TRUE(remove_res.has_value());
    apply_res = remove_res->apply(s);
    EXPECT_TRUE(apply_res.has_value());

    // The operation should have succeeded.
    EXPECT_EQ(0, s.objects.size());
}
