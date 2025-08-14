/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "gmock/gmock.h"

#include <gtest/gtest.h>

using namespace experimental::cloud_topics;
using namespace experimental::cloud_topics::l1;

namespace {

const object_id oid1 = l1::create_object_id();
const object_id oid2 = l1::create_object_id();
const object_id oid3 = l1::create_object_id();
const object_id oid4 = l1::create_object_id();

const std::string_view tid_a = "deadbeef-aaaa-0000-0000-000000000000/0";
const std::string_view tid_b = "deadbeef-bbbb-0000-0000-000000000000/0";
const std::string_view tid_c = "deadbeef-cccc-0000-0000-000000000000/0";

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}

MATCHER_P2(MatchesRange, base, last, "") {
    return arg.base_offset == base && arg.last_offset == last;
}

using om_list_t = chunked_vector<metastore::object_metadata>;
using cmap_t = metastore::compaction_map_t;
class om_builder {
public:
    om_builder(object_id oid, size_t footer_pos) {
        out.oid = oid;
        out.footer_pos = footer_pos;
    }
    om_builder& add(
      std::string_view tpr_str,
      kafka::offset base_o,
      kafka::offset last_o,
      model::timestamp last_t,
      size_t first_pos,
      size_t last_pos) {
        out.ntp_metas.emplace_back(metastore::object_metadata::ntp_metadata{
          .tidp = model::topic_id_partition::from(tpr_str),
          .base_offset = base_o,
          .last_offset = last_o,
          .max_timestamp = last_t,
          .pos = first_pos,
          .size = last_pos - first_pos,
        });
        return *this;
    }
    metastore::object_metadata build() { return std::move(out); }

private:
    metastore::object_metadata out;
};
class cm_builder {
public:
    cm_builder& clean(
      std::string_view tpr_str,
      kafka::offset base,
      kafka::offset last,
      std::optional<model::timestamp> with_tombstones_ts = std::nullopt) {
        auto tp = model::topic_id_partition::from(tpr_str);
        auto& cmp_meta = out[tp];
        cmp_meta.new_cleaned_range
          = metastore::compaction_update::cleaned_range{
            .base_offset = base,
            .last_offset = last,
            .has_tombstones = with_tombstones_ts.has_value(),
          };
        if (with_tombstones_ts) {
            cmp_meta.cleaned_at = *with_tombstones_ts;
        }
        return *this;
    }
    cm_builder& remove_tombstones(
      std::string_view tpr_str, kafka::offset base, kafka::offset last) {
        auto tp = model::topic_id_partition::from(tpr_str);
        auto& cmp_meta = out[tp];
        cmp_meta.removed_tombstones_ranges.insert(base, last);
        cmp_meta.cleaned_at = model::timestamp::now();
        return *this;
    }
    cmap_t build() { return std::move(out); }

private:
    cmap_t out;
};

} // namespace

TEST(SimpleMetastoreTest, TestGetMissingPartition) {
    simple_metastore m;
    auto offsets_res
      = m.get_offsets(model::topic_id_partition::from(tid_c)).get();
    ASSERT_FALSE(offsets_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, offsets_res.error());

    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_c), kafka::offset{0})
                     .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, get_res.error());

    auto ometa = om_builder(oid1, 200)
                   .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                   .add(tid_b, 0_o, 10_o, 2000_t, 100, 199)
                   .build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value()) << int(add_res.error());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    // We didn't add tid_c, so we should still get an error.
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(tid_c), kafka::offset{0})
                .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, get_res.error());

    // Wrong partition.
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(
                   "deadbeef-aaaa-0000-0000-000000000000/1"),
                 kafka::offset{0})
                .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, get_res.error());

    // Sanity check that we can query tid_a.
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(tid_a), kafka::offset{0})
                .get();
    ASSERT_TRUE(get_res.has_value()) << int(get_res.error());
    ASSERT_EQ(get_res->oid, oid1);
    ASSERT_EQ(get_res->footer_pos, 200);
}

TEST(SimpleMetastoreTest, TestAddWithGap) {
    simple_metastore m;
    {
        auto ometa
          = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
        ASSERT_TRUE(add_res.has_value()) << int(add_res.error());
        ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

        auto offsets_res
          = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(11_o, offsets_res->next_offset);
    }

    {
        // Add another object just past where we expect it.
        auto ometa
          = om_builder(oid2, 100).add(tid_a, 12_o, 20_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
        ASSERT_TRUE(add_res.has_value());
        ASSERT_EQ(1, add_res.value().corrected_next_offsets.size());

        // The offsets should not be affected.
        auto offsets_res
          = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(11_o, offsets_res->next_offset);
    }

    // Now add the object right at the end, where it should be.
    auto ometa
      = om_builder(oid3, 100).add(tid_a, 11_o, 20_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value()) << int(add_res.error());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto offsets_res
      = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(0_o, offsets_res->start_offset);
    ASSERT_EQ(21_o, offsets_res->next_offset);
}

TEST(SimpleMetastoreTest, TestAddWithOverlap) {
    simple_metastore m;
    {
        auto ometa
          = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(
                          chunked_vector<metastore::object_metadata>::single(
                            std::move(ometa)))
                         .get();
        ASSERT_TRUE(add_res.has_value()) << int(add_res.error());
        ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());
    }

    {
        // Add another object just below where we expect it.
        auto ometa
          = om_builder(oid2, 100).add(tid_a, 10_o, 20_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(
                          chunked_vector<metastore::object_metadata>::single(
                            std::move(ometa)))
                         .get();
        ASSERT_TRUE(add_res.has_value());
        ASSERT_EQ(1, add_res.value().corrected_next_offsets.size());
    }
    {
        // Add another object that fully overlaps with what exists.
        auto ometa
          = om_builder(oid3, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
        auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
        ASSERT_TRUE(add_res.has_value());
        ASSERT_EQ(1, add_res.value().corrected_next_offsets.size());
    }
}

TEST(SimpleMetastoreTest, TestAddPastBeginning) {
    simple_metastore m;
    // Add the first object so it doesn't start at 0.
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 1_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_EQ(1, add_res.value().corrected_next_offsets.size());
}

TEST(SimpleMetastoreTest, TestAddGetOffsetBasic) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid2, 100).add(tid_a, 11_o, 20_o, 2000_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid3, 100).add(tid_a, 21_o, 30_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& o : std::views::iota(0, 11)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& o : std::views::iota(11, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& o : std::views::iota(21, 31)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
}

TEST(SimpleMetastoreTest, TestAddGetOffsetBelowStart) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_a), kafka::offset{-1})
                     .get();
    ASSERT_TRUE(get_res.has_value());
    ASSERT_EQ(get_res->oid, oid1);
    ASSERT_EQ(get_res->footer_pos, 100);
}

TEST(SimpleMetastoreTest, TestAddGetOffsetOutOfRange) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_a), kafka::offset{11})
                     .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::out_of_range, get_res.error());
}

TEST(SimpleMetastoreTest, TestAddGetTimestampBasic) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 1999_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid2, 100).add(tid_a, 11_o, 20_o, 2999_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid3, 100).add(tid_a, 21_o, 30_o, 3999_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& t : {1000_t, 1999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& t : {2000_t, 2999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& t : {3000_t, 3999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
}

TEST(SimpleMetastoreTest, TestAddGetTimestampBelowStart) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto get_res
      = m.get_first_ge(model::topic_id_partition::from(tid_a), 999_t).get();
    ASSERT_TRUE(get_res.has_value());
    ASSERT_EQ(get_res->oid, oid1);
}

TEST(SimpleMetastoreTest, TestAddGetTimestampOutOfRange) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(om_list_t::single(std::move(ometa))).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto get_res
      = m.get_first_ge(model::topic_id_partition::from(tid_a), 3000_t).get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::out_of_range, get_res.error());
}

TEST(StateUpdateTest, TestReplaceBasic) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    om_list_t new_os;
    new_os.emplace_back(
      om_builder(oid2, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto replace_res = m.replace_objects(new_os).get();
    ASSERT_TRUE(replace_res.has_value());

    // Sanity check that replacement leaves us with expected offsets.
    auto offsets_res
      = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(0_o, offsets_res->start_offset);
    ASSERT_EQ(11_o, offsets_res->next_offset);
}

TEST(StateUpdateTest, TestReplaceMultipleOnePartition) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(om_builder(oid1, 100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                      .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid2, 100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .add(tid_b, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    om_list_t new_os;
    new_os.emplace_back(
      om_builder(oid3, 100).add(tid_a, 0_o, 20_o, 2000_t, 0, 99).build());
    auto replace_res = m.replace_objects(new_os).get();
    ASSERT_TRUE(replace_res.has_value());

    // Replaced offsets should be served from oid3.
    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& o : std::views::iota(0, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    // Others should be served from oid1 or oid2.
    tpr = model::topic_id_partition::from(tid_b);
    for (const auto& o : std::views::iota(0, 11)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    for (const auto& o : std::views::iota(11, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    // Sanity check that replacement leaves us with expected offsets.
    for (const auto& tid : {tid_a, tid_b}) {
        auto offsets_res
          = m.get_offsets(model::topic_id_partition::from(tid)).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(21_o, offsets_res->next_offset);
    }
}

TEST(StateUpdateTest, TestReplaceMultipleMultiplePartitions) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(om_builder(oid1, 100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                      .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid2, 100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .add(tid_b, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    // For one partition, replace the entire range. For another, replace part
    // of the range. As long as they're both aligned this should succeed.
    om_list_t new_os;
    new_os.emplace_back(om_builder(oid3, 100)
                          .add(tid_a, 0_o, 20_o, 2000_t, 0, 99)
                          .add(tid_b, 11_o, 20_o, 2000_t, 0, 99)
                          .build());
    auto replace_res = m.replace_objects(new_os).get();
    ASSERT_TRUE(replace_res.has_value());

    // Replaced offsets should be served from oid3.
    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& o : std::views::iota(0, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    tpr = model::topic_id_partition::from(tid_b);
    for (const auto& o : std::views::iota(11, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    // Others should be served from oid1 or oid2.
    for (const auto& o : std::views::iota(0, 11)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
    }
    // Sanity check that replacement leaves us with expected offsets.
    for (const auto& tid : {tid_a, tid_b}) {
        auto offsets_res
          = m.get_offsets(model::topic_id_partition::from(tid)).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(21_o, offsets_res->next_offset);
    }
}

TEST(StateUpdateTest, TestReplaceEmptyRequest) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    // Add a replacement object that has no objects.
    om_list_t new_os;
    auto replace_res = m.replace_objects(new_os).get();
    ASSERT_FALSE(replace_res.has_value());
    EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
}

TEST(StateUpdateTest, TestReplaceEmptyState) {
    simple_metastore m;
    {
        // Add a replacement object that has no objects.
        om_list_t new_os;
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
    {
        // Now try with an actual object. It should be rejected.
        om_list_t new_os;
        new_os.emplace_back(
          om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
}

TEST(StateUpdateTest, TestReplaceMisaligned) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    for (const auto& [base_o, last_o] :
         std::initializer_list<std::pair<kafka::offset, kafka::offset>>{
           {0_o, 11_o}, {1_o, 11_o}, {1_o, 10_o}, {1_o, 9_o}}) {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100)
                              .add(tid_a, base_o, last_o, 2000_t, 0, 99)
                              .build());
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
}

TEST(StateUpdateTest, TestReplaceOneWithMultipleMisaligned) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    {
        // Even though one extent overlaps, the complete range for tid_a does
        // not align.
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .add(tid_a, 11_o, 12_o, 2000_t, 0, 99)
                              .build());
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
    {
        // Even though tid_a overlaps exactly, tid_b does not exist.
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                              .build());
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
}

TEST(StateUpdateTest, TestReplaceMultipleMisaligned) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(om_builder(oid1, 100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                      .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid2, 100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .add(tid_b, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    {
        om_list_t new_os;
        new_os.emplace_back(
          om_builder(oid3, 100).add(tid_a, 0_o, 19_o, 2000_t, 0, 99).build());
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
    {
        // Even though tid_a overlaps exactly, tid_b is misaligned.
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid3, 100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .add(tid_b, 0_o, 19_o, 2000_t, 0, 99)
                              .build());
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
}

TEST(SimpleMetastoreTest, TestCompactionOffsetsMissingPartition) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_b, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    // Look for a different partition.
    auto cmp_a = m.get_compaction_offsets(
                    model::topic_id_partition::from(tid_a), 2000_t)
                   .get();
    ASSERT_FALSE(cmp_a.has_value());
    EXPECT_EQ(cmp_a.error(), metastore::errc::missing_ntp);
}

TEST(SimpleMetastoreTest, TestCompactionOffsetsAllDirty) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(om_builder(oid1, 100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                      .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                      .build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    // Without having anything compacted, the whole log is dirty.
    auto cmp_a = m.get_compaction_offsets(
                    model::topic_id_partition::from(tid_a), 2000_t)
                   .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(
      cmp_a->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(0_o, 10_o)));
}

TEST(SimpleMetastoreTest, TestCompactionOffsets) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    // Simple compaction to clean offsets [3, 5].
    {
        om_list_t new_os;
        new_os.emplace_back(
          om_builder(oid2, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 3_o, 5_o, 3000_t);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    // Sanity check that replacement leaves us with expected offsets.
    auto offsets_res
      = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(0_o, offsets_res->start_offset);
    ASSERT_EQ(11_o, offsets_res->next_offset);

    // Check that the cleaned range is reflected in the returned dirty ranges.
    auto cmp_a = m.get_compaction_offsets(
                    model::topic_id_partition::from(tid_a), 3000_t)
                   .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(
      cmp_a->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(0_o, 2_o), MatchesRange(6_o, 10_o)));
    EXPECT_THAT(
      cmp_a->removable_tombstone_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(3_o, 5_o)));

    // Sanity check that tombstone ranges aren't reported if the removable
    // cutoff is too low.
    cmp_a = m.get_compaction_offsets(
               model::topic_id_partition::from(tid_a), 2999_t)
              .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(
      cmp_a->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(0_o, 2_o), MatchesRange(6_o, 10_o)));
    EXPECT_THAT(
      cmp_a->removable_tombstone_ranges.to_vec(), testing::ElementsAre());

    // Now perform another replacement and remove some tombstones.
    {
        om_list_t new_os;
        new_os.emplace_back(
          om_builder(oid3, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 0_o, 2_o);
        cmb.remove_tombstones(tid_a, 3_o, 4_o);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }
    cmp_a = m.get_compaction_offsets(
               model::topic_id_partition::from(tid_a), 3000_t)
              .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(
      cmp_a->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(6_o, 10_o)));
    EXPECT_THAT(
      cmp_a->removable_tombstone_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(5_o, 5_o)));

    // Remove the rest of the tombstones and dirty ranges.
    {
        om_list_t new_os;
        new_os.emplace_back(
          om_builder(oid4, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 6_o, 10_o);
        cmb.remove_tombstones(tid_a, 5_o, 5_o);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }
    cmp_a = m.get_compaction_offsets(
               model::topic_id_partition::from(tid_a), 3000_t)
              .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(cmp_a->dirty_ranges.to_vec(), testing::ElementsAre());
    EXPECT_THAT(
      cmp_a->removable_tombstone_ranges.to_vec(), testing::ElementsAre());
}

TEST(SimpleMetastoreTest, TestCompactionOffsetsNoTombstones) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res = m.add_objects(os).get();
    ASSERT_TRUE(add_res.has_value());

    // Simple compaction to clean offsets [3, 5].
    {
        om_list_t new_os;
        new_os.emplace_back(
          om_builder(oid2, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 3_o, 5_o);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    // Sanity check that replacement leaves us with expected offsets.
    auto offsets_res
      = m.get_offsets(model::topic_id_partition::from(tid_a)).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(0_o, offsets_res->start_offset);
    ASSERT_EQ(11_o, offsets_res->next_offset);

    // Check that the cleaned range is reflected in the returned dirty ranges.
    auto cmp_a = m.get_compaction_offsets(
                    model::topic_id_partition::from(tid_a), 3000_t)
                   .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(
      cmp_a->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(0_o, 2_o), MatchesRange(6_o, 10_o)));
    EXPECT_THAT(
      cmp_a->removable_tombstone_ranges.to_vec(), testing::ElementsAre());

    // Sanity check that we have no tombstone ranges, since we didn't mark any
    // tombstoned ranges.
    cmp_a = m.get_compaction_offsets(
               model::topic_id_partition::from(tid_a), 2999_t)
              .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(
      cmp_a->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(0_o, 2_o), MatchesRange(6_o, 10_o)));
    EXPECT_THAT(
      cmp_a->removable_tombstone_ranges.to_vec(), testing::ElementsAre());

    // Now perform another replacement.
    {
        om_list_t new_os;
        new_os.emplace_back(
          om_builder(oid3, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 0_o, 2_o);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }
    cmp_a = m.get_compaction_offsets(
               model::topic_id_partition::from(tid_a), 3000_t)
              .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(
      cmp_a->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(6_o, 10_o)));
    EXPECT_THAT(
      cmp_a->removable_tombstone_ranges.to_vec(), testing::ElementsAre());

    // Remove the rest of the dirty ranges.
    {
        om_list_t new_os;
        new_os.emplace_back(
          om_builder(oid4, 100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 6_o, 10_o);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }
    cmp_a = m.get_compaction_offsets(
               model::topic_id_partition::from(tid_a), 3000_t)
              .get();
    ASSERT_TRUE(cmp_a.has_value());
    EXPECT_THAT(cmp_a->dirty_ranges.to_vec(), testing::ElementsAre());
    EXPECT_THAT(
      cmp_a->removable_tombstone_ranges.to_vec(), testing::ElementsAre());
}

TEST(SimpleMetastoreTest, TestObjectBuilder) {
    simple_metastore m;
    auto ob = m.object_builder();
    auto tp_a = model::topic_id_partition::from(tid_a);

    // Creating objects for the same partition will result in the same object.
    auto o_a = ob->get_or_create_object_for(tp_a);
    auto o_a_2 = ob->get_or_create_object_for(tp_a);
    ASSERT_EQ(o_a, o_a_2);

    // Creating objects for different partitions will result in the same
    // object.
    auto tp_b = model::topic_id_partition::from(tid_b);
    auto o_b = ob->get_or_create_object_for(tp_b);
    ASSERT_EQ(o_a, o_b);
    auto o_b_2 = ob->get_or_create_object_for(tp_b);
    ASSERT_EQ(o_b, o_b_2);

    // Add a partition's metadata to the object.
    ASSERT_TRUE(ob->add(o_a, {}).has_value());

    // Finish the current object. The next object will be different.
    ASSERT_TRUE(ob->finish(o_a, 0).has_value());

    auto o_a_3 = ob->get_or_create_object_for(tp_a);
    ASSERT_NE(o_a_2, o_a_3);

    // We can't release the result until we finish all objects.
    ASSERT_FALSE(dynamic_cast<simple_object_builder*>(ob.get())->release());
    ASSERT_TRUE(ob->finish(o_a_3, 0).has_value());

    auto release_res
      = dynamic_cast<simple_object_builder*>(ob.get())->release();
    ASSERT_TRUE(release_res.has_value());
    ASSERT_EQ(2, release_res->size());
    ASSERT_EQ(1, release_res.value()[0].ntp_metas.size());
    ASSERT_EQ(0, release_res.value()[1].ntp_metas.size());
}

TEST(SimpleMetastoreTest, TestObjectBuilderBadObjects) {
    // Test calls for objects that don't exist in the builder.
    simple_metastore m;
    auto ob = m.object_builder();
    auto add_res = ob->add(create_object_id(), {});
    ASSERT_FALSE(add_res.has_value());

    auto finish_res = ob->finish(create_object_id(), 0);
    ASSERT_FALSE(finish_res.has_value());

    // Both operations failed -- the builder should be empty.
    auto release_res
      = dynamic_cast<simple_object_builder*>(ob.get())->release();
    ASSERT_TRUE(release_res.has_value());
    ASSERT_EQ(0, release_res->size());
}

TEST(SimpleMetastoreTest, TestUpdateWithObjectBuilder) {
    simple_metastore m;
    auto tp_a = model::topic_id_partition::from(tid_a);
    {
        auto ob = m.object_builder();
        auto o_a = ob->get_or_create_object_for(tp_a);
        auto add_res = ob->add(
          o_a,
          metastore::object_metadata::ntp_metadata{
            .tidp = tp_a,
            .base_offset = 0_o,
            .last_offset = 9_o,
            .max_timestamp = 1000_t,
            .pos = 0,
            .size = 0,
          });
        ASSERT_TRUE(add_res.has_value());
        auto fin_res = ob->finish(o_a, 0);
        ASSERT_TRUE(fin_res.has_value());
        auto add_obj_res = m.add_objects(std::move(ob)).get();
        ASSERT_TRUE(add_obj_res.has_value());

        auto offsets_res = m.get_offsets(tp_a).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(10_o, offsets_res->next_offset);
    }
    {
        auto ob = m.object_builder();
        auto o_a = ob->get_or_create_object_for(tp_a);
        auto add_res = ob->add(
          o_a,
          metastore::object_metadata::ntp_metadata{
            .tidp = tp_a,
            .base_offset = 10_o,
            .last_offset = 19_o,
            .max_timestamp = 1000_t,
            .pos = 0,
            .size = 0,
          });
        ASSERT_TRUE(add_res.has_value());
        auto fin_res = ob->finish(o_a, 0);
        ASSERT_TRUE(fin_res.has_value());
        auto add_obj_res = m.add_objects(std::move(ob)).get();
        ASSERT_TRUE(add_obj_res.has_value());

        auto offsets_res = m.get_offsets(tp_a).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(20_o, offsets_res->next_offset);
    }
    {
        auto ob = m.object_builder();
        auto o_a = ob->get_or_create_object_for(tp_a);
        auto add_res = ob->add(
          o_a,
          metastore::object_metadata::ntp_metadata{
            .tidp = tp_a,
            .base_offset = 0_o,
            .last_offset = 19_o,
            .max_timestamp = 1000_t,
            .pos = 0,
            .size = 0,
          });
        ASSERT_TRUE(add_res.has_value());
        auto fin_res = ob->finish(o_a, 0);
        ASSERT_TRUE(fin_res.has_value());
        auto replace_obj_res = m.replace_objects(std::move(ob)).get();
        ASSERT_TRUE(replace_obj_res.has_value());

        auto offsets_res = m.get_offsets(tp_a).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(20_o, offsets_res->next_offset);
    }
    {
        auto ob = m.object_builder();
        auto o_a = ob->get_or_create_object_for(tp_a);
        auto add_res = ob->add(
          o_a,
          metastore::object_metadata::ntp_metadata{
            .tidp = tp_a,
            .base_offset = 0_o,
            .last_offset = 19_o,
            .max_timestamp = 1000_t,
            .pos = 0,
            .size = 0,
          });
        ASSERT_TRUE(add_res.has_value());
        auto fin_res = ob->finish(o_a, 0);
        ASSERT_TRUE(fin_res.has_value());

        cm_builder cmb;
        cmb.clean(tid_a, 10_o, 19_o);
        auto compact_obj_res
          = m.compact_objects(std::move(ob), cmb.build()).get();
        ASSERT_TRUE(compact_obj_res.has_value());

        auto offsets_res = m.get_offsets(tp_a).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(20_o, offsets_res->next_offset);

        auto compact_offsets_res = m.get_compaction_offsets(tp_a, 1000_t).get();
        auto dirty = compact_offsets_res->dirty_ranges.to_vec();
        EXPECT_THAT(dirty, testing::ElementsAre(MatchesRange(0_o, 9_o)));
    }
}
