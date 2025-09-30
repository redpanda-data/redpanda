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

using namespace cloud_topics;
using namespace cloud_topics::l1;

namespace {

const object_id oid1 = l1::create_object_id();
const object_id oid2 = l1::create_object_id();
const object_id oid3 = l1::create_object_id();
const object_id oid4 = l1::create_object_id();
const object_id oid5 = l1::create_object_id();

const std::string_view tid_a = "deadbeef-aaaa-0000-0000-000000000000/0";
const std::string_view tid_b = "deadbeef-bbbb-0000-0000-000000000000/0";
const std::string_view tid_c = "deadbeef-cccc-0000-0000-000000000000/0";

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}

model::term_id operator""_tm(unsigned long long t) {
    return model::term_id{static_cast<int64_t>(t)};
}

MATCHER_P2(MatchesRange, base, last, "") {
    return arg.base_offset == base && arg.last_offset == last;
}

using term_list_t = chunked_vector<metastore::term_offset>;
using term_map_t = metastore::term_offset_map_t;
class terms_builder {
public:
    terms_builder&
    add(std::string_view tp_str, model::term_id t, kafka::offset o) {
        auto tp = model::topic_id_partition::from(tp_str);
        out[tp].emplace_back(
          metastore::term_offset{.term = t, .first_offset = o});
        return *this;
    }
    term_map_t build() { return std::move(out); }

private:
    term_map_t out;
};

using om_list_t = chunked_vector<metastore::object_metadata>;
using cmap_t = metastore::compaction_map_t;
class om_builder {
public:
    om_builder(object_id oid, size_t footer_pos, size_t object_size) {
        out.oid = oid;
        out.footer_pos = footer_pos;
        out.object_size = object_size;
    }
    om_builder& add(
      std::string_view tpr_str,
      kafka::offset base_o,
      kafka::offset last_o,
      model::timestamp last_t,
      size_t first_pos,
      size_t last_pos) {
        out.ntp_metas.emplace_back(
          metastore::object_metadata::ntp_metadata{
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

    auto ometa = om_builder(oid1, 200, 1200)
                   .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                   .add(tid_b, 0_o, 10_o, 2000_t, 100, 199)
                   .build();
    auto add_res
      = m.add_objects(
           om_list_t::single(std::move(ometa)),
           terms_builder().add(tid_a, 0_tm, 0_o).add(tid_b, 0_tm, 0_o).build())
          .get();
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
    ASSERT_EQ(get_res->object_size, 1200);
    ASSERT_EQ(get_res->first_offset, 0_o);
    ASSERT_EQ(get_res->last_offset, 10_o);
}

TEST(SimpleMetastoreTest, TestAddWithGap) {
    simple_metastore m;
    {
        auto ometa = om_builder(oid1, 100, 1100)
                       .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                       .build();
        auto add_res = m.add_objects(
                          om_list_t::single(std::move(ometa)),
                          terms_builder().add(tid_a, 0_tm, 0_o).build())
                         .get();
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
        auto ometa = om_builder(oid2, 100, 1100)
                       .add(tid_a, 12_o, 20_o, 2000_t, 0, 99)
                       .build();
        auto add_res = m.add_objects(
                          om_list_t::single(std::move(ometa)),
                          terms_builder().add(tid_a, 0_tm, 12_o).build())
                         .get();
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
    auto ometa = om_builder(oid3, 100, 1100)
                   .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                   .build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder().add(tid_a, 0_tm, 11_o).build())
                     .get();
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
        auto ometa = om_builder(oid1, 100, 1100)
                       .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                       .build();
        auto add_res = m.add_objects(
                          chunked_vector<metastore::object_metadata>::single(
                            std::move(ometa)),
                          terms_builder().add(tid_a, 0_tm, 0_o).build())
                         .get();
        ASSERT_TRUE(add_res.has_value()) << int(add_res.error());
        ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());
    }

    {
        // Add another object just below where we expect it.
        auto ometa = om_builder(oid2, 100, 1100)
                       .add(tid_a, 10_o, 20_o, 2000_t, 0, 99)
                       .build();
        auto add_res = m.add_objects(
                          chunked_vector<metastore::object_metadata>::single(
                            std::move(ometa)),
                          terms_builder().add(tid_a, 0_tm, 10_o).build())
                         .get();
        ASSERT_TRUE(add_res.has_value());
        ASSERT_EQ(1, add_res.value().corrected_next_offsets.size());
    }
    {
        // Add another object that fully overlaps with what exists.
        auto ometa = om_builder(oid3, 100, 1100)
                       .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                       .build();
        auto add_res = m.add_objects(
                          om_list_t::single(std::move(ometa)),
                          terms_builder().add(tid_a, 0_tm, 0_o).build())
                         .get();
        ASSERT_TRUE(add_res.has_value());
        ASSERT_EQ(1, add_res.value().corrected_next_offsets.size());
    }
}

TEST(SimpleMetastoreTest, TestAddPastBeginning) {
    simple_metastore m;
    // Add the first object so it doesn't start at 0.
    auto ometa = om_builder(oid1, 100, 1100)
                   .add(tid_a, 1_o, 10_o, 2000_t, 0, 99)
                   .build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder().add(tid_a, 0_tm, 0_o).build())
                     .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_EQ(1, add_res.value().corrected_next_offsets.size());
}

TEST(SimpleMetastoreTest, TestAddGetOffsetBasic) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 21_o, 30_o, 2000_t, 0, 99)
                      .build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& o : std::views::iota(0, 11)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 0_o);
        ASSERT_EQ(get_res->last_offset, 10_o);
    }
    for (const auto& o : std::views::iota(11, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 11_o);
        ASSERT_EQ(get_res->last_offset, 20_o);
    }
    for (const auto& o : std::views::iota(21, 31)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 21_o);
        ASSERT_EQ(get_res->last_offset, 30_o);
    }
}

TEST(SimpleMetastoreTest, TestAddGetOffsetBelowStart) {
    simple_metastore m;
    auto ometa = om_builder(oid1, 100, 1100)
                   .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                   .build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder().add(tid_a, 0_tm, 0_o).build())
                     .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_a), kafka::offset{-1})
                     .get();
    ASSERT_TRUE(get_res.has_value());
    ASSERT_EQ(get_res->oid, oid1);
    ASSERT_EQ(get_res->footer_pos, 100);
    ASSERT_EQ(get_res->object_size, 1100);
    ASSERT_EQ(get_res->first_offset, 0_o);
    ASSERT_EQ(get_res->last_offset, 10_o);
}

TEST(SimpleMetastoreTest, TestAddGetOffsetOutOfRange) {
    simple_metastore m;
    auto ometa = om_builder(oid1, 100, 1100)
                   .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                   .build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder().add(tid_a, 0_tm, 0_o).build())
                     .get();
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
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 1999_t, 0, 99).build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 11_o, 20_o, 2999_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 21_o, 30_o, 3999_t, 0, 99)
                      .build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& t : {1000_t, 1999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 0_o);
        ASSERT_EQ(get_res->last_offset, 10_o);
    }
    for (const auto& t : {2000_t, 2999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 11_o);
        ASSERT_EQ(get_res->last_offset, 20_o);
    }
    for (const auto& t : {3000_t, 3999_t}) {
        auto get_res = m.get_first_ge(tpr, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 21_o);
        ASSERT_EQ(get_res->last_offset, 30_o);
    }
}

TEST(SimpleMetastoreTest, TestAddGetTimestampBelowStart) {
    simple_metastore m;
    auto ometa = om_builder(oid1, 100, 1100)
                   .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                   .build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder().add(tid_a, 0_tm, 0_o).build())
                     .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto get_res
      = m.get_first_ge(model::topic_id_partition::from(tid_a), 999_t).get();
    ASSERT_TRUE(get_res.has_value());
    ASSERT_EQ(get_res->oid, oid1);
}

TEST(SimpleMetastoreTest, TestAddGetTimestampOutOfRange) {
    simple_metastore m;
    auto ometa = om_builder(oid1, 100, 1100)
                   .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                   .build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder().add(tid_a, 0_tm, 0_o).build())
                     .get();
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
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    om_list_t new_os;
    new_os.emplace_back(
      om_builder(oid2, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
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
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                      .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .add(tid_b, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    auto add_res
      = m.add_objects(
           os,
           terms_builder().add(tid_a, 0_tm, 0_o).add(tid_b, 0_tm, 0_o).build())
          .get();
    ASSERT_TRUE(add_res.has_value());

    om_list_t new_os;
    new_os.emplace_back(
      om_builder(oid3, 100, 1100).add(tid_a, 0_o, 20_o, 2000_t, 0, 99).build());
    auto replace_res = m.replace_objects(new_os).get();
    ASSERT_TRUE(replace_res.has_value());

    // Replaced offsets should be served from oid3.
    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& o : std::views::iota(0, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 0_o);
        ASSERT_EQ(get_res->last_offset, 20_o);
    }
    // Others should be served from oid1 or oid2.
    tpr = model::topic_id_partition::from(tid_b);
    for (const auto& o : std::views::iota(0, 11)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 0_o);
        ASSERT_EQ(get_res->last_offset, 10_o);
    }
    for (const auto& o : std::views::iota(11, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 11_o);
        ASSERT_EQ(get_res->last_offset, 20_o);
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
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                      .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .add(tid_b, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    auto add_res
      = m.add_objects(
           os,
           terms_builder().add(tid_a, 0_tm, 0_o).add(tid_b, 0_tm, 0_o).build())
          .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    // For one partition, replace the entire range. For another, replace part
    // of the range. As long as they're both aligned this should succeed.
    om_list_t new_os;
    new_os.emplace_back(om_builder(oid3, 100, 1100)
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
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 0_o);
        ASSERT_EQ(get_res->last_offset, 20_o);
    }
    tpr = model::topic_id_partition::from(tid_b);
    for (const auto& o : std::views::iota(11, 21)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 11_o);
        ASSERT_EQ(get_res->last_offset, 20_o);
    }
    // Others should be served from oid1 or oid2.
    for (const auto& o : std::views::iota(0, 11)) {
        auto get_res = m.get_first_ge(tpr, kafka::offset{o}).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 0_o);
        ASSERT_EQ(get_res->last_offset, 10_o);
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
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
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
        new_os.emplace_back(om_builder(oid1, 100, 1100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .build());
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
}

TEST(StateUpdateTest, TestReplaceMisaligned) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    for (const auto& [base_o, last_o] :
         std::initializer_list<std::pair<kafka::offset, kafka::offset>>{
           {0_o, 11_o}, {1_o, 11_o}, {1_o, 10_o}, {1_o, 9_o}}) {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100, 1100)
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
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    {
        // Even though one extent overlaps, the complete range for tid_a does
        // not align.
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100, 1100)
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
        new_os.emplace_back(om_builder(oid2, 100, 1100)
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
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                      .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .add(tid_b, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    auto add_res
      = m.add_objects(
           os,
           terms_builder().add(tid_a, 0_tm, 0_o).add(tid_b, 0_tm, 0_o).build())
          .get();
    ASSERT_TRUE(add_res.has_value());

    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid3, 100, 1100)
                              .add(tid_a, 0_o, 19_o, 2000_t, 0, 99)
                              .build());
        auto replace_res = m.replace_objects(new_os).get();
        ASSERT_FALSE(replace_res.has_value());
        EXPECT_EQ(replace_res.error(), metastore::errc::invalid_request);
    }
    {
        // Even though tid_a overlaps exactly, tid_b is misaligned.
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid3, 100, 1100)
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
      om_builder(oid1, 100, 1100).add(tid_b, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_b, 0_tm, 0_o).build()).get();
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
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                      .add(tid_b, 0_o, 10_o, 2000_t, 0, 99)
                      .build());
    auto add_res
      = m.add_objects(
           os,
           terms_builder().add(tid_a, 0_tm, 0_o).add(tid_b, 0_tm, 0_o).build())
          .get();
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
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    // Simple compaction to clean offsets [3, 5].
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100, 1100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .build());

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
        new_os.emplace_back(om_builder(oid3, 100, 1100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .build());

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
        new_os.emplace_back(om_builder(oid4, 100, 1100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .build());

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
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    // Simple compaction to clean offsets [3, 5].
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100, 1100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .build());

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
        new_os.emplace_back(om_builder(oid3, 100, 1100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .build());

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
        new_os.emplace_back(om_builder(oid4, 100, 1100)
                              .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                              .build());

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
    auto ob = m.object_builder().get().value();
    auto tp_a = model::topic_id_partition::from(tid_a);

    // Creating objects for the same partition will result in the same object.
    auto o_a = ob->get_or_create_object_for(tp_a).value();
    auto o_a_2 = ob->get_or_create_object_for(tp_a).value();
    ASSERT_EQ(o_a, o_a_2);

    // Creating objects for different partitions will result in the same
    // object.
    auto tp_b = model::topic_id_partition::from(tid_b);
    auto o_b = ob->get_or_create_object_for(tp_b).value();
    ASSERT_EQ(o_a, o_b);
    auto o_b_2 = ob->get_or_create_object_for(tp_b).value();
    ASSERT_EQ(o_b, o_b_2);

    // Add a partition's metadata to the object.
    ASSERT_TRUE(ob->add(o_a, {}).has_value());

    // Finish the current object. The next object will be different.
    ASSERT_TRUE(ob->finish(o_a, 0, 1000).has_value());

    auto o_a_3 = ob->get_or_create_object_for(tp_a).value();
    ASSERT_NE(o_a_2, o_a_3);

    // We can't release the result until we finish all objects.
    ASSERT_FALSE(dynamic_cast<simple_object_builder*>(ob.get())->release());
    ASSERT_TRUE(ob->finish(o_a_3, 0, 1000).has_value());

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
    auto ob = m.object_builder().get().value();
    auto add_res = ob->add(create_object_id(), {});
    ASSERT_FALSE(add_res.has_value());

    auto finish_res = ob->finish(create_object_id(), 0, 1000);
    ASSERT_FALSE(finish_res.has_value());

    // Both operations failed -- the builder should be empty.
    auto release_res
      = dynamic_cast<simple_object_builder*>(ob.get())->release();
    ASSERT_TRUE(release_res.has_value());
    ASSERT_EQ(0, release_res->size());
}

TEST(SimpleMetastoreTest, TestObjectBuilderRemovedObjects) {
    simple_metastore m;
    auto ob = m.object_builder().get().value();
    const auto topic_id = model::create_topic_id();

    auto gen_object_id = [&] {
        return ob
          ->get_or_create_object_for(
            model::topic_id_partition(
              topic_id, model::partition_id(static_cast<int32_t>(0))))
          .value();
    };

    // pending object can be removed, but not twice
    auto oid = gen_object_id();
    ASSERT_TRUE(ob->remove_pending_object(oid).has_value());
    ASSERT_FALSE(ob->remove_pending_object(oid).has_value());

    // after removal, object id shouldn't be reused in this builder
    auto oid2 = gen_object_id();
    ASSERT_NE(oid, oid2);
    oid = oid2;

    // unfinished object with data can be removed
    ASSERT_TRUE(ob->add(oid, {}).has_value());
    ASSERT_TRUE(ob->remove_pending_object(oid).has_value());
    ASSERT_FALSE(ob->remove_pending_object(oid).has_value());
    ASSERT_FALSE(ob->add(oid, {}).has_value());

    oid2 = gen_object_id();
    ASSERT_NE(oid, oid2);
    oid = oid2;

    // finished object cannot be removed
    oid = gen_object_id();
    ASSERT_TRUE(ob->finish(oid, 0, 0).has_value());
    ASSERT_FALSE(ob->remove_pending_object(oid).has_value());
    ASSERT_FALSE(ob->finish(oid, 0, 0).has_value());
}

TEST(SimpleMetastoreTest, TestUpdateWithObjectBuilder) {
    simple_metastore m;
    auto tp_a = model::topic_id_partition::from(tid_a);
    {
        auto ob = m.object_builder().get().value();
        auto o_a = ob->get_or_create_object_for(tp_a).value();
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
        auto fin_res = ob->finish(o_a, 0, 1000);
        ASSERT_TRUE(fin_res.has_value());
        auto add_obj_res
          = m.add_objects(*ob, terms_builder().add(tid_a, 0_tm, 0_o).build())
              .get();
        ASSERT_TRUE(add_obj_res.has_value());

        auto offsets_res = m.get_offsets(tp_a).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(10_o, offsets_res->next_offset);
    }
    {
        auto ob = m.object_builder().get().value();
        auto o_a = ob->get_or_create_object_for(tp_a).value();
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
        auto fin_res = ob->finish(o_a, 0, 1000);
        ASSERT_TRUE(fin_res.has_value());
        auto add_obj_res
          = m.add_objects(*ob, terms_builder().add(tid_a, 0_tm, 10_o).build())
              .get();
        ASSERT_TRUE(add_obj_res.has_value());

        auto offsets_res = m.get_offsets(tp_a).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(20_o, offsets_res->next_offset);
    }
    {
        auto ob = m.object_builder().get().value();
        auto o_a = ob->get_or_create_object_for(tp_a).value();
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
        auto fin_res = ob->finish(o_a, 0, 1000);
        ASSERT_TRUE(fin_res.has_value());
        auto replace_obj_res = m.replace_objects(*ob).get();
        ASSERT_TRUE(replace_obj_res.has_value());

        auto offsets_res = m.get_offsets(tp_a).get();
        ASSERT_TRUE(offsets_res.has_value());
        ASSERT_EQ(0_o, offsets_res->start_offset);
        ASSERT_EQ(20_o, offsets_res->next_offset);
    }
    {
        auto ob = m.object_builder().get().value();
        auto o_a = ob->get_or_create_object_for(tp_a).value();
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
        auto fin_res = ob->finish(o_a, 0, 1000);
        ASSERT_TRUE(fin_res.has_value());

        cm_builder cmb;
        cmb.clean(tid_a, 10_o, 19_o);
        auto compact_obj_res = m.compact_objects(*ob, cmb.build()).get();
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

TEST(SimpleMetastoreState, TestInvalidTermRequest) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    // Make the term misaligned with the extent.
    auto add_res = m.add_objects(
                      os, terms_builder().add(tid_a, 0_tm, 1337_o).build())
                     .get();
    ASSERT_FALSE(add_res.has_value());
    ASSERT_EQ(add_res.error(), metastore::errc::invalid_request);
}

TEST(SimpleMetastoreTest, TestEndOffsetForEpoch) {
    simple_metastore m;
    auto ometa = om_builder(oid1, 200, 1200)
                   .add(tid_a, 0_o, 20_o, 2000_t, 0, 99)
                   .build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder()
                        .add(tid_a, 1_tm, 0_o)
                        .add(tid_a, 2_tm, 5_o)
                        .add(tid_a, 5_tm, 10_o)
                        .add(tid_a, 6_tm, 15_o)
                        .add(tid_a, 7_tm, 20_o)
                        .build())
                     .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_EQ(0, add_res->corrected_next_offsets.size());

    auto tp = model::topic_id_partition::from(tid_a);
    const auto assert_end_term_eq = [&](model::term_id t, kafka::offset o) {
        auto res = m.get_end_offset_for_term(tp, t).get();
        EXPECT_TRUE(res.has_value()) << "term: " << t;
        EXPECT_EQ(res.value(), o) << "term: " << t;
    };
    // Below the earliest term.
    assert_end_term_eq(0_tm, 0_o);

    // Exact match of term.
    assert_end_term_eq(1_tm, 5_o);
    assert_end_term_eq(2_tm, 10_o);

    // Terms that are in between term entries get bumped up to the next highest
    // end offset.
    assert_end_term_eq(3_tm, 10_o);
    assert_end_term_eq(4_tm, 10_o);

    // Exact match of term.
    assert_end_term_eq(5_tm, 15_o);
    assert_end_term_eq(6_tm, 20_o);

    // The last term.
    assert_end_term_eq(7_tm, 21_o);

    // Out of range.
    auto res = m.get_end_offset_for_term(tp, 8_tm).get();
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(metastore::errc::out_of_range, res.error());

    // Missing NTP.
    res = m.get_end_offset_for_term(
             model::topic_id_partition::from(tid_b), 0_tm)
            .get();
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(metastore::errc::missing_ntp, res.error());
}

TEST(SimpleMetastoreTest, TestEpochForOffset) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 200, 1200).add(tid_a, 0_o, 9_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder()
                        .add(tid_a, 1_tm, 0_o)
                        .add(tid_a, 2_tm, 3_o)
                        .add(tid_a, 5_tm, 5_o)
                        .add(tid_a, 6_tm, 7_o)
                        .add(tid_a, 7_tm, 9_o)
                        .build())
                     .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_EQ(0, add_res->corrected_next_offsets.size());

    auto tp = model::topic_id_partition::from(tid_a);
    const auto assert_term_eq = [&](kafka::offset o, model::term_id t) {
        auto res = m.get_term_for_offset(tp, o).get();
        EXPECT_TRUE(res.has_value()) << "offset: " << o;
        EXPECT_EQ(res.value(), t) << "offset: " << o;
    };
    assert_term_eq(0_o, 1_tm);
    assert_term_eq(1_o, 1_tm);
    assert_term_eq(2_o, 1_tm);
    assert_term_eq(3_o, 2_tm);
    assert_term_eq(4_o, 2_tm);
    assert_term_eq(5_o, 5_tm);
    assert_term_eq(6_o, 5_tm);
    assert_term_eq(7_o, 6_tm);
    assert_term_eq(8_o, 6_tm);
    assert_term_eq(9_o, 7_tm);

    // At next.
    assert_term_eq(10_o, 7_tm);

    // Below start.
    auto res = m.get_term_for_offset(tp, kafka::offset{-1}).get();
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(metastore::errc::out_of_range, res.error());

    // Above next.
    res = m.get_term_for_offset(tp, 11_o).get();
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(metastore::errc::out_of_range, res.error());

    // Missing NTP.
    res = m.get_term_for_offset(model::topic_id_partition::from(tid_b), 1_o)
            .get();
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(metastore::errc::missing_ntp, res.error());
}

TEST(SimpleMetastoreTest, TestSetStartAlignedWithExtent) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 21_o, 30_o, 2000_t, 0, 99)
                      .build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tp = model::topic_id_partition::from(tid_a);

    // Set start offset to be aligned with the second extent.
    auto set_start_res = m.set_start_offset(tp, 11_o).get();
    ASSERT_TRUE(set_start_res.has_value());

    auto offsets_res = m.get_offsets(tp).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(11_o, offsets_res->start_offset);
    ASSERT_EQ(31_o, offsets_res->next_offset);

    // Getting objects below the new start (11) should return the earliest
    // object, as should extents that actually point to that object.
    for (const auto& o : {0_o, 5_o, 10_o, 11_o, 15_o, 20_o}) {
        auto get_res = m.get_first_ge(tp, o).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
    }

    // Sanity check getting the object after.
    for (const auto& o : {21_o, 25_o, 30_o}) {
        auto get_res = m.get_first_ge(tp, o).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
    }
}

TEST(SimpleMetastoreTest, TestSetStartNotAlignedWithExtent) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, 99)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 21_o, 30_o, 2000_t, 0, 99)
                      .build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tp = model::topic_id_partition::from(tid_a);

    // Set start offset to be not aligned with any extent (offset 15 is in the
    // middle of second extent)
    auto set_start_res = m.set_start_offset(tp, 15_o).get();
    ASSERT_TRUE(set_start_res.has_value());

    auto offsets_res = m.get_offsets(tp).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(15_o, offsets_res->start_offset);
    ASSERT_EQ(31_o, offsets_res->next_offset);

    // Getting objects below the start (15) should return the earliest object,
    // as should extents that actually point to that object.
    for (const auto& o : {0_o, 5_o, 10_o, 14_o, 15_o, 16_o, 20_o}) {
        auto get_res = m.get_first_ge(tp, o).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
    }

    // Sanity check getting the object after.
    for (const auto& o : {21_o, 25_o, 30_o}) {
        auto get_res = m.get_first_ge(tp, o).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid3);
    }
}

TEST(SimpleMetastoreTest, TestSetStartEmptyWithTerms) {
    simple_metastore m;
    auto ometa
      = om_builder(oid1, 100, 1100).add(tid_a, 0_o, 9_o, 2000_t, 0, 99).build();
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder()
                        .add(tid_a, 1_tm, 0_o)
                        .add(tid_a, 2_tm, 3_o)
                        .add(tid_a, 5_tm, 7_o)
                        .build())
                     .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tp = model::topic_id_partition::from(tid_a);

    // Set start offset beyond the end of all extents to make log effectively
    // empty
    auto set_start_res = m.set_start_offset(tp, 10_o).get();
    ASSERT_TRUE(set_start_res.has_value());

    auto offsets_res = m.get_offsets(tp).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(10_o, offsets_res->start_offset);
    ASSERT_EQ(10_o, offsets_res->next_offset);

    // Getting objects should return out_of_range since log is effectively empty
    auto get_res = m.get_first_ge(tp, 10_o).get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::out_of_range, get_res.error());

    // We should still be able to get the term for the next offset The term at
    // next offset should be the last term from the extent
    auto term_res = m.get_term_for_offset(tp, 10_o).get();
    ASSERT_TRUE(term_res.has_value());
    ASSERT_EQ(5_tm, term_res.value());
}

TEST(SimpleMetastoreTest, TestSetStartWithCompactionState) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 20_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    // Clean range is [5, 15].
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100, 1100)
                              .add(tid_a, 0_o, 20_o, 2000_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 5_o, 15_o, 3000_t);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    auto tp = model::topic_id_partition::from(tid_a);

    // Verify initial compaction state and dirty ranges: [0, 4] and [16, 20].
    auto cmp_before = m.get_compaction_offsets(tp, 3000_t).get();
    ASSERT_TRUE(cmp_before.has_value());
    EXPECT_THAT(
      cmp_before->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(0_o, 4_o), MatchesRange(16_o, 20_o)));
    EXPECT_THAT(
      cmp_before->removable_tombstone_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(5_o, 15_o)));

    // Set start offset to fall within the cleaned range.
    auto set_start_res = m.set_start_offset(tp, 10_o).get();
    ASSERT_TRUE(set_start_res.has_value());

    // Check that offsets are updated.
    auto offsets_res = m.get_offsets(tp).get();
    ASSERT_TRUE(offsets_res.has_value());
    ASSERT_EQ(10_o, offsets_res->start_offset);
    ASSERT_EQ(21_o, offsets_res->next_offset);

    // Only the [16, 20] should remain dirty.
    auto cmp_after = m.get_compaction_offsets(tp, 3000_t).get();
    ASSERT_TRUE(cmp_after.has_value());
    EXPECT_THAT(
      cmp_after->dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(16_o, 20_o)));

    // Removable tombstone ranges should also be adjusted to reflect the new
    // start.
    EXPECT_THAT(
      cmp_after->removable_tombstone_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(10_o, 15_o)));
}

TEST(SimpleMetastoreTest, TestDirtyRatio) {
    simple_metastore m;
    om_list_t os;
    auto tp = model::topic_id_partition::from(tid_a);
    os.emplace_back(
      om_builder(oid1, 100, 10).add(tid_a, 0_o, 9_o, 1000_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid2, 100, 10).add(tid_a, 10_o, 19_o, 2000_t, 0, 99).build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    // Clean range is [0, 5]. Both extents still have dirty offsets.
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid3, 100, 10)
                              .add(tid_a, 0_o, 9_o, 1000_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 0_o, 5_o, 3000_t);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    auto to_sample = metastore::compaction_sample_spec{
      .tidp = tp, .tombstone_removal_upper_bound_ts = 3000_t};
    auto compaction_info = m.get_compaction_info(to_sample).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);

    // Clean range is now [0, 9]. Only one extent still has dirty offsets.
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid4, 100, 10)
                              .add(tid_a, 0_o, 9_o, 1000_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 6_o, 9_o, 3000_t);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    compaction_info = m.get_compaction_info(to_sample).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.5);

    // Clean range is now [0, 19], the entire log is clean.
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid5, 100, 10)
                              .add(tid_a, 0_o, 19_o, 1000_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 10_o, 19_o, 3000_t);
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    compaction_info = m.get_compaction_info(to_sample).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
}

TEST(SimpleMetastoreTest, TestAddGetOffsetAfterBytes) {
    simple_metastore m;
    om_list_t os;
    constexpr size_t data_size = 99;
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 10_o, 2000_t, 0, data_size)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 11_o, 20_o, 2000_t, 0, data_size)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 21_o, 30_o, 2000_t, 0, data_size)
                      .build());
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tpr = model::topic_id_partition::from(tid_a);
    auto get_res = m.get_first_offset_for_bytes(tpr, 0).get();
    ASSERT_TRUE(get_res.has_value()) << "for size: 0";
    ASSERT_EQ(get_res.value(), 31_o) << "for size: 0";
    size_t query_size = 1;
    for (auto offset : {21_o, 11_o, 0_o}) {
        for (size_t i = 0; i < data_size; ++i, ++query_size) {
            auto get_res = m.get_first_offset_for_bytes(tpr, query_size).get();
            ASSERT_TRUE(get_res.has_value()) << "for size: " << query_size;
            ASSERT_EQ(get_res.value(), offset) << "for size: " << query_size;
        }
    }
    get_res = m.get_first_offset_for_bytes(tpr, ++query_size).get();
    ASSERT_FALSE(get_res.has_value()) << "for size: " << query_size;
    ASSERT_EQ(get_res.error(), metastore::errc::out_of_range)
      << "for size: " << query_size;
}
