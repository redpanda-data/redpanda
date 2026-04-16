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
#include "cloud_topics/level_one/metastore/tests/builders.h"
#include "gmock/gmock.h"

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace cloud_topics::l1;
using namespace cloud_topics::l1::test_utils;

using ::testing::Field;
using ::testing::IsEmpty;
using ::testing::Optional;

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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid3));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid3));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto tpr = model::topic_id_partition::from(tid_a);
    for (const auto& t : {1000_t, 1999_t}) {
        auto get_res = m.get_first_ge(tpr, {}, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid1);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 0_o);
        ASSERT_EQ(get_res->last_offset, 10_o);
    }
    for (const auto& t : {2000_t, 2999_t}) {
        auto get_res = m.get_first_ge(tpr, {}, t).get();
        ASSERT_TRUE(get_res.has_value());
        ASSERT_EQ(get_res->oid, oid2);
        ASSERT_EQ(get_res->footer_pos, 100);
        ASSERT_EQ(get_res->object_size, 1100);
        ASSERT_EQ(get_res->first_offset, 11_o);
        ASSERT_EQ(get_res->last_offset, 20_o);
    }
    for (const auto& t : {3000_t, 3999_t}) {
        auto get_res = m.get_first_ge(tpr, {}, t).get();
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder().add(tid_a, 0_tm, 0_o).build())
                     .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto get_res
      = m.get_first_ge(model::topic_id_partition::from(tid_a), {}, 999_t).get();
    ASSERT_TRUE(get_res.has_value());
    ASSERT_EQ(get_res->oid, oid1);
}

TEST(SimpleMetastoreTest, TestAddGetTimestampOutOfRange) {
    simple_metastore m;
    auto ometa = om_builder(oid1, 100, 1100)
                   .add(tid_a, 0_o, 10_o, 2000_t, 0, 99)
                   .build();
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
    auto add_res = m.add_objects(
                      om_list_t::single(std::move(ometa)),
                      terms_builder().add(tid_a, 0_tm, 0_o).build())
                     .get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_a), {}, 3000_t)
                     .get();
    ASSERT_FALSE(get_res.has_value());
    ASSERT_EQ(metastore::errc::out_of_range, get_res.error());
}

TEST(SimpleMetastoreTest, TestAddGetTimestampCustomStart) {
    simple_metastore m;
    om_list_t ometas;
    ometas.push_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    ometas.push_back(om_builder(oid2, 100, 1100)
                       .add(tid_a, 11_o, 20_o, 1000_t, 0, 99)
                       .build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        m.preregister_objects(ids);
    }
    auto add_res = m.add_objects(
                      ometas, terms_builder().add(tid_a, 0_tm, 0_o).build())
                     .get();
    auto corrected_next_offsets
      = &metastore::add_response::corrected_next_offsets;
    EXPECT_THAT(add_res, Optional(Field(corrected_next_offsets, IsEmpty())));

    auto object_oid = &metastore::object_response::oid;
    auto get_res = m.get_first_ge(
                      model::topic_id_partition::from(tid_a), 5_o, 500_t)
                     .get();
    EXPECT_THAT(get_res, Optional(Field(object_oid, oid1)));
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(tid_a), 10_o, 500_t)
                .get();
    EXPECT_THAT(get_res, Optional(Field(object_oid, oid1)));
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(tid_a), 11_o, 500_t)
                .get();
    EXPECT_THAT(get_res, Optional(Field(object_oid, oid2)));
    get_res = m.get_first_ge(
                 model::topic_id_partition::from(tid_a), 21_o, 500_t)
                .get();
    EXPECT_EQ(
      get_res.error_or(metastore::errc::transport_error),
      metastore::errc::out_of_range);
}

TEST(StateUpdateTest, TestReplaceBasic) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());
    ASSERT_TRUE(add_res.value().corrected_next_offsets.empty());

    om_list_t new_os;
    new_os.emplace_back(
      om_builder(oid2, 100, 1100).add(tid_a, 0_o, 10_o, 2000_t, 0, 99).build());
    m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(
           os,
           terms_builder().add(tid_a, 0_tm, 0_o).add(tid_b, 0_tm, 0_o).build())
          .get();
    ASSERT_TRUE(add_res.has_value());

    om_list_t new_os;
    new_os.emplace_back(
      om_builder(oid3, 100, 1100).add(tid_a, 0_o, 20_o, 2000_t, 0, 99).build());
    m.preregister_objects(chunked_vector<object_id>::single(oid3));
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
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        m.preregister_objects(ids);
    }
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
    m.preregister_objects(chunked_vector<object_id>::single(oid3));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        m.preregister_objects(ids);
    }
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
        m.preregister_objects(chunked_vector<object_id>::single(oid3));
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
        m.preregister_objects(chunked_vector<object_id>::single(oid3));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{1});
        m.preregister_objects(chunked_vector<object_id>::single(oid3));
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{2});
        m.preregister_objects(chunked_vector<object_id>::single(oid4));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{1});
        m.preregister_objects(chunked_vector<object_id>::single(oid3));
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{2});
        m.preregister_objects(chunked_vector<object_id>::single(oid4));
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
    auto o_a = ob->get_or_create_object_for(tp_a).get().value();
    auto o_a_2 = ob->get_or_create_object_for(tp_a).get().value();
    ASSERT_EQ(o_a, o_a_2);

    // Creating objects for different partitions will result in the same
    // object.
    auto tp_b = model::topic_id_partition::from(tid_b);
    auto o_b = ob->get_or_create_object_for(tp_b).get().value();
    ASSERT_EQ(o_a, o_b);
    auto o_b_2 = ob->get_or_create_object_for(tp_b).get().value();
    ASSERT_EQ(o_b, o_b_2);

    // Add a partition's metadata to the object.
    ASSERT_TRUE(ob->add(o_a, {}).has_value());

    // Finish the current object. The next object will be different.
    ASSERT_TRUE(ob->finish(o_a, 0, 1000).has_value());

    auto o_a_3 = ob->get_or_create_object_for(tp_a).get().value();
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

TEST(SimpleMetastoreTest, TestObjectBuilderCreatesNewObjects) {
    simple_metastore m;
    auto ob = m.object_builder().get().value();
    auto tp = model::topic_id_partition::from(tid_a);

    chunked_hash_set<object_id> oids;
    static constexpr size_t num_objects = 1000;
    // Creating objects for the same partition will result in a different object
    // everytime.
    for (size_t i = 0; i < num_objects; ++i) {
        auto oid_opt = ob->create_object_for(tp).get();
        ASSERT_TRUE(oid_opt.has_value());
        auto [_, inserted] = oids.insert(oid_opt.value());
        ASSERT_TRUE(inserted);
    }
    ASSERT_EQ(oids.size(), num_objects);
}

TEST(SimpleMetastoreTest, TestObjectBuilderRejectsInvertedExtent) {
    simple_metastore m;
    auto ob = m.object_builder().get().value();
    auto tp = model::topic_id_partition::from(tid_a);
    auto oid = ob->create_object_for(tp).get().value();

    // base_offset > last_offset should be rejected.
    auto res = ob->add(
      oid,
      metastore::object_metadata::ntp_metadata{
        .tidp = tp,
        .base_offset = 10_o,
        .last_offset = 5_o,
        .max_timestamp = 1000_t,
        .pos = 0,
        .size = 100,
      });
    EXPECT_FALSE(res.has_value());

    // A valid extent should still be accepted.
    auto res2 = ob->add(
      oid,
      metastore::object_metadata::ntp_metadata{
        .tidp = tp,
        .base_offset = 0_o,
        .last_offset = 10_o,
        .max_timestamp = 1000_t,
        .pos = 0,
        .size = 100,
      });
    EXPECT_TRUE(res2.has_value());
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
          .get()
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
        auto o_a = ob->get_or_create_object_for(tp_a).get().value();
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
        auto o_a = ob->get_or_create_object_for(tp_a).get().value();
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
        auto o_a = ob->get_or_create_object_for(tp_a).get().value();
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
        auto o_a = ob->get_or_create_object_for(tp_a).get().value();
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
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
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
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
    auto to_collect = metastore::compaction_info_spec{
      .tidp = tp, .tombstone_removal_upper_bound_ts = 3000_t};
    auto cmp_after = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(cmp_after.has_value());
    EXPECT_THAT(
      cmp_after->offsets_response.dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(16_o, 20_o)));

    // Removable tombstone ranges should also be adjusted to reflect the new
    // start.
    EXPECT_THAT(
      cmp_after->offsets_response.removable_tombstone_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(10_o, 15_o)));

    // Assert that the new start offset is reported correctly in the compaction
    // info as well.
    ASSERT_EQ(cmp_after->start_offset, 10_o);
}

TEST(SimpleMetastoreTest, TestDirtyRatio) {
    simple_metastore m;
    om_list_t os;
    auto tp = model::topic_id_partition::from(tid_a);
    os.emplace_back(
      om_builder(oid1, 100, 10).add(tid_a, 0_o, 9_o, 1000_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid2, 100, 10).add(tid_a, 10_o, 19_o, 2000_t, 0, 99).build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        m.preregister_objects(ids);
    }
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
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
        m.preregister_objects(chunked_vector<object_id>::single(oid3));
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    auto to_collect = metastore::compaction_info_spec{
      .tidp = tp, .tombstone_removal_upper_bound_ts = 3000_t};
    auto compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 1.0);
    ASSERT_EQ(compaction_info->start_offset, 0_o);

    // Clean range is now [0, 9]. Only one extent still has dirty offsets.
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid4, 100, 10)
                              .add(tid_a, 0_o, 9_o, 1000_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 6_o, 9_o, 3000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{1});
        m.preregister_objects(chunked_vector<object_id>::single(oid4));
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.5);
    ASSERT_EQ(compaction_info->start_offset, 0_o);

    // Clean range is now [0, 19], the entire log is clean.
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid5, 100, 10)
                              .add(tid_a, 0_o, 19_o, 1000_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 10_o, 19_o, 3000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{2});
        m.preregister_objects(chunked_vector<object_id>::single(oid5));
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_FLOAT_EQ(compaction_info->dirty_ratio, 0.0);
    ASSERT_EQ(compaction_info->start_offset, 0_o);
}

TEST(SimpleMetastoreTest, TestCompactionOffsetsSingleDirtyAtEnd) {
    simple_metastore m;
    om_list_t os;
    auto tp = model::topic_id_partition::from(tid_a);

    // Create a log with offsets [0, 100] (next_offset = 101).
    os.emplace_back(om_builder(oid1, 100, 1010)
                      .add(tid_a, 0_o, 100_o, 1000_t, 0, 1009)
                      .build());
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    // Clean all offsets except the last one: clean [0, 99].
    // This leaves only offset 100 dirty.
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid2, 100, 1010)
                              .add(tid_a, 0_o, 100_o, 1000_t, 0, 1009)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 0_o, 99_o, 3000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    auto to_collect = metastore::compaction_info_spec{
      .tidp = tp, .tombstone_removal_upper_bound_ts = 3000_t};
    auto compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());

    // The dirty ratio should be non-zero
    EXPECT_GT(compaction_info->dirty_ratio, 0.0);

    // The dirty_ranges should contain [100, 100]
    EXPECT_THAT(
      compaction_info->offsets_response.dirty_ranges.to_vec(),
      testing::ElementsAre(MatchesRange(100_o, 100_o)));
}

TEST(
  SimpleMetastoreTest,
  TestEarliestDirtyTsNonMonotonicTimestampsCleanedInOrder) {
    simple_metastore m;
    om_list_t os;
    auto tp = model::topic_id_partition::from(tid_a);

    // Create three extents with non-monotonic timestamps:
    // - Extent 1: offsets [0-9], max_timestamp = 1000
    // - Extent 2: offsets [10-19], max_timestamp = 300
    // - Extent 3: offsets [20-29], max_timestamp = 500
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 9_o, 1000_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid2, 100, 1100).add(tid_a, 10_o, 19_o, 300_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid3, 100, 1100).add(tid_a, 20_o, 29_o, 500_t, 0, 99).build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    // Initially all extents are dirty. The earliest_dirty_ts should be the
    // minimum timestamp across all extents, which is 300.
    auto to_collect = metastore::compaction_info_spec{
      .tidp = tp, .tombstone_removal_upper_bound_ts = 3000_t};
    auto compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(compaction_info->earliest_dirty_ts.has_value());
    EXPECT_EQ(compaction_info->earliest_dirty_ts.value(), 300_t);

    // Clean the first extent [0-9]. Now only extents 2 and 3 are dirty.
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid4, 100, 1100)
                              .add(tid_a, 0_o, 9_o, 1000_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 0_o, 9_o, 3000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
        m.preregister_objects(chunked_vector<object_id>::single(oid4));
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(compaction_info->earliest_dirty_ts.has_value());
    EXPECT_EQ(compaction_info->earliest_dirty_ts.value(), 300_t);

    // Clean extent 2 [0-19].
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid5, 100, 1100)
                              .add(tid_a, 0_o, 19_o, 300_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 10_o, 19_o, 3000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{1});
        m.preregister_objects(chunked_vector<object_id>::single(oid5));
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(compaction_info->earliest_dirty_ts.has_value());
    EXPECT_EQ(compaction_info->earliest_dirty_ts.value(), 500_t);
}

TEST(
  SimpleMetastoreTest,
  TestEarliestDirtyTsNonMonotonicTimestampsCleanedOutOfOrder) {
    simple_metastore m;
    om_list_t os;
    auto tp = model::topic_id_partition::from(tid_a);

    // Create three extents with non-monotonic timestamps:
    // - Extent 1: offsets [0-9], max_timestamp = 1000
    // - Extent 2: offsets [10-19], max_timestamp = 500
    // - Extent 3: offsets [20-29], max_timestamp = 300
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 9_o, 1000_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid2, 100, 1100).add(tid_a, 10_o, 19_o, 500_t, 0, 99).build());
    os.emplace_back(
      om_builder(oid3, 100, 1100).add(tid_a, 20_o, 29_o, 300_t, 0, 99).build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    // Initially all extents are dirty. The earliest_dirty_ts should be the
    // minimum timestamp across all extents, which is 300.
    auto to_collect = metastore::compaction_info_spec{
      .tidp = tp, .tombstone_removal_upper_bound_ts = 3000_t};
    auto compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(compaction_info->earliest_dirty_ts.has_value());
    EXPECT_EQ(compaction_info->earliest_dirty_ts.value(), 300_t);

    // Clean the first extent [0-9]. Now only extents 2 and 3 are dirty.
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid4, 100, 1100)
                              .add(tid_a, 0_o, 9_o, 1000_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 0_o, 9_o, 3000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
        m.preregister_objects(chunked_vector<object_id>::single(oid4));
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(compaction_info->earliest_dirty_ts.has_value());
    EXPECT_EQ(compaction_info->earliest_dirty_ts.value(), 300_t);

    // Clean extent 3 [20-29].
    {
        om_list_t new_os;
        new_os.emplace_back(om_builder(oid5, 100, 1100)
                              .add(tid_a, 0_o, 29_o, 500_t, 0, 99)
                              .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 20_o, 29_o, 3000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{1});
        m.preregister_objects(chunked_vector<object_id>::single(oid5));
        auto compact_res = m.compact_objects(new_os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    compaction_info = m.get_compaction_info(to_collect).get();
    ASSERT_TRUE(compaction_info.has_value());
    ASSERT_TRUE(compaction_info->earliest_dirty_ts.has_value());
    EXPECT_EQ(compaction_info->earliest_dirty_ts.value(), 500_t);
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
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
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

TEST(SimpleMetastoreTest, TestCompactionMultipleDirtyRangesMadeClean) {
    simple_metastore m;
    {
        om_list_t os;
        os.emplace_back(om_builder(oid1, 100, 1100)
                          .add(tid_a, 0_o, 20_o, 2000_t, 0, 99)
                          .build());
        m.preregister_objects(chunked_vector<object_id>::single(oid1));
        auto add_res = m.add_objects(
                          os, terms_builder().add(tid_a, 0_tm, 0_o).build())
                         .get();
        ASSERT_TRUE(add_res.has_value());
    }

    // Clean range is [5, 15].
    {
        om_list_t os;
        os.emplace_back(om_builder(oid2, 100, 1100)
                          .add(tid_a, 0_o, 20_o, 2000_t, 0, 99)
                          .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 5_o, 15_o, 3000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{0});
        m.preregister_objects(chunked_vector<object_id>::single(oid2));
        auto compact_res = m.compact_objects(os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    auto tp = model::topic_id_partition::from(tid_a);

    {
        // Verify compaction state and dirty ranges: [0, 4] and [16, 20].
        auto cmp = m.get_compaction_offsets(tp, 3000_t).get();
        ASSERT_TRUE(cmp.has_value());
        EXPECT_THAT(
          cmp->dirty_ranges.to_vec(),
          testing::ElementsAre(
            MatchesRange(0_o, 4_o), MatchesRange(16_o, 20_o)));
        EXPECT_THAT(
          cmp->removable_tombstone_ranges.to_vec(),
          testing::ElementsAre(MatchesRange(5_o, 15_o)));
    }

    // Clean range is [5, 15]. Try to make both of [[0, 4], [16, 20]] clean.
    {
        om_list_t os;
        os.emplace_back(om_builder(oid3, 100, 1100)
                          .add(tid_a, 0_o, 20_o, 2000_t, 0, 99)
                          .build());

        auto cmb = cm_builder();
        cmb.clean(tid_a, 0_o, 4_o, 6000_t);
        cmb.clean(tid_a, 16_o, 20_o, 6000_t);
        cmb.set_expected_epoch(tid_a, metastore::compaction_epoch{1});
        m.preregister_objects(chunked_vector<object_id>::single(oid3));
        auto compact_res = m.compact_objects(os, cmb.build()).get();
        ASSERT_TRUE(compact_res.has_value());
    }

    {
        // Verify compaction state (no dirty ranges).
        auto cmp_before = m.get_compaction_offsets(tp, 6000_t).get();
        ASSERT_TRUE(cmp_before.has_value());
        EXPECT_THAT(cmp_before->dirty_ranges.to_vec(), testing::IsEmpty());
        EXPECT_THAT(
          cmp_before->removable_tombstone_ranges.to_vec(),
          testing::ElementsAre(MatchesRange(0_o, 20_o)));
    }
}

TEST(SimpleMetastoreTest, TestGetExtentMetadataForwards) {
    simple_metastore m;
    om_list_t os;
    constexpr size_t data_size = 99;
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 9_o, 2000_t, 0, data_size)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 10_o, 19_o, 2000_t, 0, data_size)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 20_o, 29_o, 2000_t, 0, data_size)
                      .build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tid_a);

    // A few basic test cases with an expanding `max_offset`.
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{9};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      10,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{15};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      10,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o), MatchesRange(10_o, 19_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{19};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      10,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o), MatchesRange(10_o, 19_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{20};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      10,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(20_o, 29_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{100};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      10,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(20_o, 29_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }

    // A few test cases where the number of extents is limited.
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{9};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      0,
                                      metastore::include_object_metadata::no)
                                     .get();
        // Requesting 0 extents still results in a single extent being returned.
        ASSERT_TRUE(extent_metadata_res.has_value());
        ASSERT_EQ(extent_metadata_res->extents.size(), 1);
        EXPECT_FALSE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{15};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      1,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(MatchesRange(0_o, 9_o)));
        EXPECT_FALSE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{20};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      2,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o), MatchesRange(10_o, 19_o)));
        EXPECT_FALSE(extent_metadata_res->end_of_stream);
    }

    // Non zero min_offset test cases.
    {
        auto min_offset = kafka::offset{5};
        auto max_offset = kafka::offset{9};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      10,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{9};
        auto max_offset = kafka::offset{29};
        auto extent_metadata_res = m.get_extent_metadata_forwards(
                                      tp,
                                      min_offset,
                                      max_offset,
                                      10,
                                      metastore::include_object_metadata::no)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(20_o, 29_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
}

TEST(SimpleMetastoreTest, TestGetExtentMetadataBackwards) {
    simple_metastore m;
    om_list_t os;
    constexpr size_t data_size = 99;
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 9_o, 2000_t, 0, data_size)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 10_o, 19_o, 2000_t, 0, data_size)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 20_o, 29_o, 2000_t, 0, data_size)
                      .build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tid_a);

    // A few basic test cases with an expanding `max_offset`.
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{9};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 10)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{15};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 10)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(10_o, 19_o), MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{19};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 10)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(10_o, 19_o), MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{20};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 10)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(20_o, 29_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{100};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 10)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(20_o, 29_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }

    // A few test cases where the number of extents is limited.
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{9};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 0)
                                     .get();
        // Requesting 0 extents still results in a single extent being returned.
        ASSERT_TRUE(extent_metadata_res.has_value());
        ASSERT_EQ(extent_metadata_res->extents.size(), 1);
        EXPECT_FALSE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{15};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 1)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(MatchesRange(10_o, 19_o)));
        EXPECT_FALSE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{21};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 1)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(MatchesRange(20_o, 29_o)));
        EXPECT_FALSE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{20};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 2)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(20_o, 29_o), MatchesRange(10_o, 19_o)));
        EXPECT_FALSE(extent_metadata_res->end_of_stream);
    }

    // Non zero min_offset test cases.
    {
        auto min_offset = kafka::offset{5};
        auto max_offset = kafka::offset{9};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 10)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{9};
        auto max_offset = kafka::offset{29};
        auto extent_metadata_res = m.get_extent_metadata_backwards(
                                      tp, min_offset, max_offset, 10)
                                     .get();
        ASSERT_TRUE(extent_metadata_res.has_value());
        EXPECT_THAT(
          extent_metadata_res->extents,
          testing::ElementsAre(
            MatchesRange(20_o, 29_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(0_o, 9_o)));
        EXPECT_TRUE(extent_metadata_res->end_of_stream);
    }
}

TEST(SimpleMetastoreTest, TestGetExtentMetadataEmpty) {
    simple_metastore m;
    om_list_t os;
    constexpr size_t data_size = 99;
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 9_o, 2000_t, 0, data_size)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 10_o, 19_o, 2000_t, 0, data_size)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 20_o, 29_o, 2000_t, 0, data_size)
                      .build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tid_a);

    auto set_start_res = m.set_start_offset(tp, 30_o).get();
    ASSERT_TRUE(set_start_res.has_value());

    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{100};
        auto extent_metadata_ge_res = m.get_extent_metadata_forwards(
                                         tp,
                                         min_offset,
                                         max_offset,
                                         10,
                                         metastore::include_object_metadata::no)
                                        .get();
        ASSERT_TRUE(extent_metadata_ge_res.has_value());
        ASSERT_TRUE(extent_metadata_ge_res->extents.empty());
        EXPECT_TRUE(extent_metadata_ge_res->end_of_stream);
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{100};
        auto extent_metadata_le_res = m.get_extent_metadata_backwards(
                                         tp, min_offset, max_offset, 10)
                                        .get();
        ASSERT_TRUE(extent_metadata_le_res.has_value());
        ASSERT_TRUE(extent_metadata_le_res->extents.empty());
        EXPECT_TRUE(extent_metadata_le_res->end_of_stream);
    }
}

TEST(SimpleMetastoreTest, TestGetSizeMissingPartition) {
    simple_metastore m;
    auto size_res = m.get_size(model::topic_id_partition::from(tid_c)).get();
    ASSERT_FALSE(size_res.has_value());
    ASSERT_EQ(metastore::errc::missing_ntp, size_res.error());
}

TEST(SimpleMetastoreTest, TestGetSizeBasic) {
    simple_metastore m;
    constexpr size_t data_size_1 = 100;
    constexpr size_t data_size_2 = 200;
    constexpr size_t data_size_3 = 300;

    om_list_t os;
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 9_o, 2000_t, 0, data_size_1)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 10_o, 19_o, 2000_t, 0, data_size_2)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 20_o, 29_o, 2000_t, 0, data_size_3)
                      .build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tid_a);
    auto size_res = m.get_size(tp).get();
    ASSERT_TRUE(size_res.has_value());
    ASSERT_EQ(data_size_1 + data_size_2 + data_size_3, size_res->size);
}

TEST(SimpleMetastoreTest, TestGetSizeAfterSetStartOffset) {
    simple_metastore m;
    constexpr size_t data_size_1 = 100;
    constexpr size_t data_size_2 = 200;
    constexpr size_t data_size_3 = 300;

    om_list_t os;
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 9_o, 2000_t, 0, data_size_1)
                      .build());
    os.emplace_back(om_builder(oid2, 100, 1100)
                      .add(tid_a, 10_o, 19_o, 2000_t, 0, data_size_2)
                      .build());
    os.emplace_back(om_builder(oid3, 100, 1100)
                      .add(tid_a, 20_o, 29_o, 2000_t, 0, data_size_3)
                      .build());
    {
        chunked_vector<object_id> ids;
        ids.push_back(oid1);
        ids.push_back(oid2);
        ids.push_back(oid3);
        m.preregister_objects(ids);
    }
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tid_a);

    // Initial size should be sum of all extents.
    auto size_res = m.get_size(tp).get();
    ASSERT_TRUE(size_res.has_value());
    ASSERT_EQ(data_size_1 + data_size_2 + data_size_3, size_res->size);

    // Set start offset to remove the first extent.
    auto set_start_res = m.set_start_offset(tp, 10_o).get();
    ASSERT_TRUE(set_start_res.has_value());

    // Size should now exclude the first extent.
    size_res = m.get_size(tp).get();
    ASSERT_TRUE(size_res.has_value());
    ASSERT_EQ(data_size_2 + data_size_3, size_res->size);

    // Set start offset to remove the second extent as well.
    set_start_res = m.set_start_offset(tp, 20_o).get();
    ASSERT_TRUE(set_start_res.has_value());

    // Size should now only include the third extent.
    size_res = m.get_size(tp).get();
    ASSERT_TRUE(size_res.has_value());
    ASSERT_EQ(data_size_3, size_res->size);

    // Set start offset to remove all extents.
    set_start_res = m.set_start_offset(tp, 30_o).get();
    ASSERT_TRUE(set_start_res.has_value());

    // Size should now be zero.
    size_res = m.get_size(tp).get();
    ASSERT_TRUE(size_res.has_value());
    ASSERT_EQ(0, size_res->size);
}

TEST(SimpleMetastoreTest, TestGetSizeMultiplePartitions) {
    simple_metastore m;
    constexpr size_t data_size_a = 100;
    constexpr size_t data_size_b = 250;

    om_list_t os;
    os.emplace_back(om_builder(oid1, 100, 1100)
                      .add(tid_a, 0_o, 9_o, 2000_t, 0, data_size_a)
                      .add(tid_b, 0_o, 9_o, 2000_t, 100, 100 + data_size_b)
                      .build());
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
    auto add_res
      = m.add_objects(
           os,
           terms_builder().add(tid_a, 0_tm, 0_o).add(tid_b, 0_tm, 0_o).build())
          .get();
    ASSERT_TRUE(add_res.has_value());

    // Each partition should report its own size.
    auto tp_a = model::topic_id_partition::from(tid_a);
    auto size_res_a = m.get_size(tp_a).get();
    ASSERT_TRUE(size_res_a.has_value());
    ASSERT_EQ(data_size_a, size_res_a->size);

    auto tp_b = model::topic_id_partition::from(tid_b);
    auto size_res_b = m.get_size(tp_b).get();
    ASSERT_TRUE(size_res_b.has_value());
    ASSERT_EQ(data_size_b, size_res_b->size);
}

TEST(SimpleMetastoreTest, TestGetExtentMetadataForwardsWithObjectMetadata) {
    simple_metastore m;
    om_list_t os;
    os.emplace_back(
      om_builder(oid1, 100, 1100).add(tid_a, 0_o, 9_o, 2000_t, 0, 500).build());
    os.emplace_back(om_builder(oid2, 200, 2200)
                      .add(tid_a, 10_o, 19_o, 3000_t, 0, 500)
                      .build());
    os.emplace_back(om_builder(oid3, 300, 3300)
                      .add(tid_a, 20_o, 29_o, 4000_t, 0, 500)
                      .build());
    m.preregister_objects(chunked_vector<object_id>::single(oid1));
    m.preregister_objects(chunked_vector<object_id>::single(oid2));
    m.preregister_objects(chunked_vector<object_id>::single(oid3));
    auto add_res
      = m.add_objects(os, terms_builder().add(tid_a, 0_tm, 0_o).build()).get();
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tid_a);

    // Without include_object_metadata, object_info should be nullopt.
    {
        auto res
          = m.get_extent_metadata_forwards(
               tp, 0_o, 100_o, 10, metastore::include_object_metadata::no)
              .get();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res->extents.size(), 3);
        EXPECT_FALSE(res->extents[0].object_info.has_value());
        EXPECT_FALSE(res->extents[1].object_info.has_value());
        EXPECT_FALSE(res->extents[2].object_info.has_value());
    }

    // With include_object_metadata, object_info should be populated from
    // the object store.
    {
        auto res
          = m.get_extent_metadata_forwards(
               tp, 0_o, 100_o, 10, metastore::include_object_metadata::yes)
              .get();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res->extents.size(), 3);

        EXPECT_EQ(res->extents[0].base_offset, 0_o);
        EXPECT_EQ(res->extents[0].last_offset, 9_o);
        ASSERT_TRUE(res->extents[0].object_info.has_value());
        EXPECT_EQ(res->extents[0].object_info->oid, oid1);
        EXPECT_EQ(res->extents[0].object_info->footer_pos, 100);
        EXPECT_EQ(res->extents[0].object_info->object_size, 1100);

        EXPECT_EQ(res->extents[1].base_offset, 10_o);
        EXPECT_EQ(res->extents[1].last_offset, 19_o);
        ASSERT_TRUE(res->extents[1].object_info.has_value());
        EXPECT_EQ(res->extents[1].object_info->oid, oid2);
        EXPECT_EQ(res->extents[1].object_info->footer_pos, 200);
        EXPECT_EQ(res->extents[1].object_info->object_size, 2200);

        EXPECT_EQ(res->extents[2].base_offset, 20_o);
        EXPECT_EQ(res->extents[2].last_offset, 29_o);
        ASSERT_TRUE(res->extents[2].object_info.has_value());
        EXPECT_EQ(res->extents[2].object_info->oid, oid3);
        EXPECT_EQ(res->extents[2].object_info->footer_pos, 300);
        EXPECT_EQ(res->extents[2].object_info->object_size, 3300);

        EXPECT_TRUE(res->end_of_stream);
    }

    // With max_num_extents limit.
    {
        auto res
          = m.get_extent_metadata_forwards(
               tp, 0_o, 100_o, 2, metastore::include_object_metadata::yes)
              .get();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res->extents.size(), 2);
        ASSERT_TRUE(res->extents[0].object_info.has_value());
        EXPECT_EQ(res->extents[0].object_info->oid, oid1);
        ASSERT_TRUE(res->extents[1].object_info.has_value());
        EXPECT_EQ(res->extents[1].object_info->oid, oid2);
        EXPECT_FALSE(res->end_of_stream);
    }
}
