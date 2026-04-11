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
#include "cloud_topics/level_one/metastore/extent_metadata_reader.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/level_one/metastore/tests/builders.h"
#include "gmock/gmock.h"
#include "test_utils/test_macros.h"

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace cloud_topics::l1;
using namespace cloud_topics::l1::test_utils;

using iter_dir = extent_metadata_reader::iteration_direction;

namespace {

const object_id oid1 = l1::create_object_id();
const object_id oid2 = l1::create_object_id();
const object_id oid3 = l1::create_object_id();

const std::string_view tid_a = "deadbeef-aaaa-0000-0000-000000000000/0";

static ss::abort_source as;

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

ss::future<metastore::extent_metadata_vec>
consume_reader_to_extent_metadata_vec(extent_metadata_reader rdr) {
    metastore::extent_metadata_vec vec;
    auto gen = rdr.generator();
    while (auto extent_res_opt = co_await gen()) {
        auto& extent_res = extent_res_opt->get();
        auto extent_md = std::move(extent_res).value();
        vec.push_back(std::move(extent_md));
    }
    co_return vec;
}

} // namespace

TEST(SimpleMetastoreTest, TestReadExtentMetadataForwards) {
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
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(read_extents, testing::ElementsAre(MatchesRange(0_o, 9_o)));
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{15};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o), MatchesRange(10_o, 19_o)));
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{19};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o), MatchesRange(10_o, 19_o)));
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{20};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(20_o, 29_o)));
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{100};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(20_o, 29_o)));
    }

    // Non zero min_offset test cases.
    {
        auto min_offset = kafka::offset{5};
        auto max_offset = kafka::offset{9};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(read_extents, testing::ElementsAre(MatchesRange(0_o, 9_o)));
    }
    {
        auto min_offset = kafka::offset{9};
        auto max_offset = kafka::offset{29};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(20_o, 29_o)));
    }

    // Edge case when returning one extent (max_offset is on the boundary of an
    // extent).
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{10};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as, 1);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(0_o, 9_o), MatchesRange(10_o, 19_o)));
    }
}

TEST(SimpleMetastoreTest, TestReadExtentMetadataBackwards) {
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
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(read_extents, testing::ElementsAre(MatchesRange(0_o, 9_o)));
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{15};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(10_o, 19_o), MatchesRange(0_o, 9_o)));
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{19};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(10_o, 19_o), MatchesRange(0_o, 9_o)));
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{20};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(20_o, 29_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(0_o, 9_o)));
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{100};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(20_o, 29_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(0_o, 9_o)));
    }

    // Non zero min_offset test cases.
    {
        auto min_offset = kafka::offset{5};
        auto max_offset = kafka::offset{9};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(read_extents, testing::ElementsAre(MatchesRange(0_o, 9_o)));
    }
    {
        auto min_offset = kafka::offset{9};
        auto max_offset = kafka::offset{29};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(20_o, 29_o),
            MatchesRange(10_o, 19_o),
            MatchesRange(0_o, 9_o)));
    }

    // Edge case when returning one extent (min_offset is on the boundary of an
    // extent).
    {
        auto min_offset = kafka::offset{9};
        auto max_offset = kafka::offset{15};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as, 1);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        EXPECT_THAT(
          read_extents,
          testing::ElementsAre(
            MatchesRange(10_o, 19_o), MatchesRange(0_o, 9_o)));
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
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::forwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        ASSERT_TRUE(read_extents.empty());
    }
    {
        auto min_offset = kafka::offset{0};
        auto max_offset = kafka::offset{100};
        auto rdr = extent_metadata_reader(
          &m, tp, min_offset, max_offset, iter_dir::backwards, as);
        auto read_extents
          = consume_reader_to_extent_metadata_vec(std::move(rdr)).get();
        ASSERT_TRUE(read_extents.empty());
    }
}
