/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/units.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

#include <seastar/core/chunked_fifo.hh>

#include <gtest/gtest.h>

#include <cmath>
#include <initializer_list>
#include <utility>

namespace model {

namespace {
model::transform_metadata make_transform_meta() {
    return model::transform_metadata{
      .name = tests::random_named_string<model::transform_name>(),
      .input_topic = model::random_topic_namespace(),
      .output_topics = {model::random_topic_namespace()},
      .uuid = uuid_t::create(),
      .source_ptr = model::random_offset()};
}

model::transform_report::processor make_processor_report(int id) {
    using state = model::transform_report::processor::state;
    return model::transform_report::processor{
      .id = model::partition_id(id),
      .status = random_generators::random_choice({
        state::unknown,
        state::inactive,
        state::running,
        state::errored,
      }),
      .node = tests::random_named_int<model::node_id>(),
    };
}

} // namespace

TEST(TransformReportTest, AddProcessors) {
    auto meta = make_transform_meta();
    auto p_one = make_processor_report(1);
    auto p_two = make_processor_report(2);
    auto p_three = make_processor_report(3);
    model::transform_report report(meta);
    report.add(p_one);
    report.add(p_two);
    report.add(p_three);
    model::transform_report expected(
      meta,
      {
        {model::partition_id(1), p_one},
        {model::partition_id(2), p_two},
        {model::partition_id(3), p_three},
      });
    EXPECT_EQ(report, expected);
}

TEST(ClusterTransformReportTest, AddIndividualReports) {
    using tid = model::transform_id;
    using pid = model::partition_id;
    auto meta_one = make_transform_meta();
    auto meta_two = make_transform_meta();
    auto p_one = make_processor_report(1);
    auto p_two = make_processor_report(2);
    auto p_three = make_processor_report(3);
    model::cluster_transform_report actual;
    for (const auto& p : {p_one, p_two, p_three}) {
        actual.add(model::transform_id(1), meta_one, p);
        actual.add(model::transform_id(2), meta_two, p);
    }
    model::cluster_transform_report expected;
    expected.transforms.emplace(
      tid(1),
      model::transform_report(
        meta_one,
        {
          {pid(1), p_one},
          {pid(2), p_two},
          {pid(3), p_three},
        }));
    expected.transforms.emplace(
      tid(2),
      model::transform_report(
        meta_two,
        {
          {pid(1), p_one},
          {pid(2), p_two},
          {pid(3), p_three},
        }));
    EXPECT_EQ(actual, expected);
}

TEST(ClusterTransformReportTest, Merge) {
    using tid = model::transform_id;
    auto meta_one = make_transform_meta();
    auto meta_two = make_transform_meta();
    auto p_one = make_processor_report(1);
    auto p_two = make_processor_report(2);
    auto p_three = make_processor_report(3);

    model::cluster_transform_report node_one;
    node_one.add(tid(1), meta_one, p_one);
    node_one.add(tid(2), meta_two, p_two);
    node_one.add(tid(1), meta_one, p_three);
    model::cluster_transform_report node_two;
    node_two.add(tid(2), meta_two, p_one);
    node_two.add(tid(1), meta_one, p_two);
    node_two.add(tid(2), meta_two, p_three);

    model::cluster_transform_report expected;
    expected.add(tid(1), meta_one, p_one);
    expected.add(tid(1), meta_one, p_two);
    expected.add(tid(1), meta_one, p_three);
    expected.add(tid(2), meta_two, p_one);
    expected.add(tid(2), meta_two, p_two);
    expected.add(tid(2), meta_two, p_three);

    node_one.merge(node_two);

    EXPECT_EQ(node_one, expected);
}

TEST(TransformedDataTest, Serialize) {
    auto src = model::test::make_random_record(
      0, random_generators::make_iobuf());
    auto validated = transformed_data::from_record(src.copy());
    auto got = std::move(validated).to_serialized_record(
      src.attributes(), src.timestamp_delta(), src.offset_delta());
    iobuf want;
    model::append_record_to_buffer(want, src);
    EXPECT_EQ(got, want) << "GOT:\n"
                         << got.hexdump(1_KiB) << "\n\nWANT:\n"
                         << want.hexdump(1_KiB);
}

TEST(TransformedDataTest, MakeBatch) {
    auto batch = test::make_random_batch({
      .allow_compression = false,
      .count = 4,
    });
    ss::chunked_fifo<transformed_data> transformed;
    for (const auto& r : batch.copy_records()) {
        transformed.push_back(transformed_data::from_record(r.copy()));
    }
    auto now = model::timestamp::now();
    auto transformed_batch = transformed_data::make_batch(
      now, std::move(transformed));
    EXPECT_EQ(transformed_batch.header().first_timestamp, now);
    EXPECT_EQ(transformed_batch.header().max_timestamp, now);
    EXPECT_EQ(transformed_batch.header().producer_id, -1);
    EXPECT_EQ(
      transformed_batch.header().type, model::record_batch_type::raft_data);
    EXPECT_EQ(transformed_batch.header().record_count, 4);
    EXPECT_EQ(transformed_batch.header().last_offset_delta, 3);
    EXPECT_EQ(
      transformed_batch.header().crc,
      model::crc_record_batch(transformed_batch));
    EXPECT_EQ(
      transformed_batch.header().header_crc,
      model::internal_header_only_crc(transformed_batch.header()));
    EXPECT_EQ(
      transformed_batch.header().size_bytes, transformed_batch.size_bytes());
    auto expected_records = batch.copy_records();
    auto actual_records = transformed_batch.copy_records();
    for (auto i = 0; i < expected_records.size(); ++i) {
        EXPECT_EQ(actual_records[i].key(), expected_records[i].key());
        EXPECT_EQ(actual_records[i].value(), expected_records[i].value());
        EXPECT_EQ(actual_records[i].headers(), expected_records[i].headers());
        EXPECT_EQ(
          actual_records[i].offset_delta(), expected_records[i].offset_delta());
        // Timestamps are different than what the test helper makes and that's
        // OK.
        EXPECT_EQ(actual_records[i].timestamp_delta(), 0);
    }
}

} // namespace model
