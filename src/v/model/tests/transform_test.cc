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

#include "model/fundamental.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/transform.h"
#include "random/generators.h"
#include "test_utils/randoms.h"
#include "units.h"

#include <gtest/gtest.h>

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

namespace {
void append_vint_to_iobuf(iobuf& b, int64_t v) {
    auto vb = vint::to_bytes(v);
    b.append(vb.data(), vb.size());
}
} // namespace

TEST(TransformedDataTest, Serialize) {
    auto src = model::test::make_random_record(
      0, random_generators::make_iobuf());
    iobuf payload;
    append_vint_to_iobuf(payload, src.key_size());
    payload.append(src.key().copy());
    append_vint_to_iobuf(payload, src.value_size());
    payload.append(src.value().copy());
    append_vint_to_iobuf(payload, int64_t(src.headers().size()));
    for (const auto& header : src.headers()) {
        append_vint_to_iobuf(payload, header.key_size());
        payload.append(header.key().copy());
        append_vint_to_iobuf(payload, header.value_size());
        payload.append(header.value().copy());
    }
    auto validated = model::transformed_data::create_validated(
      std::move(payload));
    ASSERT_TRUE(validated.has_value());
    auto got = std::move(validated.value())
                 .to_serialized_record(
                   src.attributes(), src.timestamp_delta(), src.offset_delta());
    iobuf want;
    model::append_record_to_buffer(want, src);
    EXPECT_EQ(got, want) << "GOT:\n"
                         << got.hexdump(1_KiB) << "\n\nWANT:\n"
                         << want.hexdump(1_KiB);
}

} // namespace model
