/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/level_one/compaction/tests/in_memory_sink.h"
#include "compaction/reducer.h"
#include "compaction/tests/simple_reducer.h"
#include "container/chunked_circular_buffer.h"
#include "container/chunked_vector.h"
#include "model/record.h"
#include "storage/tests/batch_generators.h"

#include <gtest/gtest.h>

#include <variant>

static const auto test_ntp = model::ntp(
  model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));
static const auto test_tidp = model::topic_id_partition(
  model::topic_id(uuid_t::create()), test_ntp.tp.partition);

TEST(ReducerTest, Batches) {
    namespace l1 = cloud_topics::l1;
    int num_batches = 10;
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false, .count = 1};
    auto input_batches = gen(spec, num_batches);

    auto src = std::make_unique<compaction::simple_source>(
      std::move(input_batches), test_ntp);
    chunked_vector<l1::in_memory_sink::object_output_t> output_objs;
    auto sink = std::make_unique<l1::in_memory_sink>(test_tidp, &output_objs);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    std::move(reducer).run().get();

    ASSERT_EQ(output_objs.size(), 1);
    auto& [info, object] = output_objs.front();
    auto rdr = l1::object_reader::create(
      make_iobuf_input_stream(std::move(object)));

    chunked_circular_buffer<model::record_batch> output_batches;
    while (true) {
        l1::object_reader::result res = rdr->read_next().get();
        if (std::holds_alternative<model::record_batch>(res)) {
            output_batches.push_back(
              std::move(std::get<model::record_batch>(res)));
        }
        if (std::holds_alternative<l1::object_reader::eof>(res)) {
            break;
        }
    }

    ASSERT_EQ(output_batches.size(), num_batches);
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));
}
