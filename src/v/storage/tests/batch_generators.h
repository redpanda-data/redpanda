/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "compression/compression.h"
#include "container/chunked_circular_buffer.h"
#include "hashing/crc32c.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "test_utils/test_macros.h"
#include "utils/vint.h"

#include <seastar/core/reactor.hh>

#include <optional>

struct random_batches_generator {
    chunked_circular_buffer<model::record_batch>
    operator()(std::optional<model::timestamp> base_ts = std::nullopt) {
        return model::test::make_random_batches(
                 model::offset(0),
                 random_generators::get_int(1, 10),
                 true,
                 base_ts)
          .get();
    }
};

struct key_limited_random_batch_generator {
    static constexpr int cardinality = 10;

    chunked_circular_buffer<model::record_batch>
    operator()(std::optional<model::timestamp> ts = std::nullopt) {
        return model::test::make_random_batches(
                 model::test::record_batch_spec{
                   .allow_compression = true,
                   .count = random_generators::get_int(1, 10),
                   .max_key_cardinality = cardinality,
                   .bt = model::record_batch_type::raft_data,
                   .timestamp = ts})
          .get();
    }
};

// Deterministic data generator that generates integer keys linearly with
// duplicates in each batch. Each batch contains a new integer key. With small
// enough batch sizes (not exceeding compaction budget), compaction should
// dedupe all the keys within a batch and the resulting log file should contain
// a single key per batch forming a sequence of integers. A handy validate()
// method is provided that validates the the batch state after compaction.
struct linear_int_kv_batch_generator {
    int _idx = 0;

    static constexpr int batches_per_call = 5;
    static constexpr int records_per_batch = 5;

    model::record_batch
    make_batch(model::test::record_batch_spec spec, int idx) {
        auto ts = spec.timestamp.value_or(
          model::timestamp(model::timestamp::now()() - (spec.count - 1)));
        auto max_ts = spec.timestamp.value_or(
          model::timestamp(ts.value() + spec.count - 1));
        auto header = model::record_batch_header{
          .size_bytes = 0, // computed later
          .base_offset = spec.offset + model::offset_delta(idx * spec.count),
          .type = spec.bt,
          .crc = 0, // we-reassign later
          .attrs = {},
          .last_offset_delta = spec.count - 1,
          .first_timestamp = ts,
          .max_timestamp = max_ts,
          .producer_id = spec.producer_id,
          .producer_epoch = spec.producer_epoch,
          .base_sequence = 0,
          .record_count = spec.count};

        if (spec.is_transactional) {
            header.attrs.set_transactional_type();
        }

        if (spec.enable_idempotence) {
            header.base_sequence = spec.base_sequence;
        }

        auto rs = model::record_batch::uncompressed_records();
        rs.reserve(spec.count);

        for (int i = 0; i < spec.count; i++) {
            rs.emplace_back(
              model::test::make_random_record(i, reflection::to_iobuf(idx)));
        }

        iobuf body;
        for (auto& r : rs) {
            model::append_record_to_buffer(body, r);
        }
        rs.clear();
        // TODO: expose term setting
        header.ctx = model::record_batch_header::context(
          model::term_id(0), ss::this_shard_id());
        header.size_bytes = static_cast<int32_t>(
          model::packed_record_batch_header_size + body.size_bytes());
        auto batch = model::record_batch(
          header, std::move(body), model::record_batch::tag_ctor_ng{});
        auto compression = static_cast<model::compression>(
          random_generators::get_int<uint8_t>(
            0, spec.allow_compression ? 4 : 0));
        if (compression == model::compression::none) {
            batch.header().reset_size_checksum_metadata(batch.data());
            return batch;
        }
        return model::compress_batch_sync(compression, std::move(batch));
    }

    chunked_circular_buffer<model::record_batch>
    operator()(std::optional<model::timestamp> ts = std::nullopt) {
        chunked_circular_buffer<model::record_batch> ret;
        auto batch_spec = model::test::record_batch_spec{
          .allow_compression = false,
          .count = records_per_batch,
          .bt = model::record_batch_type::raft_data,
          .timestamp = ts,
        };
        return operator()(batch_spec, batches_per_call);
    }

    chunked_circular_buffer<model::record_batch>
    operator()(model::test::record_batch_spec spec, int num_batches) {
        chunked_circular_buffer<model::record_batch> ret;
        for (int i = 0; i < num_batches; i++) {
            ret.push_back(make_batch(spec, _idx++));
        }
        return ret;
    }

    // Batches generated by this generator should all have 1 record
    // and the record should match the index of the batch if compaction
    // ran correctly.
    static void validate_post_compaction(
      chunked_circular_buffer<model::record_batch>&& batches) {
        int idx = 0;
        for (auto& batch : batches) {
            RPTEST_EXPECT_EQ(batch.record_count(), 1);
            if (batch.compressed()) {
                batch = model::decompress_batch_sync(batch);
            }
            batch.for_each_record([&idx](model::record rec) {
                RPTEST_EXPECT_EQ(
                  reflection::from_iobuf<int>(rec.release_key()), idx++);
            });
        }
    }
};
