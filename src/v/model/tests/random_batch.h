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

#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timestamp.h"
#include "random/generators.h"

namespace model::test {
using namespace random_generators; // NOLINT

struct record_batch_spec {
    model::offset offset{0};
    bool allow_compression{true};
    int count{0};
    std::optional<int> records{std::nullopt};
    std::optional<int> max_key_cardinality{std::nullopt};

    model::record_batch_type bt{model::record_batch_type::raft_data};
    bool enable_idempotence{false};
    int64_t producer_id{-1};
    int16_t producer_epoch{-1};
    int32_t base_sequence{-1};
    bool is_transactional{false};
    std::optional<std::vector<size_t>> record_sizes;
    std::optional<model::timestamp> timestamp;
    bool all_records_have_same_timestamp{false};
};

model::record make_random_record(int, iobuf);

/**
 * Makes random batch starting at requested offset.
 *
 * Note: it can create batches with timestamps from the past.
 */
model::record_batch make_random_batch(
  model::offset o,
  int num_records,
  bool allow_compression,
  model::record_batch_type bt,
  std::optional<std::vector<size_t>> record_sizes = std::nullopt,
  std::optional<model::timestamp> ts = std::nullopt);

model::record_batch
make_random_batch(model::offset o, int num_records, bool allow_compression);

model::record_batch make_random_batch(record_batch_spec);

model::record_batch make_random_batch(
  model::offset o,
  bool allow_compression = true,
  std::optional<model::timestamp> ts = std::nullopt);

ss::future<ss::circular_buffer<model::record_batch>> make_random_batches(
  model::offset o,
  int count,
  bool allow_compression = true,
  std::optional<model::timestamp> ts = std::nullopt);

ss::future<ss::circular_buffer<model::record_batch>>
make_random_batches(model::offset o = model::offset(0));

ss::future<ss::circular_buffer<model::record_batch>>
make_random_batches(record_batch_spec spec);
} // namespace model::test
