/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/utils/batch_utils.h"

#include "model/tests/random_batch.h"
#include "random/generators.h"

#include <numeric>

std::size_t num_records(const model::record_batch_reader::data_t& data) {
    return std::accumulate(
      data.begin(),
      data.end(),
      std::size_t{0},
      [](std::size_t acc, const model::record_batch& rb) {
          return acc + rb.record_count();
      });
}

model::record_batch_reader make_random_batch(std::size_t n_records) {
    /// Divide the number of records into a random number of batches to increase
    /// the level of chaos in wasm tests.
    auto n_batches = std::gcd(
      n_records, random_generators::get_int(std::size_t{1}, n_records));
    auto recs_per_batch = n_records / n_batches;
    return model::test::make_random_memory_record_batch_reader(
      model::test::record_batch_spec{
        .offset = model::offset{0},
        .allow_compression = false,
        .count = static_cast<int>(n_batches),
        .records = static_cast<int>(recs_per_batch)},
      1);
}

model::record_batch_reader::data_t
copy_batch(const model::record_batch_reader::data_t& data) {
    model::record_batch_reader::data_t new_batch;
    new_batch.reserve(data.size());
    std::transform(
      data.cbegin(),
      data.cend(),
      std::back_inserter(new_batch),
      [](const model::record_batch& rb) { return rb.copy(); });
    return new_batch;
}
