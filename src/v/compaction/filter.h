// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/reducer.h"
#include "compaction/types.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <optional>
#include <vector>

namespace compaction {

// Wrapper around a `sink&` for use with `record_batch_reader` interface.
// This class needs to implement two functions:
// 1. `compute_offset_deltas_to_keep(record_batch)`: This should iterate over
// the records of the provided `record_batch` and populate a vector of offset
// deltas of records which should be kept during compaction filtering.
// 2. `filter_batch_with_offset_deltas(record_batch, vector<int32_t>)`: Likely a
// pass through function to `do_filter_batch()`, but allows the `filter`
// implementation to examine the produced `offset_deltas` before creating a new
// `record_batch`.
class filter {
public:
    filter(sliding_window_reducer::sink& sink, model::ntp ntp)
      : _sink(sink)
      , _ntp(std::move(ntp)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b);
    stats end_of_stream() const { return _stats; }

protected:
    // Creates a new batch based on the provided batch and offset_deltas
    // indicated.
    ss::future<std::optional<model::record_batch>> do_filter_batch(
      model::record_batch b, std::vector<int32_t> offset_deltas) const;

    mutable stats _stats;

private:
    // For a given batch, this function should return a vector containing offset
    // deltas from records in the batch which we intend on keeping when
    // performing record batch filtering.
    virtual ss::future<std::vector<int32_t>>
    compute_offset_deltas_to_keep(const model::record_batch& b) const = 0;

    // For most implementations, this should serve as a pass through function to
    // `do_filter_batch()`. However, it provides flexibility in examining the
    // produced `offset_deltas` before creating a new `record_batch`. This is
    // useful for e.g. local storage in which we may need to create a
    // placeholder batch if `offset_deltas` is empty.
    virtual ss::future<std::optional<model::record_batch>>
    filter_batch_with_offset_deltas(
      model::record_batch b, std::vector<int32_t> offset_deltas) const = 0;

    // Computes offset deltas from the batch to keep, and then filters the
    // provided batch.
    ss::future<std::optional<model::record_batch>>
    filter_batch(model::record_batch b) const;

    // Performs filtering over the entire batch, and then delegates the result
    // to `_sink` for writing.
    ss::future<ss::stop_iteration> filter_and_rewrite_with_sink(
      model::compression original, model::record_batch b);

    sliding_window_reducer::sink& _sink;
    model::ntp _ntp;
};

} // namespace compaction
