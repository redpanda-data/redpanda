// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/outcome.h"
#include "base/units.h"
#include "bytes/iobuf.h"
#include "storage/mvlog/errc.h"
#include "storage/mvlog/reader_outcome.h"
#include "storage/types.h"

#include <seastar/core/circular_buffer.hh>

namespace storage::experimental::mvlog {

// Encapsulates the collection of record batches while reading the log.
// Expects that batches and terms are added in increasing order.
//
// Note that this class does not enforce physical correctness of the batches,
// e.g. validation of checksums is left to other classes.
class batch_collector {
public:
    batch_collector(
      const log_reader_config& cfg,
      model::term_id initial_term,
      size_t max_buffer_size = default_max_buffer_size)
      : cfg_(cfg)
      , target_max_buffer_size_(max_buffer_size)
      , last_offset_(model::offset::min())
      , cur_term_(initial_term)
      , cur_buffer_size_(0) {}

    // Adds the given header and iobuf as a record batch.
    // Enforces logical constraints and invariants on the constructed batches,
    // e.g. checking if the data exceeds the bounds of the reader config.
    result<reader_outcome, errc>
    add_batch(model::record_batch_header batch_hdr, iobuf records) noexcept;

    // Sets the term to be used for subsequent batches, returning an error if
    // the term ever goes down.
    result<reader_outcome, errc> set_term(model::term_id new_term) noexcept;

    // Releases the batches to the caller.
    ss::circular_buffer<model::record_batch> release_batches() noexcept {
        cur_buffer_size_ = 0;
        return std::move(batches_);
    }

private:
    static constexpr size_t default_max_buffer_size = 32_KiB;

    // Returns true if the reader associated with this collector can skip the
    // given batch, e.g. because the batch falls below the start of the desired
    // read range.
    bool should_skip(const model::record_batch_header&) const noexcept;

    // Reader config with which to collect records.
    const log_reader_config& cfg_;

    // Soft max of the size in bytes `batches_` is allowed to grow before
    // `add_batch()` returns `reader_outcome::buffer_full`.
    const size_t target_max_buffer_size_;

    // The last offset seen by this collector.
    model::offset last_offset_;

    // The term to use when constructing the next record batch.
    model::term_id cur_term_;

    // Size in bytes consumed by `batches_`. If this grows beyond
    // `target_max_buffer_size_`, further addition of batches may still
    // succeed, but should also signal that the buffer has reached capacity.
    size_t cur_buffer_size_;

    ss::circular_buffer<model::record_batch> batches_;
};

} // namespace storage::experimental::mvlog
