/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/object.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <optional>

namespace cloud_topics::l1 {

/// Implements object_reader over a raw Kafka-format byte stream (a TS segment).
///
/// Translates log offsets to Kafka offsets by maintaining a running delta.
/// Non-data batches are skipped while incrementing the delta.
class tiered_storage_object_reader final : public object_reader {
public:
    /// `initial_delta` is the log-to-Kafka offset-translation delta at the
    /// stream's start (from the seek). It is applied to every batch and
    /// maintained as non-data batches are skipped. Taking it from the seek --
    /// rather than inferring it from the first batch's log offset -- keeps
    /// translation correct when compaction has removed the segment's leading
    /// records (the first surviving batch may sit past the declared base).
    ///
    /// `term` is the raft term the segment was written in; it is stamped onto
    /// every emitted batch as its leader epoch (the segment is single-term).
    /// `aborted` holds the segment's aborted-transaction ranges (raw log-offset
    /// space, from the .tx manifest); data batches that fall in them are
    /// dropped so the imported region is committed-only, like native CT L1.
    tiered_storage_object_reader(
      ss::input_stream<char> stream,
      model::offset_delta initial_delta,
      model::term_id term,
      aborted_transactions aborted);

    ss::future<> close() override;
    ss::future<peek_result> peek() override;
    ss::future<result> read_next() override;

private:
    ss::future<std::optional<model::record_batch>> fetch_next_translated();

    ss::input_stream<char> _stream;
    // The current log-to-Kafka delta: seeded from the seek and advanced as
    // offset-translator (non-data) batches are skipped.
    model::offset_delta _running_delta;
    model::term_id _term;
    aborted_transactions _aborted;
    std::optional<model::record_batch> _peeked;
    bool _eof{false};
};

} // namespace cloud_topics::l1
