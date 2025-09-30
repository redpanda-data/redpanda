/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"

#include <limits>

namespace model {

// A builder of record batches. This builder should give full control over the
// resulting batch in a single API. Traditionally there is a
// storage::batch_builder that has been used all over the system, but that
// builder makes some simplifications that don't hold true when considering the
// full range of possibilities in the kafka batch format.
class batch_builder {
public:
    // A simplified record representation for most internal users of batches.
    struct simple_record {
        iobuf key;
        iobuf value;
    };

    // Add a single raw key and value pair, assuming that the timestamp_delta is
    // 0, offset_delta is N (where N is the current number of records in the
    // batch) and there are no headers.
    //
    // Expected usage:
    //
    // builder.add_record({.key = std::move(key), .value = std::move(value)});
    void add_record(simple_record);

    // Add a full record, which gives control over the offset delta, timestamp
    // delta, headers and (optional) key/value data.
    void add_record(record);

    // The type of this batch.
    //
    // Defaults to raft data batches (batches consumable
    // and producable by external clients via the kafka protocol).
    void set_batch_type(record_batch_type);

    // Set the compression for the batch.
    //
    // Defaults to no compression
    void set_compression(compression c) { _compression = c; }

    // Set if this batch is a control batch or not.
    void set_control() { _is_control = true; }

    // Set if this batch is a part of a transaction or not.
    void set_transactional() { _is_txn = true; }

    void set_producer_id(int64_t id) { _producer_id = id; }
    void set_producer_epoch(int16_t epoch) { _producer_epoch = epoch; }
    void set_base_sequence(int32_t bs) { _base_sequence = bs; }

    // Set the batch's timestamp. This is more complicated than you would think
    // due to kafka :)
    //
    // If you set this to be `model::timestamp_type::create_time`, then this is
    // the first timestamp all records will base their `timestamp_delta` off of.
    // Otherwise if set to `model::timestamp_type::append_time`, this becomes
    // the timestamp for all records in the batch.
    //
    // Default's to append_time timestamp at the moment `build` is called.
    void set_batch_timestamp(timestamp_type ttype, timestamp ts) {
        _is_append_time = ttype == timestamp_type::append_time;
        _batch_timestamp = ts;
        _has_timestamp_override = true;
    }

    // Set the base offset for the batch.
    //
    // Defaults to kafka::offset(0).
    void set_base_offset(kafka::offset o) { _base_offset = o; }
    void set_base_offset(offset o) { set_base_offset(offset_cast(o)); }
    // Set the last offset for the batch.
    //
    // By default this will be computed using the highest offset delta of
    // records passed in.
    //
    // When does this ever differ from the number of records in the batch? Good
    // question! The answer is compaction - it can remove records in the middle
    // of batches and we need to preserve the offsets still.
    void set_last_offset(kafka::offset o) {
        _has_last_offset_override = true;
        _last_offset = o;
    }
    void set_last_offset(offset o) { set_last_offset(offset_cast(o)); }

    // Set the batch's term, defaults to model::term_id::min().
    void set_term(term_id term) { _term = term; }

    // The size of the header + batch bytes
    size_t size_bytes() const {
        // This should be the same as `build_sync().size_bytes()`
        return _batch_data.size_bytes() + sizeof(model::record_batch_header);
    }

    // The memory usage of the accumulated batch once it's built
    size_t memory_usage() const {
        // This should be the same as `build_sync().memory_usage()`
        return _batch_data.size_bytes() + sizeof(model::record_batch);
    }

    // Build and (optionally) compress the record batch.
    ss::future<record_batch> build();

    // Build and (optionally) compress the record batch.
    record_batch build_sync();

private:
    record_batch_header build_header();

    iobuf _batch_data;
    timestamp _batch_timestamp;
    int64_t _max_timestamp_delta = std::numeric_limits<int64_t>::min();
    kafka::offset _base_offset = kafka::offset{0};
    kafka::offset _last_offset;
    term_id _term;
    int64_t _producer_id{0};
    int32_t _num_records{0};
    int32_t _base_sequence{0};
    int32_t _max_offset_delta = std::numeric_limits<int32_t>::min();
    int16_t _producer_epoch{0};
    record_batch_type _batch_type = record_batch_type::raft_data;
    compression _compression = compression::none;

    bool _is_control = false;
    bool _is_txn = false;
    bool _is_append_time = true;
    bool _has_timestamp_override = false;
    bool _has_last_offset_override = false;
};

} // namespace model
