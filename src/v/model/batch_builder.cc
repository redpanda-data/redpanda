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

#include "model/batch_builder.h"

#include "base/vassert.h"
#include "model/batch_compression.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>

#include <ctime>

namespace model {

void batch_builder::add_record(simple_record r) {
    add_record(
      model::record(
        /*attributes=*/{},
        /*timestamp_delta=*/0,
        /*offset_delta=*/_num_records,
        /*key=*/std::move(r.key),
        /*value=*/std::move(r.value),
        /*hdrs=*/{}));
}

void batch_builder::add_record(record r) {
    dassert(
      r.offset_delta() >= 0,
      "offset delta should never be negative: {}",
      r.offset_delta());
    _max_offset_delta = std::max(_max_offset_delta, r.offset_delta());
    // timestamp deltas *can* be negative.
    _max_timestamp_delta = std::max(_max_timestamp_delta, r.timestamp_delta());
    append_record_to_buffer(_batch_data, r);
    ++_num_records;
}

void batch_builder::set_batch_type(record_batch_type type) {
    _batch_type = type;
}

record_batch_header batch_builder::build_header() {
    if (_num_records == 0) {
        throw std::runtime_error("tried to build an empty batch");
    }
    timestamp first_timestamp, max_timestamp;
    if (_is_append_time) {
        max_timestamp = _has_timestamp_override ? _batch_timestamp
                                                : timestamp::now();
        first_timestamp = max_timestamp;
    } else {
        first_timestamp = _has_timestamp_override ? _batch_timestamp
                                                  : timestamp::now();
        max_timestamp = timestamp{first_timestamp() + _max_timestamp_delta};
    }
    record_batch_attributes attrs;
    if (_is_control) {
        attrs.set_control_type();
    }
    if (_is_txn) {
        attrs.set_transactional_type();
    }
    attrs.set_timestamp_type(
      _is_append_time ? timestamp_type::append_time
                      : timestamp_type::create_time);
    record_batch_header::context ctx;
    ctx.term = _term;
    ctx.owner_shard = ss::this_shard_id();
    return {
      .header_crc = 0, // set later in reset_size_checksum_metadata
      .size_bytes = static_cast<int32_t>(
        packed_record_batch_header_size + _batch_data.size_bytes()),
      .base_offset = kafka::offset_cast(_base_offset),
      .type = _batch_type,
      .crc = 0, // set later in reset_size_checksum_metadata
      .attrs = attrs,
      .last_offset_delta = static_cast<int32_t>(
        _has_last_offset_override ? (_last_offset - _base_offset)()
                                  : _max_offset_delta),
      .first_timestamp = first_timestamp,
      .max_timestamp = max_timestamp,
      .producer_id = _producer_id,
      .producer_epoch = _producer_epoch,
      .base_sequence = _base_sequence,
      .record_count = _num_records,
      .ctx = ctx,
    };
}

ss::future<record_batch> batch_builder::build() {
    auto header = build_header();
    record_batch batch{
      header, std::move(_batch_data), record_batch::tag_ctor_ng{}};
    if (_compression != compression::none) {
        batch = co_await compress_batch(_compression, std::move(batch));
    } else {
        batch.header().reset_size_checksum_metadata(batch.data());
    }
    co_return batch;
}

record_batch batch_builder::build_sync() {
    auto header = build_header();
    record_batch batch{
      header, std::move(_batch_data), record_batch::tag_ctor_ng{}};
    if (_compression != compression::none) {
        batch = compress_batch_sync(_compression, std::move(batch));
    } else {
        batch.header().reset_size_checksum_metadata(batch.data());
    }
    return batch;
}

} // namespace model
