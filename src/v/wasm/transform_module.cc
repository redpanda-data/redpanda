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

#include "transform_module.h"

#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "utils/named_type.h"
#include "utils/vint.h"
#include "vassert.h"
#include "wasm/ffi.h"
#include "wasm/logger.h"

#include <algorithm>
#include <exception>
#include <optional>
#include <stdexcept>
#include <vector>
namespace wasm {

constexpr int32_t SUCCESS = 0;
constexpr int32_t NO_ACTIVE_TRANSFORM = -1;
constexpr int32_t INVALID_HANDLE = -2;
constexpr int32_t INVALID_BUFFER = -3;

model::record_batch transform_module::for_each_record(
  const model::record_batch* input,
  ss::noncopyable_function<void(wasm_call_params)> func) {
    vassert(
      input->header().attrs.compression() == model::compression::none,
      "wasm transforms expect uncompressed batches");

    iobuf_const_parser parser(input->data());

    auto bh = batch_handle(input->header().crc);

    std::vector<record_position> record_positions;
    record_positions.reserve(input->record_count());

    while (parser.bytes_left() > 0) {
        auto start_index = parser.bytes_consumed();
        auto [size, amt] = parser.read_varlong();
        parser.skip(sizeof(model::record_attributes::type));
        auto [timestamp_delta, td] = parser.read_varlong();
        parser.skip(size - sizeof(model::record_attributes::type) - td);
        record_positions.push_back(
          {.start_index = start_index,
           .size = size_t(size + amt),
           .timestamp_delta = int32_t(timestamp_delta)});
    }

    _call_ctx.emplace(transform_context{
      .input = input,
    });

    for (const auto& record_position : record_positions) {
        _call_ctx->current_record = record_position;
        auto current_record_timestamp = input->header().first_timestamp()
                                        + record_position.timestamp_delta;
        try {
            func({
              .batch_handle = bh,
              .record_handle = record_handle(
                int32_t(record_position.start_index)),
              .record_size = int32_t(record_position.size),
              .current_record_offset = int32_t(_call_ctx->output_record_count),
              .current_record_timestamp = model::timestamp(
                current_record_timestamp),
            });
        } catch (...) {
            _call_ctx = std::nullopt;
            std::rethrow_exception(std::current_exception());
        }
    }

    model::record_batch::compressed_records records = std::move(
      _call_ctx->output_records);
    model::record_batch_header header = _call_ctx->input->header();
    header.size_bytes = int32_t(
      model::packed_record_batch_header_size + records.size_bytes());
    header.record_count = _call_ctx->output_record_count;
    model::record_batch batch(
      header, std::move(records), model::record_batch::tag_ctor_ng{});
    batch.header().crc = model::crc_record_batch(batch);
    batch.header().header_crc = model::internal_header_only_crc(batch.header());
    _call_ctx = std::nullopt;
    return batch;
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
int32_t transform_module::read_batch_header(
  batch_handle bh,
  int64_t* base_offset,
  int32_t* record_count,
  int32_t* partition_leader_epoch,
  int16_t* attributes,
  int32_t* last_offset_delta,
  int64_t* base_timestamp,
  int64_t* max_timestamp,
  int64_t* producer_id,
  int16_t* producer_epoch,
  int32_t* base_sequence) {
    // NOLINTEND(bugprone-easily-swappable-parameters)
    if (!_call_ctx) {
        return NO_ACTIVE_TRANSFORM;
    }
    if (bh != _call_ctx->input->header().crc) {
        return INVALID_HANDLE;
    }
    *base_offset = _call_ctx->input->base_offset();
    *record_count = _call_ctx->input->record_count();
    *partition_leader_epoch = int32_t(_call_ctx->input->term()());
    *attributes = _call_ctx->input->header().attrs.value();
    *last_offset_delta = _call_ctx->input->header().last_offset_delta;
    *base_timestamp = _call_ctx->input->header().first_timestamp();
    *max_timestamp = _call_ctx->input->header().max_timestamp();
    *producer_id = _call_ctx->input->header().producer_id;
    *producer_epoch = _call_ctx->input->header().producer_epoch;
    *base_sequence = _call_ctx->input->header().base_sequence;
    return SUCCESS;
}
int32_t
transform_module::read_record(record_handle h, ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        return NO_ACTIVE_TRANSFORM;
    }
    if (h != int32_t(_call_ctx->current_record.start_index)) {
        return INVALID_HANDLE;
    }
    if (_call_ctx->current_record.size != buf.size()) {
        // Buffer wrong size
        return INVALID_BUFFER;
    }
    iobuf_const_parser parser(_call_ctx->input->data());
    parser.skip(_call_ctx->current_record.start_index);
    parser.consume_to(buf.size(), buf.data());
    return int32_t(buf.size());
}

bool transform_module::is_valid_serialized_record(
  iobuf_const_parser parser, expected_record_metadata expected) {
    try {
        auto [record_size, amt] = parser.read_varlong();
        if (size_t(record_size) != parser.bytes_left()) {
            return false;
        }
        parser.skip(sizeof(model::record_attributes::type));
        auto [timestamp_delta, td] = parser.read_varlong();
        auto [offset_delta, od] = parser.read_varlong();
        if (expected.timestamp != timestamp_delta) {
            return false;
        }
        if (expected.offset != offset_delta) {
            return false;
        }
        auto [key_length, kl] = parser.read_varlong();
        if (key_length > 0) {
            parser.skip(key_length);
        }
        auto [value_length, vl] = parser.read_varlong();
        if (value_length > 0) {
            parser.skip(value_length);
        }
        auto [header_count, hv] = parser.read_varlong();
        for (int i = 0; i < header_count; ++i) {
            auto [key_length, kl] = parser.read_varlong();
            if (key_length > 0) {
                parser.skip(key_length);
            }
            auto [value_length, vl] = parser.read_varlong();
            if (value_length > 0) {
                parser.skip(value_length);
            }
        }
    } catch (const std::out_of_range& ex) {
        return false;
    }
    return parser.bytes_left() == 0;
}

int32_t transform_module::write_record(ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        return NO_ACTIVE_TRANSFORM;
    }
    // TODO: Add a limit on the size of output batch to be the output topic's
    // max batch size.
    iobuf b;
    b.append(buf.data(), buf.size());
    expected_record_metadata expected{
      // The delta offset should just be the current record count
      .offset = _call_ctx->output_record_count,
      // We expect the timestamp to not change
      .timestamp = _call_ctx->current_record.timestamp_delta,
    };
    if (!is_valid_serialized_record(iobuf_const_parser(b), expected)) {
        // Invalid payload
        return INVALID_BUFFER;
    }
    _call_ctx->output_records.append_fragments(std::move(b));
    _call_ctx->output_record_count += 1;
    return int32_t(buf.size());
}

} // namespace wasm
