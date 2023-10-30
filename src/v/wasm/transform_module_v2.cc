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

#include "wasm/transform_module_v2.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "utils/named_type.h"
#include "utils/vint.h"
#include "vassert.h"
#include "wasm/ffi.h"
#include "wasm/logger.h"
#include "wasm/wasi.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <exception>
#include <optional>

namespace wasm {

constexpr int32_t NO_ACTIVE_TRANSFORM = -1;
constexpr int32_t INVALID_BUFFER = -2;

transform_module_v2::transform_module_v2(wasi::preview1_module* m)
  : _wasi_module(m) {}

ss::future<ss::chunked_fifo<model::transformed_data>>
transform_module_v2::for_each_record_async(
  model::record_batch input, ss::noncopyable_function<void()> cb) {
    vassert(
      input.header().attrs.compression() == model::compression::none,
      "wasm transforms expect uncompressed batches");

    iobuf_const_parser parser(input.data());

    ss::chunked_fifo<record_metadata> records;
    records.reserve(input.record_count());
    size_t max_size = 0;

    while (parser.bytes_left() > 0) {
        auto [record_size, rs_amt] = parser.read_varlong();
        auto attrs = parser.consume_type<model::record_attributes::type>();
        auto [timestamp_delta, td_amt] = parser.read_varlong();
        auto [offset_delta, od_amt] = parser.read_varlong();
        size_t meta_size = sizeof(decltype(attrs)) + td_amt + od_amt;
        size_t payload_size = record_size - meta_size;
        max_size = std::max(payload_size, max_size);
        parser.skip(payload_size);
        model::timestamp ts = input.header().max_timestamp;
        if (
          input.header().attrs.timestamp_type()
          == model::timestamp_type::create_time) {
            ts = model::timestamp(
              input.header().first_timestamp() + timestamp_delta);
        }
        records.push_back({
          .metadata_size = rs_amt + meta_size,
          .payload_size = payload_size,
          .attributes = model::record_attributes(attrs),
          .timestamp = ts,
          .offset = input.base_offset() + offset_delta,
        });
    }

    _call_ctx.emplace(batch_transform_context{
      .batch_header = input.header(),
      .batch_data = std::move(input).release_data(),
      .max_input_record_size = max_size,
      .records = std::move(records),
      .output_data = {},
      .record_callback = std::move(cb),
    });

    co_await host_wait_for_proccessing();

    auto result = std::exchange(_call_ctx, std::nullopt);
    co_return std::move(result->output_data);
}

void transform_module_v2::check_abi_version_1() {
    // This function does nothing at runtime, it's only an opportunity for
    // static analysis of the module to determine which ABI version to use.
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
ss::future<int32_t> transform_module_v2::read_batch_header(
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

    co_await guest_wait_for_batch();

    if (!_call_ctx) {
        co_return NO_ACTIVE_TRANSFORM;
    }
    const model::record_batch_header& header = _call_ctx->batch_header;
    *base_offset = header.base_offset();
    *record_count = header.record_count;
    *partition_leader_epoch = int32_t(header.ctx.term());
    *attributes = header.attrs.value();
    *last_offset_delta = header.last_offset_delta;
    *base_timestamp = header.first_timestamp();
    *max_timestamp = header.max_timestamp();
    *producer_id = header.producer_id;
    *producer_epoch = header.producer_epoch;
    *base_sequence = header.base_sequence;

    _wasi_module->set_timestamp(
      header.attrs.timestamp_type() == model::timestamp_type::create_time
        ? header.first_timestamp
        : header.max_timestamp);

    co_return _call_ctx->max_input_record_size;
}

ss::future<int32_t> transform_module_v2::read_next_record(
  uint8_t* attributes,
  int64_t* timestamp,
  model::offset* offset,
  ffi::array<uint8_t> buf) {
    if (!_call_ctx || _call_ctx->records.empty()) {
        co_return NO_ACTIVE_TRANSFORM;
    }
    co_await ss::coroutine::maybe_yield();

    auto record = _call_ctx->records.front();
    if (buf.size() < record.payload_size) {
        vlog(
          wasm_log.debug,
          "read_record invalid buffer size: {} < {}",
          buf.size(),
          record.payload_size);
        // Buffer wrong size
        co_return INVALID_BUFFER;
    }
    _call_ctx->records.pop_front();

    _wasi_module->set_timestamp(record.timestamp);

    // Pass back the record's metadata
    *attributes = record.attributes.value();
    *timestamp = record.timestamp();
    *offset = record.offset;

    // Drop the metadata we already parsed
    _call_ctx->batch_data.trim_front(record.metadata_size);
    // Copy out the payload
    {
        iobuf_const_parser parser(_call_ctx->batch_data);
        parser.consume_to(record.payload_size, buf.data());
    }
    // Skip over the payload
    _call_ctx->batch_data.trim_front(record.payload_size);

    // Call back so we can refuel.
    _call_ctx->record_callback();

    co_return int32_t(record.payload_size);
}

int32_t transform_module_v2::write_record(ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        return NO_ACTIVE_TRANSFORM;
    }
    iobuf b;
    b.append(buf.data(), buf.size());
    auto d = model::transformed_data::create_validated(std::move(b));
    if (!d) {
        return INVALID_BUFFER;
    }
    _call_ctx->output_data.push_back(*std::move(d));
    return int32_t(buf.size());
}

void transform_module_v2::start() {
    _guest_cond_var.emplace();
    _host_cond_var.emplace();
}

void transform_module_v2::stop(const std::exception_ptr& ex) {
    if (_guest_cond_var) {
        _guest_cond_var->broken(ex);
        _guest_cond_var = std::nullopt;
    }
    if (_host_cond_var) {
        _host_cond_var->broken(ex);
        _host_cond_var = std::nullopt;
    }
}

ss::future<> transform_module_v2::host_wait_for_proccessing() {
    _guest_cond_var->signal();
    return _host_cond_var->wait();
}

ss::future<> transform_module_v2::guest_wait_for_batch() {
    _host_cond_var->signal();
    return _guest_cond_var->wait();
}

ss::future<> transform_module_v2::await_ready() {
    return _host_cond_var->wait();
}
} // namespace wasm
