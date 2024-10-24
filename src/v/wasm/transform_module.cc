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

#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "ffi.h"
#include "logger.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "wasi.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <exception>
#include <optional>

namespace wasm {

namespace {
constexpr int32_t NO_ACTIVE_TRANSFORM = -1;
constexpr int32_t INVALID_BUFFER = -2;
constexpr int32_t INVALID_WRITE = -3;

struct write_options {
    std::optional<model::topic_view> topic;

    static std::optional<write_options> parse(ffi::array<uint8_t> buffer) {
        constexpr uint8_t output_topic_key = 0x01;

        ffi::reader r(buffer);
        write_options opts;
        if (r.remaining_bytes() > 0) {
            if (r.read_byte() != output_topic_key) {
                return std::nullopt;
            }
            opts.topic = model::topic_view(r.read_sized_string_view());
        }
        if (r.remaining_bytes() > 0) {
            return std::nullopt;
        }
        return opts;
    }
};

} // namespace

transform_module::transform_module(wasi::preview1_module* m)
  : _wasi_module(m) {}

ss::future<> transform_module::for_each_record_async(
  model::record_batch input, record_callback* cb) {
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
      .callback = cb,
    });

    return host_wait_for_proccessing().finally(
      [this] { _call_ctx = std::nullopt; });
}

void transform_module::check_abi_version_1() {
    // This function does nothing at runtime, it's only an opportunity for
    // static analysis of the module to determine which ABI version to use.
}

void transform_module::check_abi_version_2() {
    // This function does nothing at runtime, it's only an opportunity for
    // static analysis of the module to determine which ABI version to use.
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
ss::future<int32_t> transform_module::read_batch_header(
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

    // If we are processing a batch (this isn't the first time this is called),
    // then we need to notify that we've finished processing this batch.
    if (_call_ctx) {
        _call_ctx->callback->post_record();
    }

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

    _wasi_module->set_walltime(
      header.attrs.timestamp_type() == model::timestamp_type::create_time
        ? header.first_timestamp
        : header.max_timestamp);

    co_return _call_ctx->max_input_record_size;
}

ss::future<int32_t> transform_module::read_next_record(
  uint8_t* attributes,
  int64_t* timestamp,
  model::offset* offset,
  ffi::array<uint8_t> buf) {
    if (!_call_ctx || _call_ctx->records.empty()) {
        co_return NO_ACTIVE_TRANSFORM;
    }

    // Callback that we finished processing the previous record,
    // but don't call this the first record that has been read.
    if (
      _call_ctx->records.size()
      != size_t(_call_ctx->batch_header.record_count)) {
        _call_ctx->callback->post_record();
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

    _wasi_module->set_walltime(record.timestamp);

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
    _call_ctx->callback->pre_record();

    co_return int32_t(record.payload_size);
}

ss::future<int32_t> transform_module::write_record(ffi::array<uint8_t> buf) {
    if (!_call_ctx) {
        co_return NO_ACTIVE_TRANSFORM;
    }
    iobuf b;
    b.append(buf.data(), buf.size());
    auto d = model::transformed_data::create_validated(std::move(b));
    if (!d) {
        co_return INVALID_BUFFER;
    }
    auto success = co_await _call_ctx->callback->emit(
      std::nullopt, *std::move(d));
    co_return success ? int32_t(buf.size()) : INVALID_WRITE;
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
ss::future<int32_t> transform_module::write_record_with_options(
  ffi::array<uint8_t> buf, ffi::array<uint8_t> options_buf) {
    // NOLINTEND(bugprone-easily-swappable-parameters)
    if (!_call_ctx) {
        co_return NO_ACTIVE_TRANSFORM;
    }
    iobuf b;
    b.append(buf.data(), buf.size());
    auto d = model::transformed_data::create_validated(std::move(b));
    if (!d) {
        co_return INVALID_BUFFER;
    }
    auto options = write_options::parse(options_buf);
    if (!options) {
        co_return INVALID_BUFFER;
    }
    auto success = co_await _call_ctx->callback->emit(
      options->topic, *std::move(d));
    co_return success ? int32_t(buf.size()) : INVALID_WRITE;
}

void transform_module::start() {
    _guest_cond_var.emplace();
    _host_cond_var.emplace();
}

void transform_module::stop(const std::exception_ptr& ex) {
    if (_guest_cond_var) {
        _guest_cond_var->broken(ex);
    }
    if (_host_cond_var) {
        _host_cond_var->broken(ex);
    }
}

ss::future<> transform_module::host_wait_for_proccessing() {
    _guest_cond_var->signal();
    return _host_cond_var->wait();
}

ss::future<> transform_module::guest_wait_for_batch() {
    _host_cond_var->signal();
    return _guest_cond_var->wait();
}

ss::future<> transform_module::await_ready() { return _host_cond_var->wait(); }
} // namespace wasm
