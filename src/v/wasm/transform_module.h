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

#pragma once

#include "bytes/iobuf.h"
#include "model/record.h"
#include "utils/named_type.h"
#include "wasm/ffi.h"

#include <seastar/util/noncopyable_function.hh>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace wasm {
using batch_handle = named_type<int32_t, struct batch_handle_tag>;
using record_handle = named_type<int32_t, struct record_handle_tag>;

constexpr std::string_view redpanda_on_record_callback_function_name
  = "redpanda_transform_on_record_written";

struct record_position {
    size_t start_index;
    size_t size;

    int32_t timestamp_delta;
};

// The data needed during a single transformation of a record_batch
struct transform_context {
    // The input record_batch being transformed.
    const model::record_batch* input;
    // The current record being transformed
    record_position current_record;
    // The serialized output records.
    iobuf output_records;
    // The number of output records we've written.
    int output_record_count{0};
};

struct wasm_call_params {
    batch_handle batch_handle;
    record_handle record_handle;
    int32_t record_size;
    int32_t current_record_offset;
    model::timestamp current_record_timestamp;
};

/**
 * The WASM module for redpanda transform specific host calls.
 *
 * This provides an ABI to WASM guests, as well as the mechanism for
 * guest<->host interactions (such as how we call into a wasm host and when).
 */
class transform_module {
public:
    transform_module() = default;
    transform_module(const transform_module&) = delete;
    transform_module& operator=(const transform_module&) = delete;
    transform_module(transform_module&&) = default;
    transform_module& operator=(transform_module&&) = default;
    ~transform_module() = default;

    static constexpr std::string_view name = "redpanda_transform";

    /**
     * A helper method for correctly adhering to the ABI contract. Given a
     * batch, a callback will be triggered with the correct parameters for
     * the redpanda_transform_on_record_written function that the guest should
     * expose.
     */
    model::record_batch for_each_record(
      const model::record_batch*,
      ss::noncopyable_function<void(wasm_call_params)>);

    // Start ABI exports

    int32_t read_batch_header(
      batch_handle,
      int64_t* base_offset,
      int32_t* record_count,
      int32_t* partition_leader_epoch,
      int16_t* attributes,
      int32_t* last_offset_delta,
      int64_t* base_timestamp,
      int64_t* max_timestamp,
      int64_t* producer_id,
      int16_t* producer_epoch,
      int32_t* base_sequence);

    int32_t read_record(record_handle, ffi::array<uint8_t>);

    int32_t write_record(ffi::array<uint8_t>);

    // End ABI exports

private:
    struct expected_record_metadata {
        int32_t offset;
        int32_t timestamp;
    };

    bool is_valid_serialized_record(
      iobuf_const_parser parser, expected_record_metadata);

    std::optional<transform_context> _call_ctx;
};
} // namespace wasm
