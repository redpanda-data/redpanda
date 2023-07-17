#pragma once

#include "bytes/iobuf.h"
#include "model/record.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/named_type.h"
#include "wasm/ffi.h"
#include "wasm/schema_registry.h"

#include <seastar/util/noncopyable_function.hh>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace wasm {
using batch_handle = named_type<int32_t, struct batch_handle_tag>;
using record_handle = named_type<int32_t, struct record_handle_tag>;

constexpr int max_output_records = 256;
constexpr std::string_view redpanda_on_record_callback_function_name
  = "redpanda_on_record";

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
 * The WASM module for redpanda specific host calls.
 *
 * This provides an ABI to WASM guests, as well as the mechanism for
 * guest<->host interactions (such as how we call into a wasm host and when).
 */
class redpanda_module {
public:
    explicit redpanda_module(schema_registry*);
    redpanda_module(const redpanda_module&) = delete;
    redpanda_module& operator=(const redpanda_module&) = delete;
    redpanda_module(redpanda_module&&) = default;
    redpanda_module& operator=(redpanda_module&&) = default;
    ~redpanda_module() = default;

    static constexpr std::string_view name = "redpanda";

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

    ss::future<int32_t> get_schema_definition_len(
      pandaproxy::schema_registry::schema_id, uint32_t*);

    ss::future<int32_t> get_schema_definition(
      pandaproxy::schema_registry::schema_id, ffi::array<uint8_t>);

    ss::future<int32_t> get_subject_schema_len(
      pandaproxy::schema_registry::subject,
      pandaproxy::schema_registry::schema_version,
      uint32_t*);

    ss::future<int32_t> get_subject_schema(
      pandaproxy::schema_registry::subject,
      pandaproxy::schema_registry::schema_version,
      ffi::array<uint8_t>);

    ss::future<int32_t> create_subject_schema(
      pandaproxy::schema_registry::subject,
      ffi::array<uint8_t>,
      pandaproxy::schema_registry::schema_id*);
    // End ABI exports

private:
    struct expected_record_metadata {
        int32_t offset;
        int32_t timestamp;
    };

    bool is_valid_serialized_record(
      iobuf_const_parser parser, expected_record_metadata);

    std::optional<transform_context> _call_ctx;
    schema_registry* _sr;
};
} // namespace wasm
