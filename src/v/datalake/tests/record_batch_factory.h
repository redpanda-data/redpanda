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

#include "bytes/iobuf.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "pandaproxy/schema_registry/types.h"
#include "storage/record_batch_builder.h"
#include "utils/vint.h"

#include <seastar/core/sstring.hh>

#include <array>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace datalake::tests {

/// \brief Encode a value with the schema registry wire format.
///
/// Format: [0x00 magic byte][4-byte big-endian schema_id][raw_data]
/// Used for Avro and JSON records in value_schema_id_prefix mode.
inline iobuf
encode_schema_id_prefix(pandaproxy::schema_registry::schema_id id, iobuf data) {
    iobuf result;
    result.append("\0", 1);
    int32_t encoded_id = ss::cpu_to_be(id());
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    result.append(reinterpret_cast<const char*>(&encoded_id), 4);
    result.append(std::move(data));
    return result;
}

/// \brief Encode protobuf message index as varints.
///
/// Special case: {0} encodes as a single null byte.
/// General case: varint count followed by varint-encoded indices.
inline iobuf
encode_protobuf_message_index(const std::vector<int32_t>& message_index) {
    iobuf ret;
    if (message_index.size() == 1 && message_index[0] == 0) {
        ret.append("\0", 1);
        return ret;
    }
    std::array<uint8_t, vint::max_length> bytes{0};
    auto sz = vint::serialize(message_index.size(), bytes.data());
    ret.append(bytes.data(), sz);
    for (const auto& o : message_index) {
        sz = vint::serialize(o, bytes.data());
        ret.append(bytes.data(), sz);
    }
    return ret;
}

/// \brief Encode a protobuf value with schema registry wire format.
///
/// Format: [0x00][4-byte BE schema_id][varint message_index][proto_data]
inline iobuf encode_protobuf_prefix(
  pandaproxy::schema_registry::schema_id id,
  const std::vector<int32_t>& message_index,
  iobuf proto_data) {
    iobuf result;
    result.append("\0", 1);
    int32_t encoded_id = ss::cpu_to_be(id());
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    result.append(reinterpret_cast<const char*>(&encoded_id), 4);
    result.append(encode_protobuf_message_index(message_index));
    result.append(std::move(proto_data));
    return result;
}

/// \brief Build record batches with deterministic content for any
/// iceberg mode.
///
/// Unlike record_generator which produces random data, this class
/// lets test authors specify exact key/value content with proper
/// wire format encoding for the target iceberg mode.
///
/// Example (cdc_key_value mode — raw binary, no schema encoding):
///   record_batch_factory bf;
///   bf.add_record("key1", "value1");
///   bf.add_tombstone("key1");
///   auto batch = bf.build(model::offset{0});
///
/// Example (value_schema_id_prefix mode — Avro):
///   record_batch_factory bf;
///   iobuf avro_data = serialize_avro(...);
///   bf.add_avro_record(schema_id{1}, to_iobuf("key"), std::move(avro_data));
///   auto batch = bf.build(model::offset{0});
class record_batch_factory {
public:
    /// Add a raw binary record (no wire format encoding).
    /// For key_value and cdc_key_value modes.
    void add_record(std::string_view key, std::string_view value) {
        iobuf k;
        k.append(key.data(), key.size());
        iobuf v;
        v.append(value.data(), value.size());
        _records.emplace_back(std::move(k), std::make_optional(std::move(v)));
    }

    /// Add a raw binary record with iobuf key and value.
    void add_record(iobuf key, std::optional<iobuf> value) {
        _records.emplace_back(std::move(key), std::move(value));
    }

    /// Add a tombstone (null value). In cdc_key_value mode, this
    /// triggers an equality delete.
    void add_tombstone(std::string_view key) {
        iobuf k;
        k.append(key.data(), key.size());
        _records.emplace_back(std::move(k), std::nullopt);
    }

    /// Add a record with schema_id_prefix encoding (for Avro/JSON).
    /// Value is prepended with [0x00][schema_id_4B_BE][data].
    void add_avro_record(
      pandaproxy::schema_registry::schema_id id,
      iobuf key,
      iobuf avro_encoded_value) {
        auto encoded = encode_schema_id_prefix(
          id, std::move(avro_encoded_value));
        _records.emplace_back(std::move(key), std::move(encoded));
    }

    /// Add a record with protobuf encoding.
    /// Value is prepended with [0x00][schema_id][varint_offsets][data].
    void add_protobuf_record(
      pandaproxy::schema_registry::schema_id id,
      const std::vector<int32_t>& message_offsets,
      iobuf key,
      iobuf proto_encoded_value) {
        auto encoded = encode_protobuf_prefix(
          id, message_offsets, std::move(proto_encoded_value));
        _records.emplace_back(std::move(key), std::move(encoded));
    }

    /// Build the record batch at the given base offset.
    model::record_batch build(model::offset base_offset) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, base_offset);
        for (auto& [key, value] : _records) {
            builder.add_raw_kv(std::move(key), std::move(value));
        }
        _records.clear();
        return std::move(builder).build();
    }

private:
    std::vector<std::pair<iobuf, std::optional<iobuf>>> _records;
};

} // namespace datalake::tests
