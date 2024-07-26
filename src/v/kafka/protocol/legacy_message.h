/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "hashing/crc32.h"
#include "kafka/protocol/logger.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "utils/to_string.h"

#include <fmt/chrono.h>

namespace kafka {

/*
 * Legacy messages are stored in message sets (effectively a packed array of
 * serialized messags). There are two message types identified by magic values.
 *
 * Message V0
 *   - offset
 *   - length
 *   - crc
 *   - magic
 *   - attributes
 *
 * Message V1
 *   - Message V0
 *   - timestamp
 *
 * Immediately after either message type is the optional key and optional value
 * encoded with fixed width size prefix (standard kafka encoding). This differs
 * from v2 which uses varint encoding.
 *
 * The crc covers the message bytes from the magic through the value. Unlike v2,
 * legacy messages use crc32 not crc32c.
 */
struct legacy_message {
    static constexpr uint16_t compression_mask = 0x07;

    model::offset base_offset;
    int32_t batch_length;
    int32_t batch_crc;
    int8_t magic;
    int8_t attributes;
    std::optional<model::timestamp> timestamp;
    std::optional<iobuf> key;
    std::optional<iobuf> value;

    model::compression compression() const {
        auto value = static_cast<uint8_t>(attributes) & compression_mask;
        switch (value) {
        case 0:
            return model::compression::none;
        case 1:
            return model::compression::gzip;
        case 2:
            return model::compression::snappy;
        case 3:
            return model::compression::lz4;
        default:
            throw std::runtime_error(
              fmt::format("Unknown compression value: {}", value));
        }
    }

    friend std::ostream& operator<<(std::ostream& os, const legacy_message& m) {
        fmt::print(
          os,
          "{{offset {} length {} crc {} magic {} attrs {}:{} ts {} key {} "
          "value "
          "{}}}",
          m.base_offset,
          m.batch_length,
          static_cast<uint32_t>(m.batch_crc),
          m.magic,
          m.attributes,
          m.compression(),
          m.timestamp,
          m.key,
          m.value);
        return os;
    }
};

inline std::optional<legacy_message> decode_legacy_batch(iobuf_parser& parser) {
    auto base_offset = model::offset(parser.consume_be_type<int64_t>());
    auto batch_length = parser.consume_be_type<int32_t>();
    auto expected_crc = parser.consume_be_type<int32_t>();
    // bytes covered by crc start immediately after the crc value, but not
    // including it
    auto crc_parser = iobuf_parser(
      parser.share_no_consume(batch_length - sizeof(int32_t)));
    auto magic = parser.consume_type<int8_t>();
    if (unlikely(magic != 0 && magic != 1)) {
        vlog(klog.error, "Expected magic 0 or 1 got {}", magic);
        return std::nullopt;
    }
    auto attributes = parser.consume_type<int8_t>();
    std::optional<model::timestamp> timestamp;
    if (magic == 1) {
        timestamp = model::timestamp(parser.consume_be_type<int64_t>());
    }

    crc::crc32 computed_crc;
    crc_parser.consume(
      crc_parser.bytes_left(), [&computed_crc](const char* src, size_t n) {
          computed_crc.extend(
            reinterpret_cast<const uint8_t*>(src), n); // NOLINT
          return ss::stop_iteration::no;
      });

    if (unlikely(static_cast<uint32_t>(expected_crc) != computed_crc.value())) {
        vlog(
          klog.error,
          "Cannot validate legacy batch (magic={}) CRC. Expected {}. Computed "
          "{}",
          magic,
          static_cast<uint32_t>(expected_crc),
          computed_crc.value());
        return std::nullopt;
    }

    auto key_size = parser.consume_be_type<int32_t>();
    std::optional<iobuf> key;
    if (key_size != -1) {
        key = parser.share(key_size);
    }

    auto value_size = parser.consume_be_type<int32_t>();
    std::optional<iobuf> value;
    if (value_size != -1) {
        value = parser.share(value_size);
    }

    return legacy_message{
      .base_offset = base_offset,
      .batch_length = batch_length,
      .batch_crc = expected_crc,
      .magic = magic,
      .attributes = attributes,
      .timestamp = timestamp,
      .key = std::move(key),
      .value = std::move(value),
    };
}

} // namespace kafka
