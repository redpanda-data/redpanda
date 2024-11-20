/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/schema_registry.h"

#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "datalake/logger.h"
#include "pandaproxy/schema_registry/types.h"

#include <exception>

namespace datalake {
get_schema_id_result get_value_schema_id(iobuf& buf) noexcept {
    // Messages that use the schema registry have a 5-byte prefix:
    // offset 0: magic byte with value 0
    // offsets 1-4: schema ID as big-endian integer
    const uint8_t schema_registry_magic_byte = 0;

    if (buf.size_bytes() < sizeof(uint8_t) + sizeof(int32_t)) {
        return get_schema_error::not_enough_bytes;
    }
    try {
        iobuf_const_parser parser(buf);
        auto magic = parser.consume_type<uint8_t>();
        if (magic != schema_registry_magic_byte) {
            return get_schema_error::no_schema_id;
        }
        auto id = parser.consume_be_type<int32_t>();
        schema_message_data res = {
          .schema_id = pandaproxy::schema_registry::schema_id{id},
          .shared_message_data = buf.share(
            parser.bytes_consumed(), parser.bytes_left())};

        return res;
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Error parsing schema id: {}",
          std::current_exception());
        return get_schema_error::not_enough_bytes;
    }
}

// TODO: this is mostly a copy-and-paste of get_proto_offsets from
// pandaproxy::schema_registry with a slightly different interface. Unify these.
get_proto_offsets_result get_schema_proto_offsets(iobuf& buf) noexcept {
    auto header = get_value_schema_id(buf);
    if (!header.has_value()) {
        return header.error();
    }
    proto_schema_message_data result;
    result.schema_id = header.value().schema_id;
    auto pb_offsets_res = get_proto_offsets(header.value().shared_message_data);
    if (pb_offsets_res.has_error()) {
        return pb_offsets_res.error();
    }
    result.protobuf_offsets = std::move(
      pb_offsets_res.value().protobuf_offsets);
    result.shared_message_data = std::move(
      pb_offsets_res.value().shared_message_data);
    return result;
}

result<proto_offsets_message_data, get_schema_error>
get_proto_offsets(iobuf& buf) noexcept {
    proto_offsets_message_data result;
    iobuf_const_parser parser(buf.share(0, buf.size_bytes()));

    // The encoding is a length, followed by indexes into the file or message.
    // Each number is a zigzag encoded integer.
    try {
        auto [offset_count, bytes_read] = parser.read_varlong();
        if (!bytes_read) {
            return get_schema_error::bad_varint;
        }
        // Reject more offsets than bytes remaining; it's not possible
        if (static_cast<size_t>(offset_count) > parser.bytes_left()) {
            return get_schema_error::not_enough_bytes;
        }
        if (offset_count == 0) {
            result.protobuf_offsets.push_back(0);
            result.shared_message_data = buf.share(
              parser.bytes_consumed(), parser.bytes_left());
            return result;
        }
        result.protobuf_offsets.resize(offset_count);
        for (auto& o : result.protobuf_offsets) {
            if (parser.bytes_left() == 0) {
                return get_schema_error::not_enough_bytes;
            }
            std::tie(o, bytes_read) = parser.read_varlong();
            if (!bytes_read) {
                return get_schema_error::bad_varint;
            }
        }

        result.shared_message_data = buf.share(
          parser.bytes_consumed(), parser.bytes_left());
        return result;
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Error parsing protobuf offsets: {}",
          std::current_exception());
        return get_schema_error::not_enough_bytes;
    }
}

} // namespace datalake
