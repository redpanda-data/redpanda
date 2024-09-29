/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "schema/utils.h"

#include "bytes/iobuf_parser.h"
#include "pandaproxy/schema_registry/types.h"

#include <optional>
#include <stdexcept>

namespace schema {

result<pandaproxy::schema_registry::schema_id, schema_id_error>
parse_schema_id(const iobuf& buf) {
    if (buf.size_bytes() < sizeof(uint8_t) + sizeof(uint32_t)) {
        // A record with a schema id must have at least 5 bytes:
        // 1 byte magic + 4 bytes id
        return schema_id_error::not_enough_bytes;
    }
    iobuf_const_parser parser(buf);

    auto magic = parser.consume_type<uint8_t>();
    if (magic != 0) {
        return schema_id_error::invalid_magic;
    }
    auto schema_id = parser.consume_type<int32_t>();
    return pandaproxy::schema_registry::schema_id{schema_id};
}

const std::error_category& error_category() {
    static schema_id_error_category e;
    return e;
}
} // namespace schema
