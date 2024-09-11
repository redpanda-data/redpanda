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

#include "pandaproxy/schema_registry/types.h"

namespace datalake {
get_schema_id_result get_value_schema_id(const iobuf& buf) {
    // Messages that use the schema registry have a 5-byte prefix:
    // offset 0: magic byte with value 0
    // offsets 1-4: schema ID as big-endian integer
    const uint8_t schema_registry_magic_byte = 0;

    if (buf.size_bytes() < sizeof(uint8_t) + sizeof(int32_t)) {
        return get_schema_error::not_enough_bytes;
    }
    iobuf_const_parser parser(buf);
    auto magic = parser.consume_type<uint8_t>();
    if (magic != schema_registry_magic_byte) {
        return get_schema_error::no_schema_id;
    }
    auto id = parser.consume_be_type<int32_t>();
    return pandaproxy::schema_registry::schema_id{id};
}

} // namespace datalake
