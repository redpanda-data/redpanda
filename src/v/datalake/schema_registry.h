/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "pandaproxy/schema_registry/types.h"

#include <system_error>
#include <type_traits>

class iobuf;

namespace datalake {

// Stores a pair of schema id and the remaining message data after the schema
// prefix. The message data is shared from the original iobuf.
struct schema_message_data {
    pandaproxy::schema_registry::schema_id schema_id;
    iobuf shared_message_data;
};

// Stores the schema_id, protobuf message offsets, and remaining message data.
// The message data is shared from the original iobuf.
struct proto_schema_message_data {
    pandaproxy::schema_registry::schema_id schema_id;
    std::vector<int32_t> protobuf_offsets;
    iobuf shared_message_data;
};

enum class get_schema_error {
    ok = 0,
    no_schema_id,
    not_enough_bytes,
    bad_varint,
};

struct get_schema_error_category : std::error_category {
    const char* name() const noexcept override { return "Get Schema Error"; }

    std::string message(int ev) const override {
        switch (static_cast<get_schema_error>(ev)) {
        case get_schema_error::ok:
            return "Ok";
        case get_schema_error::no_schema_id:
            return "No schema ID";
        case get_schema_error::not_enough_bytes:
            return "Not enough bytes in message";
        case get_schema_error::bad_varint:
            return "Bad encoded value for varint";
        }
    }

    static const std::error_category& error_category() {
        static get_schema_error_category e;
        return e;
    }
};

inline std::error_code make_error_code(get_schema_error e) noexcept {
    return {static_cast<int>(e), get_schema_error_category::error_category()};
}

using get_schema_id_result = result<schema_message_data, get_schema_error>;
using get_proto_offsets_result
  = result<proto_schema_message_data, get_schema_error>;

// Extract the schema id from a record's value. This simply extracts the id. It
// does not validate that the schema exists in the Schema Registry.
get_schema_id_result get_value_schema_id(iobuf& record);
get_proto_offsets_result get_proto_offsets(iobuf& record);

} // namespace datalake

namespace std {
template<>
struct is_error_code_enum<datalake::get_schema_error> : true_type {};
} // namespace std
