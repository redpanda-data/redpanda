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

enum class get_schema_error {
    ok = 0,
    no_schema_id,
    not_enough_bytes,
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

using get_schema_id_result
  = result<pandaproxy::schema_registry::schema_id, get_schema_error>;

// Extract the schema id from a record's value. This simply extracts the id. It
// does not do any validation. Returns std::nullopt if the record does not have
// a schema id.
get_schema_id_result get_value_schema_id(const iobuf& record);

} // namespace datalake

namespace std {
template<>
struct is_error_code_enum<datalake::get_schema_error> : true_type {};
} // namespace std
