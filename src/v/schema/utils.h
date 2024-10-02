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

#pragma once

#include "pandaproxy/schema_registry/types.h"

#include <optional>
#include <system_error>
namespace schema {

enum class schema_id_error {
    ok = 0,
    not_enough_bytes,
    invalid_magic,
};

struct schema_id_error_category : std::error_category {
    const char* name() const noexcept override { return "Schema ID Error"; }

    std::string message(int ev) const override {
        switch (static_cast<schema_id_error>(ev)) {
        case schema_id_error::ok:
            return "Ok";
        case schema_id_error::not_enough_bytes:
            return "Not enough bytes in message";
        case schema_id_error::invalid_magic:
            return "Invalid magic byte";
        }
    }
    static const std::error_category& error_category() {
        static schema_id_error_category e;
        return e;
    }
};

inline std::error_code make_error_code(schema::schema_id_error e) noexcept {
    return {
      static_cast<int>(e), schema::schema_id_error_category::error_category()};
}

// Extract schema information from the given buffer;
// If there is no schema information, schema_id will be nullopt
result<pandaproxy::schema_registry::schema_id, schema_id_error>
parse_schema_id(const iobuf& buf);

} // namespace schema

namespace std {

template<>
struct is_error_code_enum<schema::schema_id_error> : true_type {};

} // namespace std
