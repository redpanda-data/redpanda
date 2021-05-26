/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>
#include <fmt/format.h>

namespace pandaproxy::schema_registry {

enum class schema_type { avro = 0, json, protobuf };

inline constexpr std::string_view to_string_view(schema_type e) {
    switch (e) {
    case schema_type::avro:
        return "AVRO";
    case schema_type::json:
        return "JSON";
    case schema_type::protobuf:
        return "PROTOBUF";
    }
    return "{invalid}";
}

///\brief A subject is the name under which a schema is registered.
///
/// Typically it will be "<topic>-key" or "<topic>-value".
using subject = named_type<ss::sstring, struct subject_tag>;
static const subject invalid_subject{};

///\brief The definition of the schema.
///
/// TODO(Ben): Make this cheap to copy
using schema_definition = named_type<ss::sstring, struct schema_definition_tag>;
static const schema_definition invalid_schema_definition{};

///\brief The version of the schema registered with a subject.
///
/// A subject may evolve its schema over time. Each version is associated with a
/// schema_id.
using schema_version = named_type<int32_t, struct schema_version_tag>;
static constexpr schema_version invalid_schema_version{-1};

///\brief Globally unique identifier for a schema.
using schema_id = named_type<int32_t, struct schema_id_tag>;
static constexpr schema_id invalid_schema_id{-1};

///\brief Complete description of a schema.
struct schema {
    schema(schema_id id, schema_type type, schema_definition definition)
      : id{id}
      , type{type}
      , definition{std::move(definition)} {}

    schema_id id;
    schema_type type;
    schema_definition definition;
};

///\brief A mapping of version and schema id for a subject.
struct subject_version_id {
    subject_version_id(schema_version version, schema_id id)
      : version{version}
      , id{id} {}

    schema_version version;
    schema_id id;
    bool deleted{false};
};

///\brief All schema versions for a subject.
using subject_versions = std::vector<subject_version_id>;

///\brief Complete description of a subject and schema for a version.
struct subject_schema {
    subject sub{invalid_subject};
    schema_version version{invalid_schema_version};
    schema_id id{invalid_schema_id};
    schema_type type{schema_type::avro};
    schema_definition definition{invalid_schema_definition};
    bool deleted{false};
};

} // namespace pandaproxy::schema_registry
