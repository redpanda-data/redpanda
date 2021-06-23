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
#include "utils/string_switch.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <type_traits>

namespace pandaproxy::schema_registry {

using permanent_delete = ss::bool_class<struct delete_tag>;
using include_deleted = ss::bool_class<struct include_deleted_tag>;
using is_deleted = ss::bool_class<struct is_deleted_tag>;

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

enum class schema_type { avro = 0, json, protobuf };

constexpr std::string_view to_string_view(schema_type e) {
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
template<>
constexpr std::optional<schema_type>
from_string_view<schema_type>(std::string_view sv) {
    return string_switch<std::optional<schema_type>>(sv)
      .match(to_string_view(schema_type::avro), schema_type::avro)
      .match(to_string_view(schema_type::json), schema_type::json)
      .match(to_string_view(schema_type::protobuf), schema_type::protobuf)
      .default_match(std::nullopt);
}

inline std::ostream& operator<<(std::ostream& os, const schema_type& v) {
    return os << to_string_view(v);
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
    is_deleted deleted{is_deleted::no};
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
    is_deleted deleted{false};
};

enum class compatibility_level {
    none = 0,
    backward,
    backward_transitive,
    forward,
    forward_transitive,
    full,
    full_transitive,
};

constexpr std::string_view to_string_view(compatibility_level v) {
    switch (v) {
    case compatibility_level::none:
        return "NONE";
    case compatibility_level::backward:
        return "BACKWARD";
    case compatibility_level::backward_transitive:
        return "BACKWARD_TRANSITIVE";
    case compatibility_level::forward:
        return "FORWARD";
    case compatibility_level::forward_transitive:
        return "FORWARD_TRANSITIVE";
    case compatibility_level::full:
        return "FULL";
    case compatibility_level::full_transitive:
        return "FULL_TRANSITIVE";
    }
    return "{invalid}";
}
template<>
constexpr std::optional<compatibility_level>
from_string_view<compatibility_level>(std::string_view sv) {
    return string_switch<std::optional<compatibility_level>>(sv)
      .match(
        to_string_view(compatibility_level::none), compatibility_level::none)
      .match(
        to_string_view(compatibility_level::backward),
        compatibility_level::backward)
      .match(
        to_string_view(compatibility_level::backward_transitive),
        compatibility_level::backward_transitive)
      .match(
        to_string_view(compatibility_level::forward),
        compatibility_level::forward)
      .match(
        to_string_view(compatibility_level::forward_transitive),
        compatibility_level::forward_transitive)
      .match(
        to_string_view(compatibility_level::full), compatibility_level::full)
      .match(
        to_string_view(compatibility_level::full_transitive),
        compatibility_level::full_transitive)
      .default_match(std::nullopt);
}

} // namespace pandaproxy::schema_registry
