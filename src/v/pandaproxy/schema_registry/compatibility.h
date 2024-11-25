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
#include "vassert.h"

#include <fmt/format.h>

#include <filesystem>
#include <string_view>
#include <vector>

/**
 * compatibility.h
 *
 * Support classes for tracking, accumulating, and emitting formatted error
 * messages while checking compatibility of avro & protobuf schemas.
 */

namespace pandaproxy::schema_registry {

enum class avro_incompatibility_type {
    name_mismatch = 0,
    fixed_size_mismatch,
    missing_enum_symbols,
    reader_field_missing_default_value,
    type_mismatch,
    missing_union_branch,
    unknown,
};

/**
 * avro_incompatibility - A single incompatibility between Avro schemas.
 *
 * Encapsulates:
 *   - the path to the location of the incompatibility in the _writer_ schema
 *   - the type of incompatibility
 *   - any additional context for the incompatibility (e.g. a field name)
 *
 * Primary interface is `describe`, which combines the contained info into
 * a format string which can then be interpolated with identifying info for
 * the reader and writer schema in the request handler.
 */
class avro_incompatibility {
public:
    using Type = avro_incompatibility_type;
    avro_incompatibility(
      std::filesystem::path path, Type type, std::string_view info)
      : _path(std::move(path))
      , _type(type)
      , _additional_info(info) {}

    avro_incompatibility(std::filesystem::path path, Type type)
      : avro_incompatibility(std::move(path), type, "") {}

    ss::sstring describe() const;

private:
    friend std::ostream&
    operator<<(std::ostream& os, const avro_incompatibility& v);

    friend bool
    operator==(const avro_incompatibility&, const avro_incompatibility&)
      = default;

    // Useful for unit testing
    template<typename H>
    friend H AbslHashValue(H h, const avro_incompatibility& e) {
        return H::combine(
          std::move(h), e._path.string(), e._type, e._additional_info);
    }

    std::filesystem::path _path;
    Type _type;
    ss::sstring _additional_info;
};

enum class proto_incompatibility_type {
    message_removed = 0,
    field_kind_changed,
    field_scalar_kind_changed,
    field_named_type_changed,
    required_field_added,
    required_field_removed,
    oneof_field_removed,
    multiple_fields_moved_to_oneof,
    unknown,
};

/**
 * proto_incompatibility - A single incompatibility between Protobuf schemas.
 *
 * Encapsulates:
 *   - the path to the location of the incompatibility in the _writer_ schema
 *   - the type of incompatibility
 *
 * Primary interface is `describe`, which combines the contained info into
 * a format string which can then be interpolated with identifying info for
 * the reader and writer schemas in the request handler.
 */
class proto_incompatibility {
public:
    using Type = proto_incompatibility_type;
    proto_incompatibility(std::filesystem::path path, Type type)
      : _path(std::move(path))
      , _type(type) {}

    ss::sstring describe() const;
    Type type() const { return _type; }

private:
    friend std::ostream&
    operator<<(std::ostream& os, const proto_incompatibility& v);

    friend bool
    operator==(const proto_incompatibility&, const proto_incompatibility&)
      = default;

    // Helpful for unit testing
    template<typename H>
    friend H AbslHashValue(H h, const proto_incompatibility& e) {
        return H::combine(std::move(h), e._path.string(), e._type);
    }

    std::filesystem::path _path;
    Type _type;
};

/**
 * raw_compatibility_result - A collection of unformatted proto or avro
 * incompatibilities. Its purpose is twofold:
 *   - Provide an abstracted way to accumulate incompatibilities across
 *     a recursive call chain. The `merge` function makes this simple
 *     and seeks to avoid excessive copying.
 *   - Provide a (type-constrained) generic means to process raw
 *     incompatibilities into formatted error messages.
 */
class raw_compatibility_result {
    using schema_incompatibility
      = std::variant<avro_incompatibility, proto_incompatibility>;

public:
    raw_compatibility_result() = default;

    template<typename T, typename... Args>
    requires std::constructible_from<T, Args&&...>
             && std::convertible_to<T, schema_incompatibility>
    auto emplace(Args&&... args) {
        return _errors.emplace_back(
          std::in_place_type<T>, std::forward<Args>(args)...);
    }

    compatibility_result operator()(verbose is_verbose) &&;

    // Move the contents of other into the errors vec of this
    void merge(raw_compatibility_result&& other);

    bool has_error() const { return !_errors.empty(); }

private:
    std::vector<schema_incompatibility> _errors{};
};

} // namespace pandaproxy::schema_registry
