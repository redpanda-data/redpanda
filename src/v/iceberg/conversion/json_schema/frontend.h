/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/conversion/json_schema/ir.h"
#include "json/document.h"

#include <memory>
#include <vector>

namespace iceberg::conversion::json_schema {

/// \brief An exception that is thrown when compiler does not yet support a
/// specific JSON Schema feature.
class unsupported_feature_error : public std::runtime_error {
public:
    explicit unsupported_feature_error(const std::string& feature)
      : std::runtime_error(
          fmt::format("Unsupported JSON Schema feature: {}", feature)) {}
};

/// \brief Frontend compiler for JSON Schema that generates an intermediate
/// representation (IR) from a schema.
class frontend {
    class frontend_impl;

public:
    frontend();
    ~frontend();

public:
    /// \brief Compiles a JSON Schema document into an intermediate
    /// representation (IR).
    ///
    /// \param doc The JSON Schema document to compile. It must be a valid JSON
    /// Schema document.
    ///
    /// \param initial_base_uri The default base URI to use for the schema. This
    /// is used if the root schema resource does not specify an "$id". See
    /// https://json-schema.org/draft/2020-12/json-schema-core#name-initial-base-uri
    /// \param default_dialect The default dialect to use for the schema if it
    /// does not specify one. If not provided and the schema does not specify a
    /// dialect, the compiler will throw an error. See
    /// https://json-schema.org/draft/2020-12/json-schema-core#section-8.1.1-4
    ///
    /// \return The compiled intermediate representation.
    schema compile(
      const json::Document& doc,
      const std::string& initial_base_uri,
      std::optional<dialect> default_dialect) const;

    /// \brief Like compile(), but with external schemas pre-registered.
    ///
    /// Each external schema entry is (uri_key, document*). The documents
    /// are compiled into resource contexts and registered before $ref
    /// resolution, so external references resolve naturally.
    using external_schemas_t
      = std::vector<std::pair<std::string, const json::Document*>>;

    schema compile(
      const json::Document& doc,
      const std::string& initial_base_uri,
      std::optional<dialect> default_dialect,
      const external_schemas_t& external_schemas) const;

private:
    std::unique_ptr<frontend_impl> impl_;
};

}; // namespace iceberg::conversion::json_schema
