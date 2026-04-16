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

#include "json/document.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sstring.hh>

#include <string>
#include <utility>
#include <vector>

namespace pandaproxy::schema_registry {

ss::future<json_schema_definition>
make_json_schema_definition(schema_getter& store, subject_schema schema);

ss::future<subject_schema> make_canonical_json_schema(
  sharded_store& store, subject_schema def, normalize norm = normalize::no);

compatibility_result check_compatible(
  const json_schema_definition& reader,
  const json_schema_definition& writer,
  verbose is_verbose = verbose::no);

const json::Document& document(const json_schema_definition::impl& impl);

// Return a flattened list of all external schemas (including transitive
// dependencies) as (uri_key, document*) pairs. Used by the Iceberg
// integration to pass external schemas to the JSON schema frontend.
std::vector<std::pair<std::string, const json::Document*>>
external_schema_documents(const json_schema_definition::impl& impl);

// Return the root schema's base URI ($id value, or empty if absent).
// Used by the Iceberg integration to pass the correct initial_base_uri
// to the JSON schema frontend so that $ref resolution matches the
// SR's URI normalization.
ss::sstring root_base_uri(const json_schema_definition::impl& impl);

} // namespace pandaproxy::schema_registry
