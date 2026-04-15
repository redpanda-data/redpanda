/*
 * Copyright 2025 Redpanda Data, Inc.
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

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <optional>

namespace pandaproxy::schema_registry {

class sharded_store;

/// The well-known metadata property key that activates iceberg compat
/// checking for a subject.
inline constexpr std::string_view iceberg_compat_metadata_key
  = "redpanda.iceberg.compatible";

/// Checks whether registering candidate_schema for the given subject
/// would produce a valid iceberg schema evolution history.
///
/// Reads the metadata flag from candidate_def. If not set, returns nullopt
/// (check skipped). If set, fetches all versions (including soft-deleted)
/// for the subject, converts each to iceberg struct_type, and runs
/// simulate_evolution to validate the full lineage.
///
/// @param store  the sharded schema registry store
/// @param sub    the subject being registered to
/// @param candidate_def  the schema definition being registered
/// @return nullopt if check passes or flag not set; error string on failure
ss::future<std::optional<ss::sstring>> check_iceberg_compatibility(
  sharded_store& store,
  const context_subject& sub,
  const schema_definition& candidate_def);

} // namespace pandaproxy::schema_registry
