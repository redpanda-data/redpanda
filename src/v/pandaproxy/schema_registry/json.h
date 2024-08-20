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

#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/schema_registry/types.h"

namespace pandaproxy::schema_registry {

ss::future<json_schema_definition>
make_json_schema_definition(sharded_store& store, canonical_schema schema);

ss::future<canonical_schema> make_canonical_json_schema(
  sharded_store& store, unparsed_schema def, normalize norm = normalize::no);

compatibility_result check_compatible(
  const json_schema_definition& reader,
  const json_schema_definition& writer,
  verbose is_verbose = verbose::no);

} // namespace pandaproxy::schema_registry
