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
namespace schema {

// Extract schema information from the given buffer;
// If there is no schema information, schema_id will be nullopt
std::optional<pandaproxy::schema_registry::schema_id>
parse_schema_id(const iobuf& buf);

} // namespace schema
