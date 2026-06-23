/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/protocol/api_key_indexed_array.h"

#include <cstddef>

namespace kafka {

// Sizes of the two api_key_indexed_array regions, kept here as literals so this
// lean header -- and its many transitive includers via
// handler_probe.h -> connection_context.h -> request_context.h -- need not pull
// in the request schemata that messages.h includes. messages.h static_asserts
// these against the live request type lists, so they cannot silently drift: add
// an API past these bounds and messages.h fails to compile, pointing back here.
inline constexpr std::size_t standard_api_key_table_size = 67;
inline constexpr std::size_t reserved_api_key_table_size = 1;

/// Kafka-key-indexed dense table sized to the standard and reserved ranges.
template<typename T>
using api_key_table = api_key_indexed_array<
  T,
  standard_api_key_table_size,
  reserved_api_key_table_size>;

} // namespace kafka
