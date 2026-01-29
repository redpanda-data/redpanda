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

#include "model/fundamental.h"

#include <seastar/core/sstring.hh>

#include <expected>
#include <vector>

namespace cloud_storage_clients {

/// Components extracted from parsing a bucket_name (which may be a DSN).
///
/// This struct is the result of parsing a bucket_name and provides the
/// components needed for cloud storage API calls and client routing.
///
/// See `cloud_storage_clients::bucket_name` documentation in
/// model/fundamental.h for DSN format details.
struct bucket_name_parts {
    plain_bucket_name name;

    /// Connection parameters extracted from DSN query string.
    /// Empty if no parameters were specified. Vector is efficient here since
    /// the number of parameters is small.
    std::vector<std::pair<ss::sstring, ss::sstring>> params;
};

/// Parses a bucket_name (which may be a DSN) into its component parts.
///
/// Accepts both plain bucket names ("my-bucket") and DSN-format names with
/// query parameters
/// ("my-bucket?region=us-west-2&endpoint=http://localhost:9000").
std::expected<bucket_name_parts, std::string>
parse_bucket_name(const bucket_name& bucket);

} // namespace cloud_storage_clients
