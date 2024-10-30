// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace iceberg {

static constexpr auto s3_scheme = "s3";

/**
 * Type representing a canonical object store URI. This is an absolute path to
 * an object in an object storage together with the protocol scheme.
 *
 * Examples:
 *
 * s3://bucket-name/path/to/an/object.bin
 * abs://container/foo/bar/baz/
 */
using uri = named_type<ss::sstring, struct iceberg_uri_tag>;

/**
 * Parses a path from valid Iceberg URI. It uses regex to parse out the path,
 * throws an exception if URI is malformed.
 */
std::filesystem::path path_from_uri(const uri&);

/**
 * Builds uri from protocol scheme, bucket and object path
 */
uri make_uri(
  const ss::sstring& bucket,
  const std::filesystem::path& path,
  std::string_view scheme = s3_scheme);

}; // namespace iceberg
