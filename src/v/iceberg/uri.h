/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_io/provider.h"
#include "model/fundamental.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <filesystem>
#include <optional>

namespace iceberg {

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

/// An adapter to convert between Iceberg URI and filesystem path to
/// interoperate with our cloud_io::remote abstraction.
///
/// Right know we rely solely on the information from cloud_io::remote to
/// determine the provider, but in the future we can extend this to support
/// additional iceberg-specific overrides.
class uri_converter {
public:
    explicit uri_converter(cloud_io::provider p);

public:
    /// Create a URI from a bucket and key which 3rd party clients can use to
    /// access the object.
    ///
    /// This is currently used by Iceberg to generate URIs which are written in
    /// Iceberg catalog/manifests files and then are used by 3rd party tools.
    uri to_uri(
      const cloud_storage_clients::bucket_name&,
      const std::filesystem::path&) const;

    /// Parse key from URI. This will succeed only if URI scheme is compatible
    /// with the currently configured cloud storage backend and the bucket name
    /// matches.
    std::optional<std::filesystem::path>
    from_uri(const cloud_storage_clients::bucket_name&, const uri&) const;

private:
    cloud_io::provider _provider;
};

}; // namespace iceberg
