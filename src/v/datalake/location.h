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

#include "datalake/base_types.h"
#include "iceberg/uri.h"
#include "model/fundamental.h"

#include <optional>

namespace datalake {

class scoped_location {
private:
    friend class location_provider;
    friend struct testing_accessor;

    class ctor_key {
        friend class location_provider;
        friend struct testing_accessor;
        ctor_key() = default;
    };

public:
    /// Construct a scoped location from a scope and a key.
    ///
    /// As a user, you most likely want to use the \c location_provider to
    /// create scoped locations.
    ///
    /// Preconditions will not be checked in the constructor. Ensure that the
    /// scope and key are valid before calling this constructor.
    ///
    /// \pre \c scope must not end with a trailing slash.
    /// \pre \c key must not start or end with a slash.
    scoped_location(ctor_key, iceberg::uri scope, std::filesystem::path key)
      : scope_(std::move(scope))
      , scope_key_(std::move(key)) {}

public:
    /// Append path to the current path with a slash delimiter.
    scoped_location append_path(const std::filesystem::path& path) const;

    /// Replace path component with the given path. The new path must be
    /// rooted at the same scope. I.e. the key part of the scope must be a
    /// prefix of the new path.
    ///
    /// \throws std::invalid_argument if the new path does not start with the
    /// key part of the scope.
    scoped_location replace_path(const std::filesystem::path& path) const;

    /// Returns URI of the scoped location. For external presentation.
    iceberg::uri to_uri() const;

    /// Returns key of the scoped location. For internal use. I.e.
    /// uploading/downloading objects.
    std::filesystem::path to_key() const;

private:
    iceberg::uri scope_;

    // Key part extracted from the scope.
    // Invariant: key is a suffix of scope.
    std::filesystem::path scope_key_;

    // Path relative to scope. Potentially empty.
    std::filesystem::path path_;
};

class location_provider {
public:
    location_provider(
      cloud_io::provider provider, cloud_storage_clients::bucket_name bucket)
      : uri_converter_(std::move(provider))
      , bucket_(std::move(bucket)) {}

public:
    std::optional<remote_path> from_uri(const iceberg::uri& uri) const {
        auto maybe_path = uri_converter_.from_uri(bucket_, uri);
        if (!maybe_path) {
            return std::nullopt;
        }

        return remote_path(std::move(*maybe_path));
    }

    /// Convert an URI to a scoped location. This is almost always a table
    /// location relative to which we want to construct other URIs. I.e. URIs
    /// for manifests, data files, etc.
    std::optional<scoped_location>
    make_scoped_location(const iceberg::uri& uri) const;

private:
    iceberg::uri_converter uri_converter_;
    cloud_storage_clients::bucket_name bucket_;
};

} // namespace datalake
