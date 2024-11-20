// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <string>
#include <variant>

namespace cloud_io {

/// S3 compatible storage requires the scheme to identify the provider.
/// The hostname is usually configured offband.
struct s3_compat_provider {
    std::string scheme;
};

/// Azure Blob Storage/ADLS Gen2 requires an account name to build the URI.
struct abs_provider {
    std::string account_name;
};

/// Provider includes information that used together with the bucket name and
/// the path to create a URI that can be used to access the object by 3rd party
/// systems.
///
/// In particular, we use this to create URIs that are written in Iceberg
/// catalog/manifests files and then are used by 3rd party tools to read data.
using provider = std::variant<s3_compat_provider, abs_provider>;

}; // namespace cloud_io
