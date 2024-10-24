// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/partition.h"
#include "iceberg/schema.h"

namespace iceberg {

enum class format_version : uint8_t {
    v1,
    v2,
};

enum class manifest_content_type {
    data,
    deletes,
};

struct manifest_metadata {
    schema schema;
    partition_spec partition_spec;
    format_version format_version;
    manifest_content_type manifest_content_type;

    friend bool operator==(const manifest_metadata&, const manifest_metadata&)
      = default;

    manifest_metadata copy() const;
};

struct manifest {
    manifest_metadata metadata;
    chunked_vector<manifest_entry> entries;
    friend bool operator==(const manifest&, const manifest&) = default;

    manifest copy() const;
};

} // namespace iceberg
