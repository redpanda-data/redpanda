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
};

struct manifest {
    manifest_metadata metadata;

    // TODO: the manifest_entry is code-generated with avrogencpp, which is
    // restrictive for a few reasons:
    // - there is no built-in comparator
    // - comparators are difficult to implement since the generated code makes
    //   heavy use of std::any in Avro union types
    // - the manifest_entry r102 (partition) field is incomplete, as it depends
    //   on runtime partitioning and can't be statically generated
    // Rather than using these generated structs, write explicit structs to
    // represent the entries, and only use the Avro structs for serialization.
    chunked_vector<manifest_entry> entries;
};

} // namespace iceberg
