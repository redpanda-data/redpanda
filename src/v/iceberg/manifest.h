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

#include "container/chunked_vector.h"
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

    friend bool
    operator==(const manifest_metadata&, const manifest_metadata&) = default;

    manifest_metadata copy() const;
};

struct manifest {
    manifest_metadata metadata;
    chunked_vector<manifest_entry> entries;
    friend bool operator==(const manifest&, const manifest&) = default;

    manifest copy() const;
};

} // namespace iceberg
