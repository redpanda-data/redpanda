/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <compare>

namespace cloud_storage {
struct serialized_json_stream {
    ss::input_stream<char> stream;
    size_t size_bytes;
};

enum class manifest_type {
    topic,
    partition,
    tx_range,
};

class base_manifest {
public:
    virtual ~base_manifest() = default;

    /// Update manifest file from input_stream (remote set)
    virtual ss::future<> update(ss::input_stream<char> is) = 0;

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    virtual ss::future<serialized_json_stream> serialize() const = 0;

    /// Manifest object name in S3
    virtual remote_manifest_path get_manifest_path() const = 0;

    /// Get manifest type
    virtual manifest_type get_manifest_type() const = 0;

    /// Compare two manifests for equality
    bool operator==(const base_manifest& other) const = default;
};
} // namespace cloud_storage
