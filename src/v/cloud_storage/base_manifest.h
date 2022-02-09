/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <compare>

namespace cloud_storage {

/// Information contained inside the partition manifest path
struct partition_manifest_path_components {
    std::filesystem::path _origin;
    model::ns _ns;
    model::topic _topic;
    model::partition_id _part;
    model::initial_revision_id _rev;
};

std::ostream&
operator<<(std::ostream& s, const partition_manifest_path_components& c);

/// Parse partition manifest path and return components
std::optional<partition_manifest_path_components>
get_partition_manifest_path_components(const std::filesystem::path& path);

struct segment_name_components {
    model::offset base_offset;
    model::term_id term;

    auto operator<=>(const segment_name_components&) const = default;
};

std::optional<segment_name_components>
parse_segment_name(const segment_name& name);

/// Segment file name in S3
remote_segment_path generate_remote_segment_path(
  const model::ntp&,
  model::initial_revision_id,
  const segment_name&,
  model::term_id archiver_term);

/// Generate correct S3 segment name based on term and base offset
segment_name generate_segment_name(model::offset o, model::term_id t);

remote_manifest_path
generate_partition_manifest_path(const model::ntp&, model::initial_revision_id);

struct serialized_json_stream {
    ss::input_stream<char> stream;
    size_t size_bytes;
};

enum class manifest_type {
    topic,
    partition,
};

class base_manifest {
public:
    virtual ~base_manifest() = default;

    /// Update manifest file from input_stream (remote set)
    virtual ss::future<> update(ss::input_stream<char> is) = 0;

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    virtual serialized_json_stream serialize() const = 0;

    /// Manifest object name in S3
    virtual remote_manifest_path get_manifest_path() const = 0;

    /// Get manifest type
    virtual manifest_type get_manifest_type() const = 0;

    /// Compare two manifests for equality
    bool operator==(const base_manifest& other) const = default;
};
} // namespace cloud_storage
