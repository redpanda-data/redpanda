/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/types.h"
#include "bytes/iobuf.h"
#include "cluster/types.h"
#include "json/json.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "seastarx.h"
#include "tristate.h"

#include <seastar/util/bool_class.hh>

#include <absl/container/btree_map.h>
#include <rapidjson/fwd.h>

#include <compare>
#include <iterator>

namespace archival {

struct serialized_json_stream {
    ss::input_stream<char> stream;
    size_t size_bytes;
};

/// Manifest file stored in S3
class manifest final {
public:
    struct segment_meta {
        bool is_compacted;
        size_t size_bytes;
        model::offset base_offset;
        model::offset committed_offset;

        // bool operator==(const segment_meta& other) const = default;
        // bool operator<(const segment_meta& other) const = default;
        auto operator<=>(const segment_meta&) const = default;
    };
    using key = segment_name;
    using value = segment_meta;
    using segment_map = absl::btree_map<key, value>;
    using const_iterator = segment_map::const_iterator;

    /// Create empty manifest that supposed to be updated later
    manifest();

    /// Create manifest for specific ntp
    explicit manifest(model::ntp ntp, model::revision_id rev);

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const;

    /// Segment file name in S3
    remote_segment_path get_remote_segment_path(const segment_name& name) const;

    /// Get NTP
    const model::ntp& get_ntp() const;

    // Get last offset
    const model::offset get_last_offset() const;

    /// Get revision
    model::revision_id get_revision_id() const;

    /// Return iterator to the begining(end) of the segments list
    const_iterator begin() const;
    const_iterator end() const;
    size_t size() const;

    /// Check if the manifest contains particular segment
    bool contains(const segment_name& obj) const;

    /// Add new segment to the manifest
    bool add(const segment_name& key, const segment_meta& meta);

    /// Get segment if available or nullopt
    const segment_meta* get(const segment_name& key) const;

    /// Get insert iterator for segments set
    std::insert_iterator<segment_map> get_insert_iterator();

    /// Return new manifest that contains only those segments that present
    /// in local manifest and not found in 'remote_set'.
    ///
    /// \param remote_set the manifest to compare to
    /// \return manifest with segments that doesn't present in 'remote_set'
    manifest difference(const manifest& remote_set) const;

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(ss::input_stream<char>&& is);

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    serialized_json_stream serialize() const;

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    void serialize(std::ostream& out) const;

    /// Compare two manifests for equality
    bool operator==(const manifest& other) const = default;

    /// Remove segment record from manifest
    ///
    /// \param name is a segment name
    /// \return true on success, false on failure (no such segment)
    bool delete_permanently(const segment_name& name);

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void update(const rapidjson::Document& m);

    model::ntp _ntp;
    model::revision_id _rev;
    segment_map _segments;
    model::offset _last_offset;
};

class topic_manifest {
public:
    /// Create manifest for specific ntp
    explicit topic_manifest(
      const cluster::topic_configuration& cfg, model::revision_id rev);

    /// Create empty manifest that supposed to be updated later
    topic_manifest();

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(ss::input_stream<char>&& is);

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    serialized_json_stream serialize() const;

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    void serialize(std::ostream& out) const;

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const;

    /// Return all possible manifest locations
    std::vector<remote_manifest_path> get_partition_manifests() const;

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void update(const rapidjson::Document& m);

    std::optional<cluster::topic_configuration> _topic_config;
    model::revision_id _rev;
};

} // namespace archival
