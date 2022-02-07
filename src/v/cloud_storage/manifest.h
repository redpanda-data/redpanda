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

#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "json/document.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "seastarx.h"
#include "serde/serde.h"

#include <compare>

namespace cloud_storage {

/// Information contained inside the partition manifest path
struct manifest_path_components {
    std::filesystem::path _origin;
    model::ns _ns;
    model::topic _topic;
    model::partition_id _part;
    model::initial_revision_id _rev;
};

std::ostream& operator<<(std::ostream& s, const manifest_path_components& c);

/// Parse partition manifest path and return components
std::optional<manifest_path_components>
get_manifest_path_components(const std::filesystem::path& path);

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

/// Selected prefixes used to store manifest files
static constexpr std::array<std::string_view, 16> manifest_prefixes = {{
  "00000000",
  "10000000",
  "20000000",
  "30000000",
  "40000000",
  "50000000",
  "60000000",
  "70000000",
  "80000000",
  "90000000",
  "a0000000",
  "b0000000",
  "c0000000",
  "d0000000",
  "e0000000",
  "f0000000",
}};

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

/// Manifest file stored in S3
class partition_manifest final : public base_manifest {
public:
    struct segment_meta {
        using value_t = segment_meta;
        static constexpr serde::version_t redpanda_serde_version = 1;
        static constexpr serde::version_t redpanda_serde_compat_version = 0;

        bool is_compacted;
        size_t size_bytes;
        model::offset base_offset;
        model::offset committed_offset;
        model::timestamp base_timestamp;
        model::timestamp max_timestamp;
        model::offset delta_offset;

        model::initial_revision_id ntp_revision;
        model::term_id archiver_term;

        auto operator<=>(const segment_meta&) const = default;
    };

    /// Segment key in the maifest
    using key = segment_name_components;
    using value = segment_meta;
    using segment_map = absl::btree_map<key, value>;
    using const_iterator = segment_map::const_iterator;
    using const_reverse_iterator = segment_map::const_reverse_iterator;

    /// Create empty manifest that supposed to be updated later
    partition_manifest();

    /// Create manifest for specific ntp
    explicit partition_manifest(model::ntp ntp, model::initial_revision_id rev);

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const override;

    /// Get NTP
    const model::ntp& get_ntp() const;

    // Get last offset
    const model::offset get_last_offset() const;

    /// Get revision
    model::initial_revision_id get_revision_id() const;

    remote_segment_path
    generate_segment_path(const key&, const segment_meta&) const;

    /// Return iterator to the begining(end) of the segments list
    const_iterator begin() const;
    const_iterator end() const;
    const_reverse_iterator rbegin() const;
    const_reverse_iterator rend() const;
    size_t size() const;

    /// Check if the manifest contains particular segment
    bool contains(const key& key) const;
    bool contains(const segment_name& name) const;

    /// Add new segment to the manifest
    bool add(const key& key, const segment_meta& meta);
    bool add(const segment_name& name, const segment_meta& meta);

    /// Get segment if available or nullopt
    const segment_meta* get(const key& key) const;
    const segment_meta* get(const segment_name& name) const;
    /// Find element of the manifest by offset
    const_iterator find(model::offset o) const;

    /// Get insert iterator for segments set
    std::insert_iterator<segment_map> get_insert_iterator();

    /// Return new manifest that contains only those segments that present
    /// in local manifest and not found in 'remote_set'.
    ///
    /// \param remote_set the manifest to compare to
    /// \return manifest with segments that doesn't present in 'remote_set'
    partition_manifest difference(const partition_manifest& remote_set) const;

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(ss::input_stream<char> is) override;

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    serialized_json_stream serialize() const override;

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    void serialize(std::ostream& out) const;

    /// Compare two manifests for equality
    bool operator==(const partition_manifest& other) const = default;

    /// Remove segment record from manifest
    ///
    /// \param name is a segment name
    /// \return true on success, false on failure (no such segment)
    bool delete_permanently(const key& name);

    manifest_type get_manifest_type() const override {
        return manifest_type::partition;
    };

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void update(const json::Document& m);

    model::ntp _ntp;
    model::initial_revision_id _rev;
    segment_map _segments;
    model::offset _last_offset;
};

std::ostream& operator<<(std::ostream& o, const partition_manifest::key& k);
} // namespace cloud_storage
