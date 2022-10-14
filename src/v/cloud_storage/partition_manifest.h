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

#include "absl/container/btree_map.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/types.h"
#include "json/document.h"
#include "model/timestamp.h"
#include "serde/serde.h"

namespace cloud_storage {

struct partition_manifest_handler;

/// Information contained inside the partition manifest path
struct partition_manifest_path_components {
    std::filesystem::path _origin;
    model::ns _ns;
    model::topic _topic;
    model::partition_id _part;
    model::initial_revision_id _rev;

    friend std::ostream&
    operator<<(std::ostream& s, const partition_manifest_path_components& c);
};

/// Parse partition manifest path and return components
std::optional<partition_manifest_path_components>
get_partition_manifest_path_components(const std::filesystem::path& path);

struct segment_name_components {
    model::offset base_offset;
    model::term_id term;

    auto operator<=>(const segment_name_components&) const = default;

    friend std::ostream&
    operator<<(std::ostream& o, const segment_name_components& k);
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
segment_name generate_local_segment_name(model::offset o, model::term_id t);

remote_manifest_path
generate_partition_manifest_path(const model::ntp&, model::initial_revision_id);

/// Manifest file stored in S3
class partition_manifest final : public base_manifest {
public:
    struct segment_meta {
        using value_t = segment_meta;
        static constexpr serde::version_t redpanda_serde_version = 2;
        static constexpr serde::version_t redpanda_serde_compat_version = 0;

        bool is_compacted;
        size_t size_bytes;
        model::offset base_offset;
        model::offset committed_offset;
        model::timestamp base_timestamp;
        model::timestamp max_timestamp;
        model::offset_delta delta_offset;

        model::initial_revision_id ntp_revision;
        model::term_id archiver_term;
        /// Term of the segment (included in segment file name)
        model::term_id segment_term;
        /// Offset translation delta at the end of the range
        model::offset_delta delta_offset_end;
        /// Segment name format specifier
        segment_name_format sname_format{segment_name_format::v1};

        auto operator<=>(const segment_meta&) const = default;
    };

    /// Segment key in the maifest
    using key = segment_name_components;
    using value = segment_meta;
    using segment_map = absl::btree_map<key, value>;
    using segment_multimap = absl::btree_multimap<key, value>;
    using const_iterator = segment_map::const_iterator;
    using const_reverse_iterator = segment_map::const_reverse_iterator;

    struct segment_name_meta {
        segment_name name;
        segment_meta meta;
    };

    /// Generate segment name to use in the cloud
    static segment_name
    generate_remote_segment_name(const key& k, const value& val);
    /// Generate segment path to use in the cloud
    static remote_segment_path generate_remote_segment_path(
      const model::ntp& ntp, const key& k, const value& val);
    /// Generate segment path to use locally
    static local_segment_path generate_local_segment_path(
      const model::ntp& ntp, const key& k, const value& val);

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

    /// Get starting offset
    std::optional<model::offset> get_start_offset() const;

    /// Get revision
    model::initial_revision_id get_revision_id() const;

    /// Find the earliest segment that has max timestamp >= t
    std::optional<std::reference_wrapper<const segment_meta>>
    timequery(model::timestamp t) const;

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

    /// \brief Truncate the manifest
    /// \param starting_rp_offset is a new starting offset of the manifest
    /// \return manifest that contains only removed segments
    partition_manifest truncate(model::offset starting_rp_offset);

    /// Get segment if available or nullopt
    const segment_meta* get(const key& key) const;
    const segment_meta* get(const segment_name& name) const;
    /// Find element of the manifest by offset
    const_iterator find(model::offset o) const;

    /// Get insert iterator for segments set
    std::insert_iterator<segment_map> get_insert_iterator();

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

    /// Returns an iterator to the segment containing offset o, such that o >=
    /// segment.base_offset and o <= segment.committed_offset.
    const_iterator segment_containing(model::offset o) const;
    using iterator_pair = std::pair<const_iterator, const_iterator>;

    /// Return iterators that can be used to access
    /// segments that were replaced by newer segments.
    iterator_pair replaced_segments() const;

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void update(partition_manifest_handler&& handler);

    /// Move segments from _segments to _replaced
    void move_aligned_offset_range(
      model::offset begin_inclusive, model::offset end_inclusive);

    model::ntp _ntp;
    model::initial_revision_id _rev;
    segment_map _segments;
    /// Collection of replaced but not yet removed segments
    segment_map _replaced;
    model::offset _last_offset;
};

} // namespace cloud_storage
