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
#include "model/metadata.h"
#include "model/timestamp.h"
#include "serde/serde.h"
#include "utils/tracking_allocator.h"

#include <seastar/core/shared_ptr.hh>

#include <deque>

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

// This structure can be impelenented
// to allow access to private fields of the manifest.
struct partition_manifest_accessor;

/// Manifest file stored in S3
class partition_manifest final : public base_manifest {
    friend struct partition_manifest_accessor;

public:
    using segment_meta = cloud_storage::segment_meta;

    /// Compact representation of the segment_meta
    /// that can be used to generate a segment path in S3
    struct lw_segment_meta {
        model::initial_revision_id ntp_revision;
        model::offset base_offset;
        model::offset committed_offset;
        /// Archiver term, same as in segment_meta (can be set to -inf)
        model::term_id archiver_term;
        /// Term of the segment itself
        model::term_id segment_term;
        /// Size of the segment if segment_name_format::v2 is used,
        /// or 0 otherwise. The sname_format field is not added explicitly
        /// but its value is encoded using size-bytes field.
        size_t size_bytes;

        auto operator<=>(const lw_segment_meta&) const = default;

        static lw_segment_meta convert(const segment_meta& m);
        static segment_meta convert(const lw_segment_meta& m);
    };

    /// Segment key in the maifest
    using key = model::offset;
    using value = segment_meta;
    using segment_map = util::mem_tracked::map_t<absl::btree_map, key, value>;
    using replaced_segments_list = std::vector<lw_segment_meta>;
    using const_iterator = segment_map::const_iterator;
    using const_reverse_iterator = segment_map::const_reverse_iterator;

    /// Generate segment name to use in the cloud
    static segment_name generate_remote_segment_name(const value& val);
    /// Generate segment path to use in the cloud
    static remote_segment_path
    generate_remote_segment_path(const model::ntp& ntp, const value& val);
    /// Generate segment path to use locally
    static local_segment_path
    generate_local_segment_path(const model::ntp& ntp, const value& val);

    /// Create empty manifest that supposed to be updated later
    partition_manifest();

    /// Create manifest for specific ntp with memory tracked on a child tracker
    /// of `partition_mem_tracker`.
    explicit partition_manifest(
      model::ntp ntp,
      model::initial_revision_id rev,
      ss::shared_ptr<util::mem_tracker> partition_mem_tracker = nullptr);

    template<class segment_t>
    partition_manifest(
      model::ntp ntp,
      model::initial_revision_id rev,
      ss::shared_ptr<util::mem_tracker> manifest_mem_tracker,
      model::offset so,
      model::offset lo,
      model::offset lco,
      model::offset insync,
      const fragmented_vector<segment_t>& segments,
      const fragmented_vector<segment_t>& replaced)
      : _ntp(std::move(ntp))
      , _rev(rev)
      , _mem_tracker(std::move(manifest_mem_tracker))
      , _segments(
          util::mem_tracked::map<absl::btree_map, key, value>(_mem_tracker))
      , _last_offset(lo)
      , _start_offset(so)
      , _last_uploaded_compacted_offset(lco)
      , _insync_offset(insync) {
        for (auto nm : replaced) {
            auto key = parse_segment_name(nm.name);
            vassert(
              key.has_value(),
              "can't parse name of the replaced segment in the manifest '{}'",
              nm.name);
            nm.meta.segment_term = key->term;
            _replaced.push_back(lw_segment_meta::convert(nm.meta));
        }
        for (auto nm : segments) {
            auto maybe_key = parse_segment_name(nm.name);
            vassert(
              maybe_key.has_value(),
              "can't parse name of the segment in the manifest '{}'",
              nm.name);
            nm.meta.segment_term = maybe_key->term;
            _segments.insert(std::make_pair(nm.meta.base_offset, nm.meta));
        }
    }

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const override;

    /// Get NTP
    const model::ntp& get_ntp() const;

    // Get last offset
    const model::offset get_last_offset() const;

    // Get insync offset of the archival_metadata_stm
    //
    // The offset is an offset of the last applied record with the
    // archival_metadata_stm command.
    const model::offset get_insync_offset() const;

    // Move insync offset forward
    // The method is supposed to be called by the archival_metadata_stm after
    // applying all commands in the record batch to the manifest.
    void advance_insync_offset(model::offset o);

    /// Get starting offset
    std::optional<model::offset> get_start_offset() const;
    std::optional<kafka::offset> get_start_kafka_offset() const;

    /// Get last uploaded compacted offset
    model::offset get_last_uploaded_compacted_offset() const;

    /// Get revision
    model::initial_revision_id get_revision_id() const;

    /// Find the earliest segment that has max timestamp >= t
    std::optional<std::reference_wrapper<const segment_meta>>
    timequery(model::timestamp t) const;

    remote_segment_path generate_segment_path(const segment_meta&) const;
    remote_segment_path generate_segment_path(const lw_segment_meta&) const;

    /// Return an iterator to the first addressable segment (i.e. base offset
    /// is greater than or equal to the start offset). If no such segment
    /// exists, return the end iterator.
    const_iterator first_addressable_segment() const;

    /// Return iterator to the begining(end) of the segments list
    const_iterator begin() const;
    const_iterator end() const;
    std::optional<segment_meta> last_segment() const;
    size_t size() const;
    bool empty() const;

    // Return the tracked amount of memory associated with the segments in this
    // manifest, or 0 if the memory is not being tracked.
    size_t segments_metadata_bytes() const;

    // Computes the size in bytes of all segments available to clients
    // (i.e. all segments after and including the segment that starts at
    // the current _start_offset).
    uint64_t compute_cloud_log_size() const;
    uint64_t cloud_log_size() const;

    /// Check if the manifest contains particular segment
    bool contains(const key& key) const;
    bool contains(const segment_name& name) const;

    /// Add new segment to the manifest
    bool add(const key& key, const segment_meta& meta);
    bool add(const segment_name& name, const segment_meta& meta);

    /// \brief Truncate the manifest (remove entries from the manifest)
    ///
    /// \note version with parameter advances start offset before truncating
    /// \param starting_rp_offset is a new starting offset of the manifest
    /// \return manifest that contains only removed segments
    partition_manifest truncate(model::offset starting_rp_offset);
    partition_manifest truncate();

    /// \brief Set start offset without removing any data from the
    /// manifest.
    ///
    /// Only allows start_offset to move forward
    /// and can only be placed on a segment boundary (should
    /// be equal to base_offset of one of the segments).
    /// Empty manfest has start_offset set to model::offset::min()
    /// \returns true if start offset was moved
    bool advance_start_offset(model::offset start_offset);

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
    ss::future<serialized_json_stream> serialize() const override;

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    void serialize(std::ostream& out) const;

    /// Compare two manifests for equality. Don't compare the mem_tracker.
    bool operator==(const partition_manifest& other) const {
        return _ntp == other._ntp && _rev == other._rev
               && _segments == other._segments
               && _last_offset == other._last_offset
               && _start_offset == other._start_offset
               && _last_uploaded_compacted_offset
                    == other._last_uploaded_compacted_offset
               && _insync_offset == other._insync_offset
               && _replaced == other._replaced;
    }

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
    const_iterator segment_containing(kafka::offset o) const;

    // Return collection of segments that were replaced in lightweight format.
    std::vector<partition_manifest::lw_segment_meta>
    lw_replaced_segments() const;

    /// Return collection of segments that were replaced by newer segments.
    std::vector<segment_meta> replaced_segments() const;

    /// Return the number of replaced segments currently awaiting deletion.
    size_t replaced_segments_count() const;

    /// Removes all replaced segments from the manifest.
    /// Method 'replaced_segments' will return empty value
    /// after the call.
    void delete_replaced_segments();

    ss::shared_ptr<util::mem_tracker> mem_tracker() { return _mem_tracker; }

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void update(partition_manifest_handler&& handler);

    /// Move segments from _segments to _replaced
    void move_aligned_offset_range(
      model::offset begin_inclusive, model::offset end_inclusive);

    friend class serialization_cursor_data_source;

    struct serialization_cursor;
    using serialization_cursor_ptr = ss::lw_shared_ptr<serialization_cursor>;
    /// Make serialization cursor
    serialization_cursor_ptr make_cursor(std::ostream& out) const;
    /// Write prologue
    void serialize_begin(serialization_cursor_ptr cursor) const;
    /// Write next chunk of body
    void serialize_segments(serialization_cursor_ptr cursor) const;
    /// Write next chunk of body
    void serialize_replaced(serialization_cursor_ptr cursor) const;
    /// Write epilogue
    void serialize_end(serialization_cursor_ptr cursor) const;
    /// Serialize normal manifest entry
    void serialize_segment_meta(
      const segment_meta& meta, serialization_cursor_ptr cursor) const;
    /// Serialize removed manifest entry
    void serialize_removed_segment_meta(
      const lw_segment_meta& meta, serialization_cursor_ptr cursor) const;

    model::ntp _ntp;
    model::initial_revision_id _rev;

    // Tracker of memory for this manifest.
    // Currently only tracks memory allocated for `_segments`.
    ss::shared_ptr<util::mem_tracker> _mem_tracker;

    segment_map _segments;
    /// Collection of replaced but not yet removed segments
    replaced_segments_list _replaced;
    model::offset _last_offset;
    model::offset _start_offset;
    model::offset _last_uploaded_compacted_offset;
    model::offset _insync_offset;
};

} // namespace cloud_storage
