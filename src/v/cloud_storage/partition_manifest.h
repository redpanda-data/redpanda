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

#include "cloud_storage/base_manifest.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "segment_meta_cstore.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "utils/fragmented_vector.h"
#include "utils/tracking_allocator.h"

#include <seastar/core/iostream.hh>
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

remote_manifest_path generate_partition_manifest_path(
  const model::ntp&, model::initial_revision_id, manifest_format);

// This structure can be impelenented
// to allow access to private fields of the manifest.
struct partition_manifest_accessor;

/// Manifest file stored in S3
class partition_manifest : public base_manifest {
    friend struct partition_manifest_accessor;

public:
    using segment_meta = cloud_storage::segment_meta;

    /// Compact representation of the segment_meta
    /// that can be used to generate a segment path in S3
    struct lw_segment_meta
      : serde::envelope<
          lw_segment_meta,
          serde::version<0>,
          serde::compat_version<0>> {
        model::initial_revision_id ntp_revision;
        model::offset base_offset;
        model::offset committed_offset;
        /// Archiver term, same as in segment_meta (can be set to -inf)
        model::term_id archiver_term;
        /// Term of the segment itself
        model::term_id segment_term;
        /// Size of the segment if segment_name_format::v2 or later is used,
        /// or 0 otherwise.
        size_t size_bytes;

        segment_name_format sname_format{segment_name_format::v1};

        auto operator<=>(const lw_segment_meta&) const = default;

        static lw_segment_meta convert(const segment_meta& m);
        static segment_meta convert(const lw_segment_meta& m);
    };

    /// Segment key in the maifest
    using key = model::offset;
    using value = segment_meta;
    using segment_map = segment_meta_cstore;
    using spillover_manifest_map = segment_meta_cstore;
    using replaced_segments_list = std::vector<lw_segment_meta>;
    using const_iterator = segment_map::const_iterator;

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
      const fragmented_vector<segment_t>& replaced,
      kafka::offset start_kafka_offset,
      model::offset archive_start_offset,
      model::offset_delta archive_start_offset_delta,
      model::offset archive_clean_offset,
      uint64_t archive_size_bytes,
      const fragmented_vector<segment_t>& spillover)
      : _ntp(std::move(ntp))
      , _rev(rev)
      , _mem_tracker(std::move(manifest_mem_tracker))
      , _segments()
      , _last_offset(lo)
      , _start_offset(so)
      , _last_uploaded_compacted_offset(lco)
      , _insync_offset(insync)
      , _archive_start_offset(archive_start_offset)
      , _archive_start_offset_delta(archive_start_offset_delta)
      , _archive_clean_offset(archive_clean_offset)
      , _start_kafka_offset_override(start_kafka_offset)
      , _archive_size_bytes(archive_size_bytes) {
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
            _segments.insert(nm.meta);

            if (
              nm.meta.base_offset >= _start_offset
              && nm.meta.committed_offset <= _last_offset) {
                _cloud_log_size_bytes += nm.meta.size_bytes;
            }
        }
        for (auto m : spillover) {
            _spillover_manifests.insert(m.meta);
        }
    }

    /// Manifest object name in S3
    std::pair<manifest_format, remote_manifest_path>
    get_manifest_format_and_path() const override;

    remote_manifest_path get_manifest_path(manifest_format fmt) const {
        switch (fmt) {
        case manifest_format::json:
            return get_legacy_manifest_format_and_path().second;
        case manifest_format::serde:
            return get_manifest_format_and_path().second;
        }
    }

    remote_manifest_path get_manifest_path() const override {
        return get_manifest_format_and_path().second;
    }

    /// Manifest object name before feature::cloud_storage_manifest_format_v2
    std::pair<manifest_format, remote_manifest_path>
    get_legacy_manifest_format_and_path() const;

    /// Get NTP
    const model::ntp& get_ntp() const;

    // Get last offset
    model::offset get_last_offset() const;

    // Get the last inclusive Kafka offset
    std::optional<kafka::offset> get_last_kafka_offset() const;

    // Get the last exclusive Kafka offset
    std::optional<kafka::offset> get_next_kafka_offset() const;

    // Get insync offset of the archival_metadata_stm
    //
    // The offset is an offset of the last applied record with the
    // archival_metadata_stm command.
    model::offset get_insync_offset() const;

    // Move insync offset forward
    // The method is supposed to be called by the archival_metadata_stm after
    // applying all commands in the record batch to the manifest.
    void advance_insync_offset(model::offset o);

    /// Return start offset that takes into account start kafka offset of the
    /// manifest, start kafka offset override and start offset of the archive.
    std::optional<kafka::offset> full_log_start_kafka_offset() const;

    /// Get starting offset of the current manifest (doesn't take into account
    /// spillover manifests)
    std::optional<model::offset> get_start_offset() const;

    /// Get starting kafka offset of the current manifest (doesn't take into
    /// account spillover manifests)
    std::optional<kafka::offset> get_start_kafka_offset() const;

    /// Get last uploaded compacted offset
    model::offset get_last_uploaded_compacted_offset() const;

    /// Get revision
    model::initial_revision_id get_revision_id() const;

    /// Find the earliest segment that has max timestamp >= t
    std::optional<segment_meta> timequery(model::timestamp t) const;

    remote_segment_path generate_segment_path(const segment_meta&) const;
    remote_segment_path generate_segment_path(const lw_segment_meta&) const;

    /// Return an iterator to the first addressable segment (i.e. base offset
    /// is greater than or equal to the start offset). If no such segment
    /// exists, return the end iterator.
    const_iterator first_addressable_segment() const;

    /// Return iterator to the beginning of the segments list
    const_iterator begin() const;
    /// Return iterator to the end of the segments list
    const_iterator end() const;
    /// Return last segment in the list
    std::optional<segment_meta> last_segment() const;
    size_t size() const;
    bool empty() const;

    // Return the tracked amount of memory associated with the segments in this
    // manifest, or 0 if the memory is not being tracked.
    size_t segments_metadata_bytes() const;

    /// Return map that contains spillover manifests.
    /// It stores 'segment_meta' objects but the meaning of fields are
    /// different.
    ///
    /// is_compacted - not used
    /// size_bytes - size of all segments in the manifests
    /// base_offset - start_offset of the manifest
    /// committed_offset - last offset of the manifest
    /// base_timestamp - first timestamp stored in the manifest
    /// max_timestamp - last timestamp stored in the manifest
    /// delta_offset - number of config records in all previous
    ///                segments (in prev. manifests)
    /// ntp_revision - initial revision of the partition
    /// archiver_term - term of the first segment stored in the manifest
    /// segment_term - term of the last segment stored in the manifest
    /// delta_offset_end - num. of config records including this manifest
    /// sname_format - always v3
    /// metadata_size_hint - size estimate of the manifest
    const spillover_manifest_map& get_spillover_map() const;

    // Flush c-store write buffer
    void flush_write_buffer();

    // Returns the cached size in bytes of all segments available to clients.
    // This includes both the STM region and the archive.
    uint64_t cloud_log_size() const;

    // Returns the cached size in bytes of all segments within the STM region
    // that are above the start offset. (i.e. all segments after and including
    // the segment that starts at the current _start_offset).
    uint64_t stm_region_size_bytes() const;

    /// Returns cached size of the archive in bytes.
    ///
    /// The segments which contributed to this value are not stored in the
    /// manifest.
    uint64_t archive_size_bytes() const;

    /// Check if the manifest contains particular segment
    bool contains(const key& key) const;
    bool contains(const segment_name& name) const;

    /// Add new segment to the manifest
    bool add(segment_meta meta);
    bool add(const segment_name& name, const segment_meta& meta);

    /// Return 'true' if the segment meta can be added safely
    bool safe_segment_meta_to_add(const segment_meta& meta);

    /// \brief Truncate the manifest (remove entries from the manifest)
    ///
    /// \note version with parameter advances start offset before truncating
    /// The method is used by spillover mechanism to remove segments from
    /// the manifest. Because of that it updates archive_start_offset.
    /// \param starting_rp_offset is a new starting offset of the manifest
    /// \return manifest that contains only removed segments
    partition_manifest truncate(model::offset starting_rp_offset);
    partition_manifest truncate();

    /// Clone the entire manifest
    partition_manifest clone() const;

    /// \brief Truncate the manifest (remove entries from the manifest)
    ///
    /// \note this works the same way as 'truncate' but the 'archive' size
    ///       gets correctly updated.
    /// \param spillover_manifest_meta is a new spillover manifest to add
    void spillover(const segment_meta& spillover_manifest_meta);

    /// Pack manifest metadata into the 'segment_meta' struct
    ///
    /// This mechanism is used by the 'spillover' mechanism. The 'segment_meta'
    /// instances that describe spillover manifests are stored using this
    /// format.
    segment_meta make_manifest_metadata() const;

    /// Return 'true' if the spillover manifest can be added to
    /// the manifest without creating a gap
    bool safe_spillover_manifest(const segment_meta& meta);

    /// \brief Set start offset without removing any data from the
    /// manifest.
    ///
    /// Only allows start_offset to move forward
    /// and can only be placed on a segment boundary (should
    /// be equal to base_offset of one of the segments).
    /// Empty manfest has start_offset set to model::offset::min()
    /// \returns true if start offset was moved
    bool advance_start_offset(model::offset start_offset);

    /// \brief Set start kafka offset without removing any data from the
    /// manifest.
    ///
    /// Allows start_kafka_offset to move forward freely, without changing
    /// start_offset.
    ///
    /// \returns true if start_kafka_offset was moved
    bool advance_start_kafka_offset(kafka::offset new_start_offset);

    /// \brief Resets the state of the manifest to the default constructed
    /// state.
    ///
    /// Should only be used as a part of an escape hatch, not during the
    /// regular operation of a partition.
    ///
    /// There may not be anything necessarily unsafe about this, but marking
    /// "unsafe" to deter further authors from using with giving this a lot of
    /// thought.
    void unsafe_reset();

    /// Get segment if available or nullopt
    std::optional<segment_meta> get(const key& key) const;
    std::optional<segment_meta> get(const segment_name& name) const;

    /// Find element of the manifest by offset
    const_iterator find(model::offset o) const;

    /// Update manifest file from iobuf
    void update_with_json(iobuf buf);

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(
      manifest_format serialization_format, ss::input_stream<char> is) override;

    ss::future<> update(ss::input_stream<char> is) override {
        return update(manifest_format::serde, std::move(is));
    }

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    ss::future<serialized_data_stream> serialize() const override;

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    void serialize_json(std::ostream& out) const;

    // Serialize the manifest to an ss::output_stream in JSON format
    /// \param out output stream to serialize into; must be kept alive
    /// by the caller until the returned future completes.
    ///
    /// \return a future that completes after serialization is done
    ss::future<> serialize_json(ss::output_stream<char>& out) const;

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

    /// Transition manifest into such state which makes any uploads or reuploads
    /// impossible.
    void disable_permanently();

    model::offset get_archive_start_offset() const;
    model::offset_delta get_archive_start_offset_delta() const;
    kafka::offset get_archive_start_kafka_offset() const;
    model::offset get_archive_clean_offset() const;

    /// Advance archive_start_offset
    ///
    /// \param start_rp_offset is a new start offset in the archive
    /// \param start_delta number of configuration batches prior to
    ///                    start_rp_offset
    void set_archive_start_offset(
      model::offset start_rp_offset, model::offset_delta start_delta);

    /// Advance start_archive_clean_offset and adjuct the
    /// archive_size_bytes accordingly
    ///
    /// \param start_rp_offset is a new clean offset in the archive
    /// \param size_bytes number of bytes removed from the archive
    void set_archive_clean_offset(
      model::offset start_rp_offset, uint64_t size_bytes);

    kafka::offset get_start_kafka_offset_override() const;

    auto serde_fields() {
        // this list excludes _mem_tracker, which is not serialized
        return std::tie(
          _ntp,
          _rev,
          _segments,
          _replaced,
          _last_offset,
          _start_offset,
          _last_uploaded_compacted_offset,
          _insync_offset,
          _cloud_log_size_bytes,
          _archive_start_offset,
          _archive_start_offset_delta,
          _archive_clean_offset,
          _start_kafka_offset_override,
          _archive_size_bytes,
          _spillover_manifests);
    }
    auto serde_fields() const {
        // this list excludes _mem_tracker, which is not serialized
        return std::tie(
          _ntp,
          _rev,
          _segments,
          _replaced,
          _last_offset,
          _start_offset,
          _last_uploaded_compacted_offset,
          _insync_offset,
          _cloud_log_size_bytes,
          _archive_start_offset,
          _archive_start_offset_delta,
          _archive_clean_offset,
          _start_kafka_offset_override,
          _archive_size_bytes,
          _spillover_manifests);
    }

    /// Compare two manifests for equality. Don't compare the mem_tracker.
    bool operator==(const partition_manifest& other) const {
        return serde_fields() == other.serde_fields();
    }

    void from_iobuf(iobuf in);

    iobuf to_iobuf() const;

private:
    std::optional<kafka::offset> compute_start_kafka_offset_local() const;

    void set_start_offset(model::offset start_offset);

    void subtract_from_cloud_log_size(size_t to_subtract);

    // Computes the size in bytes of all segments available to clients
    // (i.e. all segments after and including the segment that starts at
    // the current _start_offset).
    uint64_t compute_cloud_log_size() const;

    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void do_update(partition_manifest_handler&& handler);

    /// Copy segments from _segments to _replaced
    /// Returns the total size in bytes of the replaced segments, or nullopt if
    /// the manifest contains already a segment that has the same remote path as
    /// replacing segment.
    std::optional<size_t>
    move_aligned_offset_range(const segment_meta& replacing_segment);

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
    /// Write next chunk of body
    void serialize_spillover(serialization_cursor_ptr cursor) const;
    /// Write epilogue
    void serialize_end(serialization_cursor_ptr cursor) const;
    /// Serialize normal manifest entry
    void serialize_segment_meta(
      const segment_meta& meta, serialization_cursor_ptr cursor) const;
    void serialize_spillover_manifest_meta(
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
    // Note: always set this member with `set_start_offset` as it has cached
    // values associated with it that need to be invalidated.
    model::offset _start_offset;
    model::offset _last_uploaded_compacted_offset;
    model::offset _insync_offset;
    // Size of the segments within the STM region
    size_t _cloud_log_size_bytes{0};
    // First accessible offset of the 'archive' region. Default value means
    // that there is no archive.
    model::offset _archive_start_offset;
    model::offset_delta _archive_start_offset_delta;
    // First offset of the 'archive'. The data between 'clean' and 'archive'
    // could be removed by the housekeeping. The invariant is that 'clean' is
    // less or equal to 'start'.
    model::offset _archive_clean_offset;
    // Start kafka offset set by the DeleteRecords request
    kafka::offset _start_kafka_offset_override;
    // Size of the segments within the archive region (i.e. excluding this
    // manifest)
    uint64_t _archive_size_bytes{0};
    /// Map of spillover manifests that were uploaded to S3
    spillover_manifest_map _spillover_manifests;

    // The starting offset for a Kafka batch in the segment that corresponds
    // with `_start_offset`. This value is computed from
    // `compute_start_kafka_offset_local` and is not in the serialized manifest.
    mutable std::optional<kafka::offset> _cached_start_kafka_offset_local;
};

} // namespace cloud_storage
