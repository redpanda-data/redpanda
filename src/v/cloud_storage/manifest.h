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

#include "bytes/iobuf.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "json/json.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "seastarx.h"
#include "tristate.h"

#include <seastar/util/bool_class.hh>

#include <rapidjson/fwd.h>

#include <compare>
#include <iterator>
#include <map>

namespace cloud_storage {

/// Information contained inside the partition manifest path
struct manifest_path_components {
    std::filesystem::path _origin;
    model::ns _ns;
    model::topic _topic;
    model::partition_id _part;
    model::revision_id _rev;
};

/// Information contained inside the segment path
///
/// The struct can contain information obtained from the full
/// S3 segment path. In this case it will have all fields properly
/// set (_origin, _ns, _topic, _part, _rev). It can also be created using
/// segment name only. In this case the _is_full field will be set
/// to false and some fields wouldn't be set (_origin, _ns, _topic, _part,
/// _rev).
struct segment_path_components : manifest_path_components {
    bool _is_full;
    cloud_storage::segment_name _name;
    model::offset _base_offset;
    model::term_id _term;
};

std::ostream& operator<<(std::ostream& s, const manifest_path_components& c);

std::ostream& operator<<(std::ostream& s, const segment_path_components& c);

/// Parse partition manifest path and return components
std::optional<manifest_path_components>
get_manifest_path_components(const std::filesystem::path& path);

/// Parse segment path and return components
std::optional<segment_path_components>
get_segment_path_components(const std::filesystem::path& path);

/// Parse base offset from the segment path or segment name
std::optional<model::offset> get_base_offset(const std::filesystem::path& path);

/// Parse segment file name
/// \return offset, term id, and success flag
std::tuple<model::offset, model::term_id, bool>
parse_segment_name(const std::filesystem::path& path);

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
class manifest final : public base_manifest {
public:
    struct segment_meta {
        bool is_compacted;
        size_t size_bytes;
        model::offset base_offset;
        model::offset committed_offset;
        model::timestamp base_timestamp;
        model::timestamp max_timestamp;
        model::offset delta_offset;

        auto operator<=>(const segment_meta&) const = default;
    };

    using key = std::variant<segment_name, remote_segment_path>;
    using value = segment_meta;
    using segment_map = std::map<key, value>;
    using const_iterator = segment_map::const_iterator;
    using const_reverse_iterator = segment_map::const_reverse_iterator;

    /// Create empty manifest that supposed to be updated later
    manifest();

    /// Create manifest for specific ntp
    explicit manifest(model::ntp ntp, model::revision_id rev);

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const override;

    /// Segment file name in S3
    remote_segment_path get_remote_segment_path(const segment_name& name) const;
    remote_segment_path get_remote_segment_path(const key& name) const;

    /// Get NTP
    const model::ntp& get_ntp() const;

    // Get last offset
    const model::offset get_last_offset() const;

    /// Get revision
    model::revision_id get_revision_id() const;

    /// Return iterator to the begining(end) of the segments list
    const_iterator begin() const;
    const_iterator end() const;
    const_reverse_iterator rbegin() const;
    const_reverse_iterator rend() const;
    size_t size() const;

    /// Check if the manifest contains particular path
    ///
    /// The manifest may contain two types of keys
    /// 1. Segment names like `193984-4-v1.log`
    /// 2. Full segment paths like
    /// `f28dac93/kafka/mytopic/12_32/193984-4-v1.log`
    /// This overloads handles the second case.
    bool contains(const key& path) const;

    /// Add new segment to the manifest
    bool add(const segment_name& key, const segment_meta& meta);

    /// Add new segment to the manifest
    bool add(const remote_segment_path& key, const segment_meta& meta);

    /// Get segment if available or nullopt
    ///
    /// The manifest may contain two types of keys
    /// 1. Segment names like `193984-4-v1.log`
    /// 2. Full segment paths like
    /// `f28dac93/kafka/mytopic/12_32/193984-4-v1.log`
    const segment_meta* get(const key& path) const;

    /// Get insert iterator for segments set
    std::insert_iterator<segment_map> get_insert_iterator();

    /// Return new manifest that contains only those segments that present
    /// in local manifest and not found in 'remote_set'.
    ///
    /// \param remote_set the manifest to compare to
    /// \return manifest with segments that doesn't present in 'remote_set'
    manifest difference(const manifest& remote_set) const;

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
    bool operator==(const manifest& other) const = default;

    /// Remove segment record from manifest
    ///
    /// \param name is a segment name
    /// \return true on success, false on failure (no such segment)
    bool delete_permanently(const segment_name& name);

    manifest_type get_manifest_type() const override {
        return manifest_type::partition;
    };

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void update(const rapidjson::Document& m);

    model::ntp _ntp;
    model::revision_id _rev;
    segment_map _segments;
    model::offset _last_offset;
};

class topic_manifest final : public base_manifest {
public:
    /// Create manifest for specific ntp
    explicit topic_manifest(
      const cluster::topic_configuration& cfg, model::revision_id rev);

    /// Create empty manifest that supposed to be updated later
    topic_manifest();

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(ss::input_stream<char> is) override;

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    serialized_json_stream serialize() const override;

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const override;

    static remote_manifest_path
    get_topic_manifest_path(model::ns ns, model::topic topic);

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    void serialize(std::ostream& out) const;

    /// Return all possible manifest locations
    std::vector<remote_manifest_path> get_partition_manifests() const;

    manifest_type get_manifest_type() const override {
        return manifest_type::partition;
    };

    model::revision_id get_revision() const noexcept { return _rev; }

    /// Change topic-manifest revision
    void set_revision(model::revision_id id) noexcept { _rev = id; }

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void update(const rapidjson::Document& m);

    std::optional<cluster::topic_configuration> _topic_config;
    model::revision_id _rev;
};

} // namespace cloud_storage
