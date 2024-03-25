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

#include "base/seastarx.h"
#include "cloud_storage_clients/configuration.h"
#include "config/configuration.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "utils/named_type.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/node_hash_set.h>

#include <chrono>
#include <filesystem>

namespace cloud_storage {

using remote_metrics_disabled
  = ss::bool_class<struct remote_metrics_disabled_tag>;

/// Segment file name without working directory,
/// expected format: <base-offset>-<term-id>-<revision>.log
using segment_name = named_type<ss::sstring, struct archival_segment_name_t>;
using remote_manifest_path
  = named_type<std::filesystem::path, struct archival_remote_manifest_path_t>;
/// Local segment path, expected format:
/// <work-dir>/<ns>/<topic>/<part-id>_<rev>/<base-offset>-<term-id>-<revision>.log
using local_segment_path
  = named_type<std::filesystem::path, struct archival_local_segment_path_t>;
/// Number of simultaneous connections to S3
using connection_limit = named_type<size_t, struct archival_connection_limit_t>;

using segment_reader_units
  = named_type<ssx::semaphore_units, struct segment_reader_units_type>;
using segment_units
  = named_type<ssx::semaphore_units, struct segment_units_type>;

/// Version of the segment name format
enum class segment_name_format : int16_t {
    // Original metadata format, segment name has simple format (same as on
    // disk).
    v1 = 1,
    // Extended format, segment name is different from on-disk representation,
    // the committed offset and size are added to the name.
    v2 = 2,
    // Extra field which is used to track size of the tx-manifest is added.
    v3 = 3
};

std::ostream& operator<<(std::ostream& o, const segment_name_format& r);

enum class [[nodiscard]] download_result : int32_t {
    success,
    notfound,
    timedout,
    failed,
};

enum class [[nodiscard]] upload_result : int32_t {
    success,
    timedout,
    failed,
    cancelled,
};

enum class manifest_version : int32_t {
    v1 = 1,
    v2 = 2,
    v3 = 3, // v23.3.x
    v4 = 4, // add applied_offset field
};

enum class tx_range_manifest_version : int32_t {
    v1 = 1,
    current_version = v1,
    compat_version = v1,
};

static constexpr int32_t topic_manifest_version = 1;

std::ostream& operator<<(std::ostream& o, const download_result& r);

std::ostream& operator<<(std::ostream& o, const upload_result& r);

struct configuration {
    /// Client configuration
    cloud_storage_clients::client_configuration client_config;
    /// Number of simultaneous client uploads
    connection_limit connection_limit;
    /// Disable metrics in the remote
    remote_metrics_disabled metrics_disabled;
    /// The S3 bucket or ABS container to use
    cloud_storage_clients::bucket_name bucket_name;

    model::cloud_credentials_source cloud_credentials_source;

    friend std::ostream& operator<<(std::ostream& o, const configuration& cfg);

    static ss::future<configuration> get_config();
    static ss::future<configuration> get_s3_config();
    static ss::future<configuration> get_abs_config();
    static const config::property<std::optional<ss::sstring>>&
    get_bucket_config();
};

struct offset_range {
    kafka::offset begin;
    kafka::offset end;
    model::offset begin_rp;
    model::offset end_rp;
};

/// Topic configuration substitute for the manifest
struct manifest_topic_configuration {
    model::topic_namespace tp_ns;
    int32_t partition_count;
    int32_t replication_factor;
    struct topic_properties {
        std::optional<model::compression> compression;
        std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
        std::optional<model::compaction_strategy> compaction_strategy;
        std::optional<model::timestamp_type> timestamp_type;
        std::optional<size_t> segment_size;
        tristate<size_t> retention_bytes{std::nullopt};
        tristate<std::chrono::milliseconds> retention_duration{std::nullopt};
        std::optional<model::vcluster_id> virtual_cluster_id;
        bool operator==(const topic_properties& other) const = default;
    };
    topic_properties properties;
    bool operator==(const manifest_topic_configuration& other) const = default;
};

struct segment_meta {
    using value_t = segment_meta;
    static constexpr serde::version_t redpanda_serde_version = 3;
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
    /// Offset translation delta just past the end of this segment. This is
    /// useful in determining the next Kafka offset, e.g. when reporting the
    /// high watermark.
    model::offset_delta delta_offset_end;
    /// Segment name format specifier
    segment_name_format sname_format{segment_name_format::v1};
    /// Size of the metadata (optional)
    uint64_t metadata_size_hint{0};

    kafka::offset base_kafka_offset() const {
        // Manifests created with the old version of redpanda won't have the
        // delta_offset field. In this case the value will be initialized to
        // model::offset::min(). In this case offset translation couldn't be
        // performed.
        auto delta = delta_offset == model::offset_delta::min()
                       ? model::offset_delta(0)
                       : delta_offset;
        return base_offset - delta;
    }

    // NOTE: in older versions of Redpanda, delta_offset_end may have been
    // under-reported by 1 if the last segment contained no data batches,
    // meaning next_kafka_offset could over-report the next offset by 1.
    kafka::offset next_kafka_offset() const {
        // Manifests created with the old version of redpanda won't have the
        // delta_offset_end field. In this case offset translation couldn't be
        // performed.
        auto delta = delta_offset_end == model::offset_delta::min()
                       ? model::offset_delta(0)
                       : delta_offset_end;
        // NOTE: delta_offset_end is a misnomer and actually refers to the
        // offset delta of the next offset after this segment.
        return model::next_offset(committed_offset) - delta;
    }

    kafka::offset last_kafka_offset() const {
        return next_kafka_offset() - kafka::offset(1);
    }

    auto operator<=>(const segment_meta&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const segment_meta& meta) {
        return H::combine(
          std::move(h),
          meta.base_offset(),
          meta.committed_offset(),
          meta.delta_offset(),
          meta.delta_offset_end());
    }
};
std::ostream& operator<<(std::ostream& o, const segment_meta& r);

enum class error_outcome {
    // Represent general failure that can't be handled and doesn't fit into
    // any particular category (like download failure)
    failure,
    scan_bucket_error,
    manifest_download_error,
    manifest_upload_error,
    manifest_not_found,
    segment_download_error,
    segment_upload_error,
    segment_not_found,
    // Represent transient error that can be retried. This is the only error
    // outcome that shouldn't be bubbled up to the client.
    repeat,
    timed_out,
    out_of_range,
    shutting_down
};

struct error_outcome_category final : public std::error_category {
    const char* name() const noexcept final {
        return "cloud_storage::error_outcome";
    }

    std::string message(int c) const final {
        switch (static_cast<error_outcome>(c)) {
        case error_outcome::failure:
            return "cloud storage failure";
        case error_outcome::scan_bucket_error:
            return "cloud storage scan bucket error";
        case error_outcome::manifest_download_error:
            return "cloud storage manifest download error";
        case error_outcome::manifest_upload_error:
            return "cloud storage manifest upload error";
        case error_outcome::manifest_not_found:
            return "cloud storage manifest not found";
        case error_outcome::segment_download_error:
            return "cloud storage segment download error";
        case error_outcome::segment_upload_error:
            return "cloud storage segment upload error";
        case error_outcome::segment_not_found:
            return "cloud storage segment not found";
        case error_outcome::repeat:
            return "cloud storage repeat operation";
        case error_outcome::timed_out:
            return "cloud storage operation timed out";
        case error_outcome::out_of_range:
            return "cloud storage out of range";
        case error_outcome::shutting_down:
            return "shutting down";
        default:
            return "unknown";
        }
    }
};

inline const std::error_category& error_category() noexcept {
    static error_outcome_category e;
    return e;
}

inline std::error_code make_error_code(error_outcome e) noexcept {
    return {static_cast<int>(e), error_category()};
}

// target_min_bytes: the minimum number of bytes that the cache should
// reserve in order to maintain proper functionality.
//
// target_bytes: the minimum number of bytes that the cache should reserve
// in order to maining "good" functionality, for some definition of good.
// this may include heuristics to avoid too much thrashing, as well as
// allowing read-ahead optimizations to be effective.
struct cache_usage_target {
    size_t target_min_bytes{0};
    size_t target_bytes{0};
    bool chunked{false};

    friend cache_usage_target
    operator+(cache_usage_target lhs, const cache_usage_target& rhs) {
        lhs.target_min_bytes += rhs.target_min_bytes;
        lhs.target_bytes += rhs.target_bytes;
        // if we have one case of chunked storage, then continue to estimate
        // as if chunked storage is used.
        lhs.chunked = lhs.chunked || rhs.chunked;
        return lhs;
    }
};

struct spillover_manifest_path_components
  : serde::envelope<
      spillover_manifest_path_components,
      serde::version<0>,
      serde::compat_version<0>> {
    model::offset base;
    model::offset last;
    kafka::offset base_kafka;
    kafka::offset next_kafka;
    model::timestamp base_ts;
    model::timestamp last_ts;

    auto serde_fields() {
        return std::tie(base, last, base_kafka, next_kafka, base_ts, last_ts);
    }

    bool operator==(spillover_manifest_path_components const&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const spillover_manifest_path_components& c) {
        return H::combine(
          std::move(h),
          c.base(),
          c.last(),
          c.base_kafka(),
          c.next_kafka(),
          c.base_ts(),
          c.last_ts());
    }
};

std::ostream&
operator<<(std::ostream& o, const spillover_manifest_path_components& c);

enum class scrub_status : uint8_t { full, partial, failed };

std::ostream& operator<<(std::ostream& o, const scrub_status&);

enum class anomaly_type : int8_t {
    missing_delta,
    non_monotonical_delta,
    end_delta_smaller,
    committed_smaller,
    offset_gap,
    offset_overlap
};

std::ostream& operator<<(std::ostream& o, const anomaly_type&);

struct anomaly_meta
  : serde::envelope<anomaly_meta, serde::version<0>, serde::compat_version<0>> {
    anomaly_type type;
    segment_meta at;
    std::optional<segment_meta> previous;

    bool operator==(anomaly_meta const&) const = default;

    auto serde_fields() { return std::tie(type, at, previous); }

    template<typename H>
    friend H AbslHashValue(H h, const anomaly_meta& am) {
        return H::combine(std::move(h), am.type, am.at);
    }
};

std::ostream& operator<<(std::ostream& o, const anomaly_meta&);

using segment_meta_anomalies = absl::node_hash_set<anomaly_meta>;

void scrub_segment_meta(
  const segment_meta& current,
  const std::optional<segment_meta>& previous,
  segment_meta_anomalies& detected);

struct anomalies
  : serde::envelope<anomalies, serde::version<1>, serde::compat_version<0>> {
    // Missing partition manifests
    bool missing_partition_manifest{false};
    // Spillover manifests referenced by the manifest which were not
    // found
    absl::node_hash_set<spillover_manifest_path_components>
      missing_spillover_manifests;
    // Segments referenced by the manifests which were not found
    absl::node_hash_set<segment_meta> missing_segments;
    // Segments that have metadata anomalies (e.g. gaps or overlaps)
    segment_meta_anomalies segment_metadata_anomalies;
    // Optional timestamp indicating the last time point at which
    // the scrub of the full log completed.
    std::optional<model::timestamp> last_complete_scrub;

    // Number of discarded anomalies
    uint32_t num_discarded_missing_spillover_manifests{0};
    uint32_t num_discarded_missing_segments{0};
    uint32_t num_discarded_metadata_anomalies{0};

    auto serde_fields() {
        return std::tie(
          missing_partition_manifest,
          missing_spillover_manifests,
          missing_segments,
          segment_metadata_anomalies,
          last_complete_scrub,
          num_discarded_missing_spillover_manifests,
          num_discarded_missing_segments,
          num_discarded_metadata_anomalies);
    }

    bool has_value() const;

    size_t count_segment_meta_anomaly_type(anomaly_type type) const;

    anomalies& operator+=(anomalies&&);

    friend bool operator==(const anomalies& lhs, const anomalies& rhs);
};

std::ostream& operator<<(std::ostream& o, const anomalies& a);

enum class upload_type {
    object,
    segment_index,
    manifest,
    group_offsets_snapshot,
    download_result_file,
    remote_lifecycle_marker,
    inventory_configuration,
};

std::ostream& operator<<(std::ostream&, upload_type);

enum class download_type { object, segment_index, inventory_report_manifest };

std::ostream& operator<<(std::ostream&, download_type);

enum class existence_check_type { object, segment };

std::ostream& operator<<(std::ostream&, existence_check_type);

class remote_probe;
using probe_callback_t = ss::noncopyable_function<void(remote_probe&)>;

struct transfer_details {
    cloud_storage_clients::bucket_name bucket;
    cloud_storage_clients::object_key key;

    retry_chain_node& parent_rtc;

    std::optional<probe_callback_t> success_cb{std::nullopt};
    std::optional<probe_callback_t> failure_cb{std::nullopt};
    std::optional<probe_callback_t> backoff_cb{std::nullopt};

    void on_success(remote_probe& probe);
    void on_failure(remote_probe& probe);
    void on_backoff(remote_probe& probe);
};

struct upload_request {
    transfer_details transfer_details;
    upload_type type;
    iobuf payload;
};

struct download_request {
    transfer_details transfer_details;
    download_type type;
    iobuf& payload;
};

} // namespace cloud_storage

namespace std {
template<>
struct is_error_code_enum<cloud_storage::error_outcome> : true_type {};
} // namespace std
