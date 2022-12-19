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

#include "cloud_storage_clients/configuration.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <chrono>
#include <filesystem>

namespace cloud_storage {

using remote_metrics_disabled
  = ss::bool_class<struct remote_metrics_disabled_tag>;

/// Segment file name without working directory,
/// expected format: <base-offset>-<term-id>-<revision>.log
using segment_name = named_type<ss::sstring, struct archival_segment_name_t>;
/// Segment path in S3, expected format:
/// <prefix>/<ns>/<topic>/<part-id>_<rev>/<base-offset>-<term-id>-<revision>.log.<archiver-term>
using remote_segment_path
  = named_type<std::filesystem::path, struct archival_remote_segment_path_t>;
using remote_manifest_path
  = named_type<std::filesystem::path, struct archival_remote_manifest_path_t>;
/// Local segment path, expected format:
/// <work-dir>/<ns>/<topic>/<part-id>_<rev>/<base-offset>-<term-id>-<revision>.log
using local_segment_path
  = named_type<std::filesystem::path, struct archival_local_segment_path_t>;
/// Number of simultaneous connections to S3
using s3_connection_limit
  = named_type<size_t, struct archival_s3_connection_limit_t>;

/// Version of the segment name format
enum class segment_name_format : int16_t { v1 = 1, v2 = 2 };

std::ostream& operator<<(std::ostream& o, const segment_name_format& r);

enum class download_result : int32_t {
    success,
    notfound,
    timedout,
    failed,
};

enum class upload_result : int32_t {
    success,
    timedout,
    failed,
    cancelled,
};

enum class manifest_version : int32_t {
    v1 = 1,
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
    /// S3 configuration
    cloud_storage_clients::client_configuration client_config;
    /// Number of simultaneous S3 uploads
    s3_connection_limit connection_limit;
    /// Disable metrics in the remote
    remote_metrics_disabled metrics_disabled;
    /// The bucket to use
    cloud_storage_clients::bucket_name bucket_name;

    model::cloud_credentials_source cloud_credentials_source;

    friend std::ostream& operator<<(std::ostream& o, const configuration& cfg);

    static ss::future<configuration> get_config();
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
    };
    topic_properties properties;
};

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

    kafka::offset committed_kafka_offset() const {
        // Manifests created with the old version of redpanda won't have the
        // delta_offset_end field. In this case offset translation couldn't be
        // performed.
        auto delta = delta_offset_end == model::offset_delta::min()
                       ? model::offset_delta(0)
                       : delta_offset_end;
        return committed_offset - delta;
    }

    auto operator<=>(const segment_meta&) const = default;
};

} // namespace cloud_storage
