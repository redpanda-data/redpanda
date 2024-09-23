/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/fwd.h"
#include "cloud_storage/inventory/ntp_hashes.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/archival/run_quota.h"
#include "model/fundamental.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/sstring.hh>

namespace cloud_storage {

struct existence_query_context {
    bool is_inv_scrub_enabled{false};
    bool is_inv_data_available{false};
    bool force_segment_existence_check{false};
    std::optional<inventory::ntp_path_hashes> hashes;

    existence_query_context(bool always_check_for_segments, model::ntp ntp);

    ss::future<> load_from_disk();

    bool should_lookup_in_cloud_storage(const remote_segment_path& p) const;

    static ss::future<existence_query_context>
    load(bool always_check_for_segments, model::ntp ntp);
};

/*
 * Utility class that detects anomalies in the data and metadata uploaded
 * by a partition to cloud storage.
 *
 * It performs the following steps:
 * 1. Download partition manifest
 * 2. Check for existence of spillover manifests
 * 3. Check for existence of segments referenced by partition manifest
 * 4. For each spillover manifest, check for existence of the referenced
 * segments
 */
class anomalies_detector {
public:
    anomalies_detector(
      cloud_storage_clients::bucket_name bucket,
      model::ntp ntp,
      model::initial_revision_id initial_rev,
      const remote_path_provider&,
      remote& remote,
      retry_chain_logger& logger,
      ss::abort_source& as);

    struct result {
        scrub_status status{scrub_status::full};
        std::optional<model::offset> last_scrubbed_offset;
        anomalies detected;
        int32_t ops{0};
        int32_t segments_visited{0};

        result& operator+=(result&&);
    };

    using segment_depth_t = named_type<int32_t, struct segment_depth_tag>;
    struct quota_limit {
        // max num of GET/HEAD request allowed in a run
        archival::run_quota_t max_num_operations = archival::run_quota_t::max();
        // max num of segment_meta to check in a run
        segment_depth_t max_num_segments = segment_depth_t::max();

        constexpr quota_limit() noexcept = default;

        constexpr quota_limit(archival::run_quota_t q) noexcept
          : max_num_operations(q) {}
        constexpr quota_limit(
          archival::run_quota_t q, segment_depth_t a) noexcept
          : max_num_operations{q}
          , max_num_segments{a} {}
        constexpr quota_limit(segment_depth_t a) noexcept
          : max_num_segments{a} {}
    };

    /// \brief run validation up to quota_limit then return
    /// \param quota_total reprensent the total number of GET request to perform
    /// (0 will still download some objects, to ensure forward progress)
    /// \param scrub_from it's the starting offset for the scan
    /// \param force_segment_api_checks always use HTTP head request for
    /// every segment to be checked, even if inventory data set is not
    /// available. This flag is useful during a topic recovery scenario where
    /// inventory data set may be missing.
    ss::future<result> run(
      retry_chain_node&,
      quota_limit quota_total,
      std::optional<model::offset> scrub_from = std::nullopt,
      bool force_segment_api_checks = false);

private:
    ss::future<std::optional<spillover_manifest>> download_spill_manifest(
      const ss::sstring& path, retry_chain_node& rtc_node);

    using stop_detector = ss::bool_class<struct stop_detector_tag>;

    ss::future<stop_detector> check_manifest(
      const partition_manifest& manifest,
      std::optional<model::offset>,
      retry_chain_node& rtc_node,
      const existence_query_context& query_ctx);

    bool should_stop() const;

    /// compute how many segments can be visited, based on _received_quota and
    /// _result
    size_t get_visitable_segments() const;

    ss::future<> do_lookup_segment(
      remote_segment_path path, segment_meta meta, retry_chain_node& rtc_node);

    cloud_storage_clients::bucket_name _bucket;
    model::ntp _ntp;
    model::initial_revision_id _initial_rev;
    const remote_path_provider& _remote_path_provider;

    remote& _remote;
    retry_chain_logger& _logger;
    ss::abort_source& _as;

    result _result;
    quota_limit _received_quota;
};

} // namespace cloud_storage
