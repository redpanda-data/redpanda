/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/async_data_uploader.h"
#include "base/outcome.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

namespace ss = seastar;

namespace archival {

/// The API provides operations that can be used by different
/// upload workflows. The object is not specific to any particular
/// NTP. Multiple workflows can use single API instance.
class archiver_operations_api {
public:
    archiver_operations_api() = default;
    archiver_operations_api(const archiver_operations_api&) = delete;
    archiver_operations_api(archiver_operations_api&&) noexcept = delete;
    archiver_operations_api& operator=(const archiver_operations_api&) = delete;
    archiver_operations_api& operator=(archiver_operations_api&&) noexcept
      = delete;
    virtual ~archiver_operations_api();

public:
    /// Reconciled upload candidate
    struct segment_upload_candidate_t {
        /// Partition
        model::ktp ntp;
        /// Stream of log data in cloud-storage format
        ss::input_stream<char> payload;
        /// Size of the data stream
        size_t size_bytes;
        /// Segment metadata
        cloud_storage::segment_meta metadata;
        // Transactional metadata
        fragmented_vector<model::tx_range> tx;

        // NOTE: the operator is needed for tests only, it doesn't check
        // equality of byte streams.
        bool operator==(const segment_upload_candidate_t& o) const noexcept;

        friend std::ostream&
        operator<<(std::ostream& o, const segment_upload_candidate_t& s);
    };
    using segment_upload_candidate_ptr
      = ss::lw_shared_ptr<segment_upload_candidate_t>;
    struct find_upload_candidates_arg {
        static constexpr auto default_initialized
          = std::numeric_limits<size_t>::max();
        model::ktp ntp;
        // Current term of the archiver
        model::term_id archiver_term;
        // Optimal segment upload size
        size_t target_size{default_initialized};
        // Smallest acceptable upload size (can be set to 0 by the scheduler if
        // upload interval expired)
        size_t min_size{default_initialized};

        // Upload size quota. The 'find_upload_candidates' method can find
        // multiple upload candidates but their total size shouldn't exceed the
        // quota. If set to -1 then there is no limit.
        ssize_t upload_size_quota{-1};
        // PUT requests quota. Value -1 means no limit.
        ssize_t upload_requests_quota{-1};

        // Set to true if compacted reupload is enabled in the config
        bool compacted_reupload{false};
        // Set to true if the manifest upload has to be performed in parallel
        // with segment upload
        bool inline_manifest{false};

        bool operator==(const find_upload_candidates_arg&) const = default;

        friend std::ostream&
        operator<<(std::ostream& o, const find_upload_candidates_arg& s);
    };

    struct find_upload_candidates_result {
        model::ktp ntp;
        std::deque<segment_upload_candidate_ptr> results;
        model::offset read_write_fence;

        bool operator==(const find_upload_candidates_result& rhs) const;

        friend std::ostream&
        operator<<(std::ostream& o, const find_upload_candidates_result& r);
    };

    /// Return upload candidate(s) if data is available or an error
    /// if there is not enough data to start an upload.
    virtual ss::future<result<find_upload_candidates_result>>
    find_upload_candidates(
      retry_chain_node&, find_upload_candidates_arg) noexcept
      = 0;

    struct schedule_upload_results {
        model::ktp ntp;
        std::deque<std::optional<cloud_storage::segment_record_stats>> stats;
        std::deque<cloud_storage::upload_result> results;

        // Insync offset of the uploaded manifest
        model::offset manifest_clean_offset;
        model::offset read_write_fence;

        size_t num_put_requests{0};
        size_t num_bytes_sent{0};

        bool operator==(const schedule_upload_results& o) const noexcept;

        friend std::ostream&
        operator<<(std::ostream& o, const schedule_upload_results& s);
    };

    /// Upload data to S3 and return results
    ///
    /// The method uploads segments with their corresponding tx-manifests and
    /// indexes and also the manifest. The result contains the insync offset of
    /// the uploaded manifest. The state of the uploaded manifest doesn't
    /// include uploaded segments because they're not admitted yet.
    virtual ss::future<result<schedule_upload_results>> schedule_uploads(
      retry_chain_node&,
      find_upload_candidates_result,
      bool embed_manifest) noexcept
      = 0;

    struct admit_uploads_result {
        model::ktp ntp;
        size_t num_succeeded{0};
        size_t num_failed{0};

        // The in-sync offset of the manifest after update
        model::offset manifest_dirty_offset;

        auto operator<=>(const admit_uploads_result&) const = default;

        friend std::ostream&
        operator<<(std::ostream& o, const admit_uploads_result& s);
    };
    /// Add metadata to the manifest by replicating archival metadata
    /// configuration batch
    virtual ss::future<result<admit_uploads_result>>
    admit_uploads(retry_chain_node&, schedule_upload_results) noexcept = 0;

    struct manifest_upload_arg {
        model::ktp ntp;

        bool operator==(const manifest_upload_arg& other) const = default;

        friend std::ostream&
        operator<<(std::ostream& o, const manifest_upload_arg& arg);
    };

    struct manifest_upload_result {
        model::ktp ntp;
        size_t num_put_requests{0};
        size_t size_bytes{0};

        bool operator==(const manifest_upload_result& other) const = default;

        friend std::ostream&
        operator<<(std::ostream& o, const manifest_upload_result& arg);
    };

    /// Reupload manifest and replicate configuration batch
    virtual ss::future<result<manifest_upload_result>>
    upload_manifest(retry_chain_node&, manifest_upload_arg) noexcept = 0;
};

} // namespace archival
