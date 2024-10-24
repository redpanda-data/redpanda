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

#include "base/outcome.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/types.h"
#include "cluster/archival/async_data_uploader.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

namespace ss = seastar;

namespace archival {

struct manifest_upload_result {
    model::ntp ntp;
    size_t num_put_requests{0};
    size_t size_bytes{0};

    bool operator==(const manifest_upload_result& other) const = default;

    friend std::ostream&
    operator<<(std::ostream& o, const manifest_upload_result& arg);
};

struct reconciled_upload_candidate {
    /// Partition
    model::ntp ntp;
    /// Stream of log data in cloud-storage format
    ss::input_stream<char> payload;
    /// Size of the data stream
    size_t size_bytes;
    /// Segment metadata
    cloud_storage::segment_meta metadata;
    // Transactional metadata
    fragmented_vector<model::tx_range> tx;

    reconciled_upload_candidate() = default;

    reconciled_upload_candidate(
      model::ntp ntp,
      ss::input_stream<char> payload,
      size_t size_bytes,
      cloud_storage::segment_meta metadata,
      fragmented_vector<model::tx_range> tx)
      : ntp(std::move(ntp))
      , payload(std::move(payload))
      , size_bytes(size_bytes)
      , metadata(metadata)
      , tx(std::move(tx)) {}

    reconciled_upload_candidate(const reconciled_upload_candidate&) = delete;
    reconciled_upload_candidate(reconciled_upload_candidate&&) noexcept
      = default;
    reconciled_upload_candidate& operator=(const reconciled_upload_candidate&)
      = delete;
    reconciled_upload_candidate&
    operator=(reconciled_upload_candidate&&) noexcept
      = default;

    // NOTE: the operator is needed for tests only, it doesn't check
    // equality of byte streams.
    bool operator==(const reconciled_upload_candidate& o) const noexcept;

    friend std::ostream&
    operator<<(std::ostream& o, const reconciled_upload_candidate& s);
};

struct upload_candidate_search_parameters {
    upload_candidate_search_parameters() = default;
    upload_candidate_search_parameters(
      model::ntp ntp,
      model::term_id archiver_term,
      size_t target_size,
      size_t min_size,
      std::optional<size_t> upload_size_quota,
      std::optional<size_t> upload_requests_quota,
      bool compacted_reupload,
      bool inline_manifest);

    upload_candidate_search_parameters(
      const upload_candidate_search_parameters&)
      = default;
    upload_candidate_search_parameters&
    operator=(const upload_candidate_search_parameters&)
      = default;
    upload_candidate_search_parameters(
      upload_candidate_search_parameters&&) noexcept
      = default;
    upload_candidate_search_parameters&
    operator=(upload_candidate_search_parameters&&) noexcept
      = default;

    bool operator==(const upload_candidate_search_parameters&) const = default;

    friend std::ostream&
    operator<<(std::ostream& o, const upload_candidate_search_parameters& s);
    model::ntp ntp;
    // Current term of the archiver
    model::term_id archiver_term;
    // Optimal segment upload size
    size_t target_size;
    // Smallest acceptable upload size (can be set to 0 by the scheduler if
    // upload interval expired)
    size_t min_size;

    // Upload size quota. The 'find_upload_candidates' method can find
    // multiple upload candidates but their total size shouldn't exceed the
    // quota. If set to 'nullopt' then there is no limit.
    std::optional<size_t> upload_size_quota;
    // PUT requests quota. Value 'nullopt' means no limit.
    std::optional<size_t> upload_requests_quota;

    // Set to true if compacted reupload is enabled in the config
    bool compacted_reupload{false};
    // Set to true if the manifest upload has to be performed in parallel
    // with segment upload
    bool inline_manifest{false};
};

using reconciled_upload_candidate_ptr
  = ss::lw_shared_ptr<reconciled_upload_candidate>;

struct reconciled_upload_candidates_list {
    model::ntp ntp;
    std::deque<reconciled_upload_candidate_ptr> results;
    model::offset read_write_fence;

    reconciled_upload_candidates_list(
      model::ntp ntp,
      std::deque<reconciled_upload_candidate_ptr> results,
      model::offset read_write_fence)
      : ntp(std::move(ntp))
      , results(std::move(results))
      , read_write_fence(read_write_fence) {}

    reconciled_upload_candidates_list(const reconciled_upload_candidates_list&)
      = delete;
    reconciled_upload_candidates_list(
      reconciled_upload_candidates_list&&) noexcept
      = default;
    reconciled_upload_candidates_list&
    operator=(const reconciled_upload_candidates_list&)
      = delete;
    reconciled_upload_candidates_list&
    operator=(reconciled_upload_candidates_list&&) noexcept
      = default;

    bool operator==(const reconciled_upload_candidates_list& rhs) const;

    friend std::ostream&
    operator<<(std::ostream& o, const reconciled_upload_candidates_list& r);
};

struct upload_results_list {
    model::ntp ntp;
    std::deque<std::optional<cloud_storage::segment_record_stats>> stats;
    std::deque<cloud_storage::upload_result> results;
    std::deque<cloud_storage::segment_meta> metadata;

    // Last uploaded offset of the partition for which we persisted metadata
    // (manifest) to the cloud storage
    model::offset manifest_clean_offset;
    // The field is used for concurrency control. This is the offset of the last
    // archival_metadata_stm command applied to the manifest.
    model::offset read_write_fence;

    size_t num_put_requests{0};
    size_t num_bytes_sent{0};

    upload_results_list() = default;

    upload_results_list(
      model::ntp ntp,
      std::deque<std::optional<cloud_storage::segment_record_stats>> stats,
      std::deque<cloud_storage::upload_result> results,
      std::deque<cloud_storage::segment_meta> metadata,
      model::offset manifest_clean_offset,
      model::offset read_write_fence,
      size_t num_put_requests,
      size_t num_bytes_sent)
      : ntp(std::move(ntp))
      , stats(std::move(stats))
      , results(std::move(results))
      , metadata(std::move(metadata))
      , manifest_clean_offset(manifest_clean_offset)
      , read_write_fence(read_write_fence)
      , num_put_requests(num_put_requests)
      , num_bytes_sent(num_bytes_sent) {}

    upload_results_list(const upload_results_list&) = delete;
    upload_results_list(upload_results_list&&) noexcept = default;
    upload_results_list& operator=(const upload_results_list&) = delete;
    upload_results_list& operator=(upload_results_list&&) noexcept = default;

    bool operator==(const upload_results_list& o) const noexcept;

    friend std::ostream&
    operator<<(std::ostream& o, const upload_results_list& s);
};

struct admit_uploads_result {
    model::ntp ntp;
    size_t num_succeeded{0};
    size_t num_failed{0};

    // The in-sync offset of the manifest after update
    model::offset manifest_dirty_offset;

    bool operator==(const admit_uploads_result&) const = default;

    friend std::ostream&
    operator<<(std::ostream& o, const admit_uploads_result& s);
};

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
    /// Return upload candidate(s) if data is available or nullopt
    /// if there is not enough data to start an upload.
    virtual ss::future<result<reconciled_upload_candidates_list>>
    find_upload_candidates(
      retry_chain_node&, upload_candidate_search_parameters) noexcept
      = 0;

    /// Upload data to S3 and return results
    ///
    /// The method uploads segments with their corresponding tx-manifests and
    /// indexes and also the manifest. The result contains the insync offset of
    /// the uploaded manifest. The state of the uploaded manifest doesn't
    /// include uploaded segments because they're not admitted yet.
    virtual ss::future<result<upload_results_list>> schedule_uploads(
      retry_chain_node&,
      reconciled_upload_candidates_list,
      bool embed_manifest) noexcept
      = 0;

    /// Add metadata to the manifest by replicating archival metadata
    /// configuration batch
    virtual ss::future<result<admit_uploads_result>>
    admit_uploads(retry_chain_node&, upload_results_list) noexcept = 0;

    // Metadata management

    /// Reupload manifest and replicate configuration batch
    virtual ss::future<result<manifest_upload_result>>
    upload_manifest(retry_chain_node&, model::ntp) noexcept = 0;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;
};

} // namespace archival
