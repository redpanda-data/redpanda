/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "archival/archiver_operations_impl.h"

#include "archival/archiver_operations_api.h"
#include "archival/async_data_uploader.h"
#include "archival/types.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "errc.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "storage/batch_consumer_utils.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>

#include <exception>

namespace archival {
namespace detail {

class archiver_operations_impl : public archiver_operations_api {
public:
    /// C-tor
    archiver_operations_impl(
      ss::shared_ptr<cloud_storage_remote_api> api,
      ss::shared_ptr<cluster_partition_manager_api> pm,
      ss::shared_ptr<segment_upload_builder_api> upl_builder,
      ss::scheduling_group sg,
      cloud_storage_clients::bucket_name bucket)
      : _rtc(_as)
      , _rtclog(archival_log, _rtc)
      , _api(std::move(api))
      , _upl_builder(std::move(upl_builder))
      , _pm(std::move(pm))
      , _read_buffer_size(
          config::shard_local_cfg().storage_read_buffer_size.bind())
      , _readahead(
          config::shard_local_cfg().storage_read_readahead_count.bind())
      , _bucket(std::move(bucket))
      , _sg(sg) {}

    /// Return upload candidate(s) if data is available or not_enough_data error
    /// if there is not enough data to start an upload.
    ss::future<result<reconciled_upload_candidates_list>>
    find_upload_candidates(
      retry_chain_node& workflow_rtc,
      upload_candidate_search_parameters arg) noexcept override {
        co_return error_outcome::unexpected_failure;
    }

    /// Upload data to S3 and return results
    ///
    /// The method uploads segments with their corresponding tx-manifests and
    /// indexes and also the manifest. The result contains the insync offset of
    /// the uploaded manifest. The state of the uploaded manifest doesn't
    /// include uploaded segments because they're not admitted yet.
    ss::future<result<upload_results_list>> schedule_uploads(
      retry_chain_node& workflow_rtc,
      reconciled_upload_candidates_list bundle,
      bool inline_manifest_upl) noexcept override {
        co_return error_outcome::unexpected_failure;
    }

    /// Add metadata to the manifest by replicating archival metadata
    /// configuration batch
    ss::future<result<admit_uploads_result>> admit_uploads(
      retry_chain_node& workflow_rtc,
      upload_results_list upl_res) noexcept override {
        co_return error_outcome::unexpected_failure;
    }

    /// Reupload manifest and replicate configuration batch
    ss::future<result<manifest_upload_result>>
    upload_manifest(retry_chain_node&, model::ntp) noexcept override {
        co_return error_outcome::unexpected_failure;
    }

private:
    ss::gate _gate [[maybe_unused]];
    ss::abort_source _as [[maybe_unused]];
    retry_chain_node _rtc [[maybe_unused]];
    retry_chain_logger _rtclog [[maybe_unused]];
    ss::shared_ptr<cloud_storage_remote_api> _api [[maybe_unused]];
    ss::shared_ptr<segment_upload_builder_api> _upl_builder [[maybe_unused]];
    ss::shared_ptr<cluster_partition_manager_api> _pm [[maybe_unused]];
    config::binding<size_t> _read_buffer_size [[maybe_unused]];
    config::binding<int16_t> _readahead [[maybe_unused]];
    cloud_storage_clients::bucket_name _bucket [[maybe_unused]];
    ss::scheduling_group _sg [[maybe_unused]]; // TODO: remove unused
};

ss::shared_ptr<archiver_operations_api> make_archiver_operations_api(
  ss::shared_ptr<cloud_storage_remote_api> remote,
  ss::shared_ptr<cluster_partition_manager_api> pm,
  ss::shared_ptr<segment_upload_builder_api> upl,
  cloud_storage_clients::bucket_name bucket) {
    return ss::make_shared<archiver_operations_impl>(
      std::move(remote),
      std::move(pm),
      std::move(upl),
      ss::default_scheduling_group(), // TODO: use proper scheduling group
      std::move(bucket));
}

} // namespace detail

} // namespace archival
