/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/archival/archiver_operations_impl.h"

#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/remote_probe.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/archiver_operations_api.h"
#include "cluster/archival/async_data_uploader.h"
#include "cluster/archival/types.h"
#include "cluster/errc.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "storage/batch_consumer_utils.h"
#include "storage/segment_reader.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"
#include "utils/stream_utils.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>

namespace archival {
namespace detail {

inline std::tuple<remote_segment_path, ss::sstring, remote_segment_path>
segment_index_object_names(
  const ss::shared_ptr<cluster_partition_api>& part,
  const reconciled_upload_candidate_ptr& upl) {
    auto path = part->get_remote_segment_path(upl->metadata);
    auto ix = fmt::format("{}.index", path().native());
    auto tx = fmt::format("{}.tx", path().native());
    return std::make_tuple(path, ix, remote_segment_path(tx));
}

/// Cloning upload with the exception of tx data
inline std::tuple<ss::input_stream<char>, ss::input_stream<char>>
clone_stream(ss::input_stream<char> source, int16_t readahead) {
    auto res = input_stream_fanout<2>(std::move(source), readahead);

    auto [lhs, rhs] = std::move(res);
    return std::make_tuple(std::move(lhs), std::move(rhs));
}

class archiver_operations_impl : public archiver_operations_api {
    /// Combined result of several uploads
    struct aggregated_upload_result {
        cloud_storage::upload_result code{
          cloud_storage::upload_result::success};
        /// Populated on success
        std::optional<cloud_storage::segment_record_stats> stats;
        /// Total number of PUT requests used
        size_t put_requests{0};
        /// Total number of bytes sent
        size_t bytes_sent{0};

        // Combine aggregated results
        ///
        /// The error codes are combined using max function (the worst
        /// error wins). The order is: success < timeout < failure < cancelled.
        /// The 'stats' objects can't be combined so the series of aggregated
        /// results can't contains more than one 'stats' instances.
        void combine(const aggregated_upload_result& other) {
            put_requests += other.put_requests;
            bytes_sent += other.bytes_sent;
            if (!stats.has_value()) {
                stats = other.stats;
            }
            code = std::max(code, other.code);
        }
    };

public:
    /// C-tor
    archiver_operations_impl(
      std::unique_ptr<cloud_storage_remote_api> api,
      std::unique_ptr<cluster_partition_manager_api> pm,
      std::unique_ptr<segment_upload_builder_api> upl_builder,
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

    ss::future<> start() override { co_return; }

    ss::future<> stop() override { co_await _gate.close(); }

    /// Return upload candidate(s) if data is available or not_enough_data
    /// error if there is not enough data to start an upload.
    ss::future<result<reconciled_upload_candidates_list>>
    find_upload_candidates(
      retry_chain_node& workflow_rtc,
      upload_candidate_search_parameters arg) noexcept override {
        vlog(_rtclog.debug, "find_upload_candidates {}", arg);
        try {
            auto gate = _gate.hold();
            auto partition = _pm->get_partition(arg.ntp);
            if (partition == nullptr) {
                // maybe race condition (partition was stopped or moved)
                vlog(
                  _rtclog.debug,
                  "find_upload_candidates - can't find partition {}",
                  arg.ntp);
                co_return error_outcome::unexpected_failure;
            }
            reconciled_upload_candidates_list result(
              arg.ntp, {}, partition->get_applied_offset());
            auto base_offset = partition->get_next_uploaded_offset();

            // If not limit is specified (quota is set to nullopt) we don't want
            // to upload unlimited amount of data in one iteration. The defaults
            // in this case are configured to allow only three segments to be
            // uploaded.
            auto size_quota = arg.upload_size_quota.value_or(
              arg.target_size * 3);
            // By default we will be uploading up to 4 segments
            // with up to 3 requests to upload one segment.
            constexpr size_t default_req_quota = 12;

            auto req_quota = arg.upload_requests_quota.value_or(
              default_req_quota);

            size_limited_offset_range range(
              base_offset, arg.target_size, arg.min_size);

            retry_chain_node op_rtc(&workflow_rtc);

            vlog(
              _rtclog.debug,
              "start collecting segments, base_offset {}, size quota {}, "
              "requests "
              "quota {}",
              base_offset,
              size_quota,
              req_quota);

            while (size_quota > 0 && req_quota > 0) {
                auto upload = co_await make_non_compacted_upload(
                  base_offset, partition, arg, _sg, op_rtc.get_deadline());

                if (
                  upload.has_error()
                  && upload.error() == error_outcome::not_enough_data) {
                    vlog(
                      _rtclog.debug,
                      "make_non_compacted_upload failed {}",
                      upload.error());
                    break;
                }
                if (upload.has_error()) {
                    vlog(
                      _rtclog.error,
                      "make_non_compacted_upload failed {}",
                      upload.error());
                    co_return upload.error();
                }
                vlog(
                  _rtclog.debug,
                  "make_non_compacted_upload success {}",
                  upload.value());
                base_offset = model::next_offset(
                  upload.value()->metadata.committed_offset);
                size_quota -= upload.value()->size_bytes;
                // 2 PUT requests for segment upload + index upload
                req_quota -= 2;
                if (upload.value()->metadata.metadata_size_hint > 0) {
                    // another PUT request if tx-manifest is not empty
                    req_quota -= 1;
                }
                result.results.push_back(std::move(upload.value()));
            }

            vlog(
              _rtclog.debug,
              "find_upload_candidates completed with {} results, read-write "
              "fence "
              "{}",
              result.results.size(),
              result.read_write_fence);
            // TODO: compacted upload (if possible)
            // TODO: merge adjacent segments
            co_return std::move(result);
        } catch (...) {
            vlog(
              _rtclog.error,
              "Failed to create upload candidate: {}",
              std::current_exception());
        }
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
        try {
            auto gate = _gate.hold();
            upload_results_list result;
            auto partition = _pm->get_partition(bundle.ntp);
            if (partition == nullptr) {
                // maybe race condition (partition was stopped or moved)
                vlog(
                  _rtclog.debug,
                  "schedule_uploads - can't find partition {}",
                  bundle.ntp);
                co_return error_outcome::unexpected_failure;
            }
            std::deque<cloud_storage::segment_meta> metadata;
            chunked_vector<ss::future<aggregated_upload_result>>
              segment_uploads;
            chunked_vector<ss::future<aggregated_upload_result>>
              manifest_uploads;
            for (auto& upl : bundle.results) {
                metadata.push_back(upl->metadata);
                segment_uploads.emplace_back(
                  upload_candidate(workflow_rtc, partition, upl));
            }
            model::offset projected_clean_offset
              = partition->manifest().get_insync_offset();
            if (inline_manifest_upl) {
                const auto& manifest = partition->manifest();
                const auto estimated_manifest_upl_size
                  = manifest.estimate_serialized_size();
                auto key = partition->get_remote_manifest_path(manifest);
                manifest_uploads.emplace_back(
                  _api
                    ->upload_manifest(
                      _bucket, manifest, std::move(key), workflow_rtc)
                    .then([estimated_manifest_upl_size](
                            cloud_storage::upload_result r) {
                        return aggregated_upload_result{
                          .code = r,
                          .put_requests = 1,
                          // Use size estimate instead of the actual value
                          .bytes_sent = estimated_manifest_upl_size};
                    }));
            }

            // Wait for all uploads to complete
            std::vector<ss::future<aggregated_upload_result>>
              segment_result_vec;
            std::vector<ss::future<aggregated_upload_result>>
              manifest_result_vec;
            if (manifest_uploads.empty()) {
                segment_result_vec = co_await ss::when_all(
                  segment_uploads.begin(), segment_uploads.end());
            } else {
                auto [first, second] = co_await ss::coroutine::all(
                  [&segment_uploads] {
                      return ss::when_all(
                        segment_uploads.begin(), segment_uploads.end());
                  },
                  [&manifest_uploads] {
                      return ss::when_all(
                        manifest_uploads.begin(), manifest_uploads.end());
                  });
                segment_result_vec = std::move(first);
                manifest_result_vec = std::move(second);
            }
            size_t num_put_requests = 0;
            size_t num_bytes_sent = 0;

            // Process manifest upload results.
            model::offset manifest_clean_offset;
            if (inline_manifest_upl) {
                vassert(
                  manifest_result_vec.size() == 1,
                  "Manifest upload wasn't scheduled correctly {}",
                  manifest_result_vec.size());
                if (manifest_result_vec.back().failed()) {
                    // Manifest upload failed, we shouldn't add clean command
                    vlog(
                      _rtclog.error,
                      "Manifest upload failed {}",
                      manifest_result_vec.back().get_exception());
                } else {
                    auto m_res = manifest_result_vec.back().get();
                    if (m_res.code != cloud_storage::upload_result::success) {
                        // Same here
                        vlog(
                          _rtclog.error,
                          "Manifest upload failed {}",
                          m_res.code);
                    } else {
                        // Manifest successfully uploaded
                        manifest_clean_offset = projected_clean_offset;
                    }
                    num_put_requests += m_res.put_requests;
                    num_bytes_sent += m_res.bytes_sent;
                }
            }

            // Process segment upload results.
            std::deque<std::optional<cloud_storage::segment_record_stats>>
              upload_stats;
            std::deque<cloud_storage::upload_result> upload_results;
            for (auto& res : segment_result_vec) {
                if (res.failed()) {
                    vlog(
                      _rtclog.error,
                      "Segment upload failed {}",
                      res.get_exception());
                    upload_stats.emplace_back(std::nullopt);
                    upload_results.push_back(
                      cloud_storage::upload_result::failed);
                    continue;
                }

                auto op_result = std::move(res).get();
                num_put_requests += op_result.put_requests;
                num_bytes_sent += op_result.bytes_sent;

                if (op_result.code != cloud_storage::upload_result::success) {
                    vlog(
                      _rtclog.error,
                      "Segment upload failed {}",
                      op_result.code);
                }

                upload_stats.push_back(op_result.stats);
                upload_results.push_back(op_result.code);
            }

            upload_results_list results(
              bundle.ntp,
              std::move(upload_stats),
              std::move(upload_results),
              std::move(metadata),
              manifest_clean_offset,
              bundle.read_write_fence,
              num_put_requests,
              num_bytes_sent);

            co_return std::move(results);
        } catch (...) {
            vlog(
              _rtclog.error,
              "Unexpected 'schedule_uploads' exception {}",
              std::current_exception());
            co_return error_outcome::unexpected_failure;
        }
        __builtin_unreachable();
    }

    /// Add metadata to the manifest by replicating archival metadata
    /// configuration batch
    ss::future<result<admit_uploads_result>> admit_uploads(
      retry_chain_node& workflow_rtc,
      upload_results_list upl_res) noexcept override {
        // Generate archival metadata for every uploaded segment and
        // apply it in offset order.
        // Stop on first error.
        // Validate consistency.
        try {
            auto gate = _gate.hold();
            size_t num_segments = upl_res.results.size();
            if (
              num_segments != upl_res.stats.size()
              || num_segments != upl_res.metadata.size()) {
                vlog(
                  _rtclog.error,
                  "Bad 'admit_uploads' input. Number of segments: {}, number "
                  "of "
                  "stats: {}, number of metadata records: {}",
                  num_segments,
                  upl_res.stats.size(),
                  upl_res.metadata.size());
                co_return error_outcome::unexpected_failure;
            }
            bool validation_required
              = !config::shard_local_cfg()
                   .cloud_storage_disable_upload_consistency_checks();
            auto part = _pm->get_partition(upl_res.ntp);
            if (part == nullptr) {
                vlog(
                  _rtclog.info,
                  "admit_uploads - can't find partition {}",
                  upl_res.ntp);
                co_return error_outcome::unexpected_failure;
            }
            std::vector<cloud_storage::segment_meta> metadata;
            for (size_t ix = 0; ix < num_segments; ix++) {
                auto sg = upl_res.results.at(ix);
                auto st = upl_res.stats.at(ix);
                auto meta = upl_res.metadata.at(ix);

                if (sg != upload_result::success) {
                    break;
                }
                // validate meta against the record stats
                if (st.has_value() && validation_required) {
                    if (!this->segment_meta_matches_stats(
                          meta, st.value(), _rtclog)) {
                        break;
                    }
                } else {
                    vlog(
                      _rtclog.debug,
                      "Segment self-validation skipped, meta: {}, record stats "
                      "available: {}, validation_required: {}",
                      meta,
                      st.has_value(),
                      validation_required);
                }
                metadata.push_back(meta);
            }
            // optionally validate the data
            bool is_validated = false;
            if (validation_required) {
                auto num_accepted = part->manifest().safe_segment_meta_to_add(
                  metadata);
                if (num_accepted == 0) {
                    vlog(
                      _rtclog.error,
                      "Metadata can't be replicated because of the validation "
                      "error");
                    co_return cloud_storage::error_outcome::failure;
                } else if (num_accepted < metadata.size()) {
                    vlog(
                      _rtclog.warn,
                      "Only {} segments can be admitted, segments: {}",
                      num_accepted,
                      metadata);
                    metadata.resize(num_accepted);
                }
                // TODO: probe->gap_detected(...)
                is_validated = true;
            }
            auto num_succeeded = metadata.size();
            auto num_failed = upl_res.results.size() - num_succeeded;
            // replicate metadata
            // add clean command if needed
            auto offset_to_opt = [](model::offset o) {
                std::optional<model::offset> r;
                if (o != model::offset{}) {
                    r = o;
                }
                return r;
            };

            auto replication_result = co_await part->add_segments(
              std::move(metadata),
              offset_to_opt(upl_res.manifest_clean_offset),
              offset_to_opt(upl_res.read_write_fence),
              part->get_highest_producer_id(),
              workflow_rtc.get_deadline(),
              _as,
              is_validated);

            if (replication_result.has_error()) {
                vlog(
                  _rtclog.error,
                  "Failed to replicate archival metadata: {}",
                  replication_result.error());
            }
            co_return admit_uploads_result{
              .ntp = upl_res.ntp,
              .num_succeeded = num_succeeded,
              .num_failed = num_failed,
              .manifest_dirty_offset = replication_result.value(),
            };
        } catch (...) {
            co_return error_outcome::unexpected_failure;
        }
        __builtin_unreachable();
    }

    /// Reupload manifest and replicate configuration batch
    ss::future<result<manifest_upload_result>> upload_manifest(
      retry_chain_node& workflow_rtc, model::ntp ntp) noexcept override {
        try {
            auto gate = _gate.hold();
            auto partition = _pm->get_partition(ntp);
            const auto& manifest = partition->manifest();
            const auto estimated_size = manifest.estimate_serialized_size();
            auto key = partition->get_remote_manifest_path(manifest);
            auto res = co_await _api->upload_manifest(
              _bucket, manifest, std::move(key), workflow_rtc);
            switch (res) {
            case upload_result::cancelled:
                co_return error_outcome::shutting_down;
            case upload_result::timedout:
                co_return error_outcome::timed_out;
            case upload_result::failed:
                co_return error_outcome::unexpected_failure;
            case upload_result::success:
                co_return manifest_upload_result{
                  .ntp = ntp,
                  .num_put_requests = 1,
                  .size_bytes = estimated_size,
                };
            }
        } catch (...) {
            vlog(
              _rtclog.error,
              "Can't upload manifest due to exception {}",
              std::current_exception());
            co_return error_outcome::unexpected_failure;
        }
        __builtin_unreachable();
    }

private:
    ss::future<result<ss::lw_shared_ptr<reconciled_upload_candidate>>>
    make_non_compacted_upload(
      model::offset base_offset,
      ss::shared_ptr<cluster_partition_api> part,
      upload_candidate_search_parameters arg,
      ss::scheduling_group sg,
      ss::lowres_clock::time_point deadline) noexcept {
        vlog(
          _rtclog.debug,
          "make_non_compacted_upload base_offset {}, arg {}",
          base_offset,
          arg);
        try {
            auto guard = _gate.hold();
            auto candidate = ss::make_lw_shared<reconciled_upload_candidate>();
            size_limited_offset_range range(
              base_offset, arg.target_size, arg.min_size);
            auto upload = co_await _upl_builder->prepare_segment_upload(
              part, range, _read_buffer_size(), sg, deadline);
            if (upload.has_error()) {
                if (
                  upload.error() != archival::error_outcome::not_enough_data) {
                    vlog(
                      _rtclog.warn,
                      "prepare_segment_upload failed {}",
                      upload.error().message());
                }
                co_return upload.error();
            }
            auto res = std::move(upload.value());
            vlog(
              _rtclog.debug,
              "prepare_segment_upload returned meta {}, offsets {}, payload "
              "size "
              "{}",
              res->meta,
              res->offsets,
              res->size_bytes);

            // Convert metadata
            auto delta_offset = part->offset_delta(res->offsets.base);

            auto delta_offset_next = part->offset_delta(
              model::next_offset(res->offsets.last));

            // FIXME: make sure that the upload doesn't cross the term boundary
            auto segment_term = part->get_offset_term(res->offsets.base);
            if (!segment_term.has_value()) {
                vlog(
                  _rtclog.error,
                  "Can't find term for offset {}",
                  res->offsets.base);
                co_return error_outcome::offset_not_found;
            }

            cloud_storage::segment_meta segm_meta{
              .is_compacted = res->is_compacted,
              .size_bytes = res->size_bytes,
              .base_offset = res->offsets.base,
              .committed_offset = res->offsets.last,
              // Timestamps will be populated during the upload
              .base_timestamp = {},
              .max_timestamp = {},
              .delta_offset = delta_offset,
              .ntp_revision = part->get_initial_revision(),
              .archiver_term = arg.archiver_term,
              /// Term of the segment (included in segment file name)
              .segment_term = segment_term.value(),
              .delta_offset_end = delta_offset_next,
              .sname_format = cloud_storage::segment_name_format::v3,
              /// Size of the tx-range (in v3 format)
              .metadata_size_hint = 0,
            };

            candidate->ntp = arg.ntp;
            candidate->size_bytes = res->size_bytes;
            candidate->metadata = segm_meta;

            // Generate payload
            candidate->payload = std::move(res->payload);

            // Generate tx-manifest data
            auto tx = co_await get_aborted_transactions(
              part, res->offsets.base, res->offsets.last);
            candidate->tx = std::move(tx);

            if (candidate->tx.size() > 0) {
                // This value has to be set to 0 by default to avoid
                // downloading the tx-manifest in case if there were
                // no transactions in the offset range.
                candidate->metadata.metadata_size_hint = candidate->tx.size();
            }

            co_return std::move(candidate);
        } catch (...) {
            vlog(
              _rtclog.error,
              "Failed to create non-compacted upload candidate {}",
              std::current_exception());
        }
        co_return error_outcome::unexpected_failure;
    }

    ss::future<fragmented_vector<model::tx_range>> get_aborted_transactions(
      ss::shared_ptr<cluster_partition_api> part,
      model::offset base,
      model::offset last) {
        auto guard = _gate.hold();
        co_return co_await part->aborted_transactions(base, last);
    }

    /// Generate and upload segment index
    ///
    /// The method consumes a segment as a stream. While the segment
    /// index is generated it also computes segment stats. Then it compares
    /// the stats with the metadata of the segment and returns error in case
    //// of the mismatch. The last step is to upload the segment index.
    ss::future<aggregated_upload_result> upload_segment_index(
      std::reference_wrapper<retry_chain_node> workflow_rtc,
      std::reference_wrapper<cloud_storage::segment_record_stats> stats,
      reconciled_upload_candidate_ptr upload,
      std::string_view index_path,
      ss::input_stream<char> stream) noexcept {
        try {
            vlog(
              _rtclog.debug, "creating remote segment index: {}", index_path);
            auto base_kafka_offset = upload->metadata.base_kafka_offset();
            constexpr auto index_sampling_step
              = 64_KiB; // TODO: use proper constant

            cloud_storage::offset_index ix{
              upload->metadata.base_offset,
              base_kafka_offset,
              0,
              index_sampling_step,
              upload->metadata.base_timestamp};

            auto builder = cloud_storage::make_remote_segment_index_builder(
              upload->ntp,
              std::move(stream),
              ix,
              upload->metadata.delta_offset,
              index_sampling_step,
              stats);

            auto res = co_await builder->consume().finally(
              [&builder] { return builder->close(); });

            if (res.has_error()) {
                vlog(
                  _rtclog.error,
                  "failed to create remote segment index: {}, error: {}",
                  index_path,
                  res.error());
                co_return upload_result::failed;
            }
            // Compare stats to the expectation
            // return upload_result.
            const bool checks_disabled
              = config::shard_local_cfg()
                  .cloud_storage_disable_upload_consistency_checks.value();
            if (!checks_disabled) {
                // Compare against manifest
                if (
                  !upload->metadata.is_compacted
                  && !segment_meta_matches_stats(
                    upload->metadata, stats, _rtclog)) {
                    co_return cloud_storage::upload_result::failed;
                }
            }
            // Upload index
            auto payload = ix.to_iobuf();
            auto ix_size = payload.size_bytes();
            auto ix_stream = make_iobuf_input_stream(std::move(payload));

            // If we fail to upload the index but successfully upload the
            // segment, the read path will create the index on the fly while
            // downloading the segment, so it is okay to ignore the index upload
            // failure, we still want to advance the offsets because the segment
            // did get uploaded.
            std::ignore = co_await _api->upload_stream(
              cloud_storage_clients::bucket_name(_bucket()),
              cloud_storage_clients::object_key(index_path),
              ix_size,
              std::move(ix_stream),
              cloud_storage::upload_type::segment_index,
              workflow_rtc);

            co_return aggregated_upload_result{
              .code = cloud_storage::upload_result::success,
              .put_requests = 1,
              .bytes_sent = ix_size,
            };
        } catch (...) {
            vlog(
              _rtclog.error,
              "Failed to build an index, unexpected error: {}",
              std::current_exception());
            co_return aggregated_upload_result{
              .code = cloud_storage::upload_result::failed};
        }
        vassert(false, "unreachable");
    }

    static bool segment_meta_matches_stats(
      const cloud_storage::segment_meta& meta,
      const cloud_storage::segment_record_stats& stats,
      retry_chain_logger& ctxlog) {
        // Validate segment content. The 'stats' is computed when
        // the actual segment is scanned and represents the 'ground truth' about
        // its content. The 'meta' is the expected segment metadata. We
        // shouldn't replicate it if it doesn't match the 'stats'.
        if (
          meta.size_bytes != stats.size_bytes
          || meta.base_offset != stats.base_rp_offset
          || meta.committed_offset != stats.last_rp_offset
          || static_cast<size_t>(meta.delta_offset_end - meta.delta_offset)
               != stats.total_conf_records) {
            vlog(
              ctxlog.error,
              "Metadata of the uploaded segment [size: {}, base: {}, last: {}, "
              "begin delta: {}, end delta: {}] "
              "doesn't match the segment [size: {}, base: {}, last: {}, total "
              "config records: {}]",
              meta.size_bytes,
              meta.base_offset,
              meta.committed_offset,
              meta.delta_offset,
              meta.delta_offset_end,
              stats.size_bytes,
              stats.base_rp_offset,
              stats.last_rp_offset,
              stats.total_conf_records);
            return false;
        }
        return true;
    }

    /// Upload segment with index and tx-manifest
    ///
    /// The result is a combination of all upload results
    /// plus the segment metadata generated during the index build
    /// step.
    ss::future<aggregated_upload_result> upload_candidate(
      std::reference_wrapper<retry_chain_node> workflow_rtc,
      ss::shared_ptr<cluster_partition_api> partition,
      reconciled_upload_candidate_ptr upl) {
        auto [segment_name, index_name, tx_name] = segment_index_object_names(
          partition, upl);

        cloud_storage::segment_record_stats stats{};

        auto [index_stream, upload_stream] = clone_stream(
          std::move(upl->payload), _readahead());

        auto make_su_fut = upload_segment(
          workflow_rtc, segment_name, upl, std::move(upload_stream));
        auto make_ix_fut = upload_segment_index(
          workflow_rtc, stats, upl, index_name, std::move(index_stream));

        std::vector<ss::future<aggregated_upload_result>> uploads;
        uploads.reserve(3);
        uploads.emplace_back(std::move(make_su_fut));
        uploads.emplace_back(std::move(make_ix_fut));

        if (!upl->tx.empty()) {
            auto tx = std::move(upl->tx);
            auto txm = ss::make_shared<cloud_storage::tx_range_manifest>(
              segment_name, std::move(tx));
            auto tx_key = partition->get_remote_manifest_path(*txm);
            uploads.emplace_back(
              _api
                ->upload_manifest(
                  _bucket, *txm, std::move(tx_key), workflow_rtc)
                .then([txm](cloud_storage::upload_result r) {
                    return aggregated_upload_result{
                      .code = r,
                      .put_requests = 1,
                      .bytes_sent = txm->estimate_serialized_size()};
                }));
        } else {
            // put ready future into the vec
            uploads.emplace_back(
              ss::make_ready_future<aggregated_upload_result>(
                aggregated_upload_result{}));
        }
        // Wait result and combine
        auto fut_results = co_await ss::when_all(
          uploads.begin(), uploads.end());
        aggregated_upload_result result{};
        for (auto& f : fut_results) {
            if (f.failed()) {
                vlog(_rtclog.error, "Upload failed {}", f.get_exception());
                result.code = cloud_storage::upload_result::failed;
            } else {
                result.combine(f.get());
            }
        }
        vlog(
          _rtclog.debug,
          "Upload complete, status code: {}, record stats: {}, bytes sent: {}, "
          "PUT requests used: {}",
          result.code,
          result.stats.has_value(),
          result.bytes_sent,
          result.put_requests);
        co_return result;
    }

    ss::future<aggregated_upload_result> upload_segment(
      std::reference_wrapper<retry_chain_node> workflow_rtc,
      remote_segment_path path,
      reconciled_upload_candidate_ptr candidate,
      ss::input_stream<char> payload) {
        vlog(_rtclog.debug, "Uploading segment {} to {}", candidate, path);
        auto response = cloud_storage::upload_result::success;
        size_t num_bytes = 0;
        size_t num_requests = 0;
        auto res = co_await ss::coroutine::as_future(_api->upload_stream(
          _bucket,
          cloud_storage_clients::object_key(path()),
          candidate->size_bytes,
          std::move(payload),
          cloud_storage::upload_type::object,
          workflow_rtc));
        if (!res.failed()) {
            num_requests += 1;
            num_bytes += candidate->size_bytes;
            response = res.get();
        } else {
            auto e = res.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                response = cloud_storage::upload_result::cancelled;
            } else {
                vlog(_rtclog.error, "failed to upload segment {}: {}", path, e);
                response = cloud_storage::upload_result::failed;
            }
        }
        co_return aggregated_upload_result{
          .code = response,
          .put_requests = num_requests,
          .bytes_sent = num_bytes,
        };
    }

    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _rtclog;
    std::unique_ptr<cloud_storage_remote_api> _api;
    std::unique_ptr<segment_upload_builder_api> _upl_builder;
    std::unique_ptr<cluster_partition_manager_api> _pm;
    config::binding<size_t> _read_buffer_size;
    config::binding<int16_t> _readahead;
    cloud_storage_clients::bucket_name _bucket;
    ss::scheduling_group _sg;
};

ss::shared_ptr<archiver_operations_api> make_archiver_operations_api(
  std::unique_ptr<cloud_storage_remote_api> remote,
  std::unique_ptr<cluster_partition_manager_api> pm,
  std::unique_ptr<segment_upload_builder_api> upl,
  cloud_storage_clients::bucket_name bucket) {
    return ss::make_shared<archiver_operations_impl>(
      std::move(remote),
      std::move(pm),
      std::move(upl),
      ss::default_scheduling_group(),
      std::move(bucket));
}

} // namespace detail

struct one_time_stream_provider : public stream_provider {
    explicit one_time_stream_provider(ss::input_stream<char> s)
      : _st(std::move(s)) {}
    ss::input_stream<char> take_stream() override {
        auto tmp = std::exchange(_st, std::nullopt);
        return std::move(tmp.value());
    }
    ss::future<> close() override {
        if (_st.has_value()) {
            return _st->close().then([this] { _st = std::nullopt; });
        }
        return ss::now();
    }
    std::optional<ss::input_stream<char>> _st;
};

class remote_gateway_adapter : public detail::cloud_storage_remote_api {
public:
    using upload_result = cloud_storage::upload_result;

    explicit remote_gateway_adapter(cloud_storage::remote& remote)
      : _remote(remote) {}

    /// Upload object to the cloud storage
    ///
    /// This method is similar to 'cloud_storage::remote::upload_segment' but
    /// it's not performing any retries. The upload workflow should retry in
    /// case of error. Otherwise the retries are invisible to the scheduler and
    /// the workflow. The method can be used to upload different types of
    /// objects. Not only segments. The type of object is encoded in the 'type'
    /// parameter and the key is generic.
    ss::future<upload_result> upload_stream(
      const cloud_storage_clients::bucket_name& bucket,
      cloud_storage_clients::object_key key,
      uint64_t content_length,
      ss::input_stream<char> stream,
      cloud_storage::upload_type type,
      retry_chain_node& parent) override {
        // Without retries we don't have to use lazy abort source
        lazy_abort_source noop(
          []() -> std::optional<ss::sstring> { return std::nullopt; });

        // Segment upload
        auto path = cloud_storage::remote_segment_path(key());

        auto handle_exceptions_fn = [type](ss::future<upload_result> fut) {
            if (fut.failed()) {
                const char* upl_type = "Segment";
                if (type == cloud_storage::upload_type::segment_index) {
                    upl_type = "Segment index";
                }
                vlog(
                  archival_log.error,
                  "{} upload failure, error: {}",
                  upl_type,
                  fut.get_exception());
                return upload_result::failed;
            }
            return fut.get();
        };
        static constexpr size_t num_retries = 1;

        auto result = upload_result::success;
        if (type == cloud_storage::upload_type::segment_index) {
            // The index object is small so it's OK to materialize it
            iobuf payload;
            auto pst = make_iobuf_ref_output_stream(payload);
            co_await ss::copy(stream, pst);
            auto& p = _remote.get_probe();
            result = co_await _remote
                       .upload_object(cloud_storage::upload_request{
                         .transfer_details = {
                           .bucket = bucket,
                           .key = key,
                           .parent_rtc = parent,
                           .success_cb = [&p]() { p.index_upload(); },
                           .failure_cb = [&p]() { p.failed_index_upload(); }},
                         .type = cloud_storage::upload_type::segment_index,
                         .payload = std::move(payload)})
                       .then_wrapped(handle_exceptions_fn);
        } else {
            auto s = std::make_unique<one_time_stream_provider>(
              std::move(stream));

            auto reset_fn = [str = std::move(s)]() mutable {
                return ss::make_ready_future<std::unique_ptr<stream_provider>>(
                  std::move(str));
            };
            result = co_await _remote
                       .upload_segment(
                         bucket,
                         path,
                         content_length,
                         std::move(reset_fn),
                         parent,
                         noop,
                         num_retries)
                       .then_wrapped(handle_exceptions_fn);
        }

        co_return result;
    }

    /// Upload manifest to the cloud storage location
    ///
    /// The location is defined by the manifest itself
    ss::future<upload_result> upload_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage::base_manifest& manifest,
      cloud_storage::remote_manifest_path path,
      retry_chain_node& parent) override {
        co_return co_await _remote.upload_manifest(
          bucket, manifest, path, parent);
    }

private:
    cloud_storage::remote& _remote;
};

class cluster_partition : public detail::cluster_partition_api {
public:
    explicit cluster_partition(ss::lw_shared_ptr<cluster::partition> p)
      : _part(std::move(p)) {}

    const cloud_storage::partition_manifest& manifest() const override {
        return _part->archival_meta_stm()->manifest();
    }

    model::offset get_next_uploaded_offset() const override {
        // We have to increment last offset to guarantee progress.
        // The manifest's last offset contains dirty_offset of the
        // latest uploaded segment but '_policy' requires offset that
        // belongs to the next offset or the gap. No need to do this
        // if we haven't uploaded anything.
        //
        // When there are no segments but there is a non-zero 'last_offset', all
        // cloud segments have been removed for retention. In that case, we
        // still need to take into account 'last_offset'.
        const auto& manifest = _part->archival_meta_stm()->manifest();
        auto last_offset = manifest.get_last_offset();

        auto base_offset = manifest.size() == 0
                               && last_offset == model::offset(0)
                             ? model::offset(0)
                             : last_offset + model::offset(1);

        return base_offset;
    }

    model::offset get_applied_offset() const override {
        return _part->archival_meta_stm()->manifest().get_applied_offset();
    }

    model::producer_id get_highest_producer_id() const override {
        return _part->archival_meta_stm()->manifest().highest_producer_id();
    }

    /// Same as log->offset_delta
    /// \throws std::runtime_error if offset is out of range
    model::offset_delta offset_delta(model::offset o) const override {
        return _part->log()->offset_delta(o);
    }

    /// Get term of the locally available offset
    std::optional<model::term_id>
    get_offset_term(model::offset o) const override {
        return _part->log()->get_term(o);
    }

    model::initial_revision_id get_initial_revision() const override {
        return _part->log()->config().get_initial_revision();
    }

    // aborted transactions
    ss::future<fragmented_vector<model::tx_range>> aborted_transactions(
      model::offset base, model::offset last) const override {
        return _part->aborted_transactions(base, last);
    }

    ss::future<result<model::offset>> add_segments(
      std::vector<cloud_storage::segment_meta> meta,
      std::optional<model::offset> clean_offset,
      std::optional<model::offset> read_write_fence,
      model::producer_id highest_pid,
      ss::lowres_clock::time_point deadline,
      ss::abort_source& as,
      bool is_validated) noexcept override {
        auto batch_builder = _part->archival_meta_stm()->batch_start(
          deadline, as);
        if (
          read_write_fence.has_value()
          && read_write_fence.value() != model::offset{}) {
            batch_builder.read_write_fence(read_write_fence.value());
        }
        if (
          clean_offset.has_value() && clean_offset.value() != model::offset{}) {
            batch_builder.mark_clean(clean_offset.value());
        }
        batch_builder.add_segments(
          std::move(meta),
          is_validated ? cluster::segment_validated::yes
                       : cluster::segment_validated::no);
        batch_builder.update_highest_producer_id(highest_pid);
        auto error_code = co_await batch_builder.replicate();
        if (error_code) {
            vlog(
              archival_log.error,
              "Failed to replicate archival metadata: {}",
              error_code);
            co_return error_code;
        }
        co_return _part->archival_meta_stm()->manifest().get_applied_offset();
    }

    ss::lw_shared_ptr<cluster::partition> underlying() { return _part; }

    cloud_storage::remote_segment_path
    get_remote_segment_path(const cloud_storage::segment_meta& sm) override {
        const auto& pp = _part->archival_meta_stm()->path_provider();
        return _part->archival_meta_stm()->manifest().generate_segment_path(
          sm, pp);
    }

    cloud_storage::remote_manifest_path
    get_remote_manifest_path(const cloud_storage::base_manifest& m) override {
        const auto& pp = _part->archival_meta_stm()->path_provider();
        switch (m.get_manifest_type()) {
        case cloud_storage::manifest_type::partition:
            return dynamic_cast<const cloud_storage::partition_manifest&>(m)
              .get_manifest_path(pp);
        case cloud_storage::manifest_type::tx_range:
            return dynamic_cast<const cloud_storage::tx_range_manifest&>(m)
              .get_manifest_path();
        default:
            break;
        };
        throw std::runtime_error("manifest type not supported");
    }

private:
    ss::lw_shared_ptr<cluster::partition> _part;
};

class cluster_partition_manager : public detail::cluster_partition_manager_api {
public:
    explicit cluster_partition_manager(cluster::partition_manager& pm)
      : _pm(pm) {}

    ss::shared_ptr<detail::cluster_partition_api>
    get_partition(const model::ntp& ntp) override {
        auto part = _pm.get(ntp);
        if (part == nullptr) {
            return nullptr;
        }
        return ss::make_shared<cluster_partition>(part);
    }

private:
    cluster::partition_manager& _pm;
};

class upload_builder : public detail::segment_upload_builder_api {
    ss::future<result<std::unique_ptr<detail::prepared_segment_upload>>>
    prepare_segment_upload(
      ss::shared_ptr<detail::cluster_partition_api> part,
      size_limited_offset_range range,
      size_t read_buffer_size,
      ss::scheduling_group sg,
      model::timeout_clock::time_point deadline) override {
        // This is supposed to work only with cluster_partition implementation
        // defined above.
        auto pp = part.get();
        auto wrapper = dynamic_cast<cluster_partition*>(pp);
        auto cp = wrapper->underlying();
        //
        auto upl = co_await segment_upload::make_segment_upload(
          cp, range, read_buffer_size, sg, deadline);
        if (upl.has_error()) {
            if (upl.error() != archival::error_outcome::not_enough_data) {
                vlog(
                  archival_log.error,
                  "Can't find upload candidate: {}",
                  upl.error().message());
            }
            co_return upl.error();
        }

        auto meta = upl.value()->get_meta();
        auto payload_stream = co_await std::move(*upl.value()).detach_stream();

        // Meta is not required for correctness but it's logged later
        cloud_storage::segment_meta segm_meta{
          .is_compacted = meta.is_compacted,
          .size_bytes = meta.size_bytes,
          .base_offset = meta.offsets.base,
          .committed_offset = meta.offsets.last,
          // Timestamps will be populated during the upload
          .base_timestamp = {},
          .max_timestamp = {},
          .delta_offset = part->offset_delta(meta.offsets.base),
          .ntp_revision = part->get_initial_revision(),
          .archiver_term = cp->term(),
          .segment_term
          = part->get_offset_term(meta.offsets.base).value_or(model::term_id{}),
          .delta_offset_end = part->offset_delta(meta.offsets.last),
          .sname_format = cloud_storage::segment_name_format::v3,
          /// Size of the tx-range (in v3 format)
          .metadata_size_hint = 0,
        };

        co_return std::make_unique<detail::prepared_segment_upload>(
          detail::prepared_segment_upload{
            .offsets = meta.offsets,
            .size_bytes = meta.size_bytes,
            .is_compacted = meta.is_compacted,
            .meta = segm_meta,
            .payload = std::move(payload_stream),
          });
    }
};

namespace detail {

std::unique_ptr<cluster_partition_api>
make_cluster_partition_wrapper(ss::lw_shared_ptr<cluster::partition> p) {
    return std::make_unique<cluster_partition>(std::move(p));
}

std::unique_ptr<segment_upload_builder_api>
make_segment_upload_builder_wrapper() {
    return std::make_unique<upload_builder>();
}

} // namespace detail

ss::shared_ptr<archiver_operations_api> make_archiver_operations_api(
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cluster::partition_manager>& pm,
  cloud_storage_clients::bucket_name bucket,
  ss::scheduling_group sg) {
    auto a_remote = std::make_unique<remote_gateway_adapter>(remote.local());
    auto a_pm = std::make_unique<cluster_partition_manager>(pm.local());
    auto a_upl = std::make_unique<upload_builder>();

    return ss::make_shared<detail::archiver_operations_impl>(
      std::move(a_remote),
      std::move(a_pm),
      std::move(a_upl),
      sg,
      std::move(bucket));
}

} // namespace archival
