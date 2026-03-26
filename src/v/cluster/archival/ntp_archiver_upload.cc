/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "cluster/archival/adjacent_segment_merger.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/archival_policy.h"
#include "cluster/archival/async_data_uploader.h"
#include "cluster/archival/logger.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/replica_state_validator.h"
#include "cluster/archival/retention_calculator.h"
#include "cluster/archival/scrubber.h"
#include "cluster/archival/segment_reupload.h"
#include "cluster/archival/types.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "net/connection.h"
#include "raft/fundamental.h"
#include "ssx/abort_source.h"
#include "ssx/checkpoint_mutex.h"
#include "ssx/future-util.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/ntp_config.h"
#include "storage/parser.h"
#include "utils/execution_monitor.h"
#include "utils/human.h"
#include "utils/lazy_abort_source.h"
#include "utils/prefix_logger.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <numeric>
#include <stdexcept>
#include <utility>

namespace archival {

namespace {

bool segment_meta_matches_stats(
  const cloud_storage::segment_meta& meta,
  const cloud_storage::segment_record_stats& stats,
  retry_chain_logger& ctxlog,
  bool gaps_allowed) {
    const auto offsets_valid = [&] {
        if (gaps_allowed) {
            return stats.base_rp_offset >= meta.base_offset
                   && stats.last_rp_offset <= meta.committed_offset;
        }
        return meta.base_offset == stats.base_rp_offset
               && meta.committed_offset == stats.last_rp_offset;
    };
    if (
      meta.size_bytes != stats.size_bytes || !offsets_valid()
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

cloud_storage::segment_meta convert_segment_meta(
  const segment_collector_stream& strm,
  const cluster::partition& parent,
  model::initial_revision_id rev,
  model::term_id archiver_term) {
    return cloud_storage::segment_meta{
      .is_compacted = strm.is_compacted,
      .size_bytes = strm.size,
      .base_offset = strm.start_offset,
      .committed_offset = strm.end_offset,
      .base_timestamp = strm.min_timestamp,
      .max_timestamp = strm.max_timestamp,
      .delta_offset = parent.log()->offset_delta(strm.start_offset),
      .ntp_revision = rev,
      .archiver_term = archiver_term,
      .segment_term = strm.term,
      .delta_offset_end = parent.log()->offset_delta(
        model::next_offset(strm.end_offset)),
      .sname_format = cloud_storage::segment_name_format::v3,
      .metadata_size_hint = 0,
    };
}

struct one_time_stream_wrapper final : public stream_provider {
    std::optional<ss::input_stream<char>> stream;

    one_time_stream_wrapper(const one_time_stream_wrapper&) = delete;
    one_time_stream_wrapper(one_time_stream_wrapper&&) = default;
    one_time_stream_wrapper& operator=(const one_time_stream_wrapper&) = delete;
    one_time_stream_wrapper& operator=(one_time_stream_wrapper&&) = default;

    ~one_time_stream_wrapper() override = default;

    explicit one_time_stream_wrapper(ss::input_stream<char> s)
      : stream(std::move(s)) {}

    ss::input_stream<char> take_stream() override {
        vassert(stream.has_value(), "no stream to take");
        ss::input_stream<char> s = std::move(stream.value());
        stream = std::nullopt;
        return s;
    }

    ss::future<> close() override {
        if (stream.has_value()) {
            co_await stream.value().close();
        }
        co_return;
    }
};

std::vector<std::exception_ptr> flatten_exception(const std::exception_ptr& e) {
    std::vector<std::exception_ptr> result;
    chunked_vector<std::exception_ptr> stk;
    stk.push_back(e);
    while (!stk.empty()) {
        auto curr = stk.back();
        stk.pop_back();
        try {
            std::rethrow_exception(curr);
        } catch (const ss::nested_exception& e) {
            stk.push_back(e.inner);
            stk.push_back(e.outer);
        } catch (...) {
            result.push_back(std::current_exception());
        }
    }
    return result;
}

} // namespace

ss::future<ntp_archiver_upload_result> ntp_archiver::upload_segment(
  segment_collector_stream strm,
  const cloud_storage::segment_meta& meta,
  std::optional<chunked_vector<model::tx_range>> tx_ranges) {
    retry_chain_node rtc(
      _conf->segment_upload_timeout(),
      _conf->upload_loop_initial_backoff(),
      &_rtcnode);
    retry_chain_logger ctxlog(archival_log, rtc, _ntp.path());
    auto h = _gate.hold();
    auto path = manifest().generate_segment_path(meta, remote_path_provider());
    auto index_path = cloud_storage::generate_index_path(path).string();
    auto lazy_abort = lazy_abort_source{
      [this]() { return upload_should_abort(); }};
    auto stream = strm.create_input_stream();
    auto [upload_stream, indexing_stream] = input_stream_fanout<2>(
      std::move(stream),
      config::shard_local_cfg().storage_read_readahead_count());

    auto tx_upload_started = maybe_upload_aborted_tx(
      path, std::move(tx_ranges), rtc);

    auto make_index_started = make_segment_index(
      meta.base_offset,
      meta.base_timestamp,
      ctxlog,
      index_path,
      std::move(indexing_stream));

    // NOTE: input_stream_fanout returns non-optional input_streams. we move
    // the upload stream into an optional in order to detect whether
    // cloud_io::remote::upload_stream called the `get_stream` callback (below).
    // If not, we manually close the stream on the other side of the call to
    // upload_segment (also below).

    std::optional<ss::input_stream<char>> stream_state = std::move(
      upload_stream);

    auto get_stream = [&stream_state, &strm] {
        using provider_t = std::unique_ptr<stream_provider>;
        if (!stream_state.has_value()) {
            stream_state = strm.create_input_stream();
        }
        auto prov = std::make_unique<one_time_stream_wrapper>(
          std::move(stream_state).value());
        stream_state.reset();
        return ss::make_ready_future<provider_t>(std::move(prov));
    };

    // Upload segment in foreground and upload tx-manifest and build an
    // index in the background.
    auto upload_segment_ready = co_await ss::coroutine::as_future(
      _remote.upload_segment(
        get_bucket_name(), path, meta.size_bytes, get_stream, rtc, lazy_abort));

    // As noted above, check whether 'get_stream' was called. If not, close the
    // upload stream.
    if (stream_state.has_value()) {
        co_await stream_state->close();
    }

    // This future should be ready at the moment or will
    // be ready very soon. The index building is tied to
    // the segment upload through the fanout stream.
    auto make_index_ready = co_await ss::coroutine::as_future(
      std::move(make_index_started));
    auto tx_upload_ready = co_await ss::coroutine::as_future(
      std::move(tx_upload_started));

    struct error_state {
        ss::sstring source;
        std::exception_ptr e_ptr;
    };
    std::vector<error_state> e_ptr;
    auto log_exception =
      [&e_ptr](std::string_view source, const std::exception_ptr& e) {
          auto flat = flatten_exception(e);
          e_ptr.reserve(e_ptr.size() + flat.size());
          std::ranges::transform(
            flat,
            std::back_inserter(e_ptr),
            [source](const std::exception_ptr& e) -> error_state {
                return {
                  .source = ss::sstring{source.data(), source.size()},
                  .e_ptr = e};
            });
      };
    if (upload_segment_ready.failed()) {
        log_exception("Segment upload", upload_segment_ready.get_exception());
    }
    if (make_index_ready.failed()) {
        log_exception("Index construction", make_index_ready.get_exception());
    }
    if (tx_upload_ready.failed()) {
        log_exception("Tx-manifest upload", tx_upload_ready.get_exception());
    }

    size_t shutdown_error = 0;
    size_t other_failure = 0;
    if (!e_ptr.empty()) {
        for (auto [src, e] : e_ptr) {
            if (ssx::is_shutdown_exception(e)) {
                vlog(
                  ctxlog.debug,
                  "{} ({}) failed due to shutdown error",
                  src,
                  path);
                shutdown_error++;
            } else {
                vlog(ctxlog.warn, "{} ({}) upload failed: {}", src, path, e);
                other_failure++;
            }
        }
    }
    if (other_failure) {
        co_return ntp_archiver_upload_result(
          cloud_storage::upload_result::failed);
    }
    if (shutdown_error) {
        co_return ntp_archiver_upload_result(
          cloud_storage::upload_result::cancelled);
    }
    auto upload_result = upload_segment_ready.get();
    switch (upload_result) {
    case cloud_storage::upload_result::failed:
    case cloud_storage::upload_result::timedout:
        vlog(
          ctxlog.info,
          "Segment upload failed: {}, error: {}",
          path,
          upload_result);
        [[fallthrough]];
    case cloud_storage::upload_result::cancelled:
        co_return ntp_archiver_upload_result(upload_result);
    case cloud_storage::upload_result::success:
        break;
    }
    auto index_res = make_index_ready.get();
    if (!index_res.has_value()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Failed to construct segment for {}", path));
    }

    auto [index, index_stats] = std::move(index_res.value());

    // If we fail to upload the index but successfully upload the segment,
    // the read path will create the index on the fly while downloading the
    // segment, so it is okay to ignore the index upload failure, we still
    // want to advance the offsets because the segment did get uploaded.
    co_await upload_index(index_path, std::move(index));

    co_return ntp_archiver_upload_result(index_stats);

    // Note on segment locks, which were captured by
    // segment_collector_stream::create_input_stream:
    //
    // We've successfully uploaded the segment. Before we replicate an update
    // with the archival STM, drop any segment locks that may be held. It's
    // possible these locks are blocking other fibers from taking write locks,
    // which in turn, may prevent further read locks from being held.
    // Replicating and waiting on archival batches to be applied may require
    // taking read locks on these segments.
    //
    // Specifically, we want to avoid a series of events like:
    // 1. This fiber holds the uploaded segment's read lock
    // 2. Another fiber attempts to write lock the segment (e.g. during a
    //    segment roll), but can't. Instead, it prevents other read locks from
    //    being taken as it waits.
    // 3. This fiber attempts to replicate an archival batch, which
    //    subsequently waits for all prior ops to be applied, which may require
    //    consuming from this segment. In doing so, we attempt to read lock a
    //    locked segment.
    //
    // To avoid this, simply drop the locks here, now that we're done with
    // them. There's no concern that the underlying offsets will disappear
    // since the archival STM pins offsets until they are recorded in the
    // manifest.
}

auto ntp_archiver::do_schedule_single_upload_streaming(
  segment_collector_stream strm,
  model::term_id start_term,
  segment_upload_kind upload_kind) -> ss::future<scheduled_upload> {
    auto sname = segment_name_for_stream(strm, start_term);
    auto meta = convert_segment_meta(strm, _parent, _rev, start_term);
    auto [tx_ranges, tx_size] = co_await get_aborted_transactions(strm, sname);
    meta.metadata_size_hint = tx_size;

    vlog(
      _rtclog.debug,
      "Starting segment upload in the background, name: {}, meta: {}",
      sname,
      meta);

    auto background_upload = upload_segment(
      std::move(strm), meta, std::move(tx_ranges));

    // Happy path, we have found upload candidate and started uploading.
    // The upload is running in the background at the moment or will be
    // running soon.
    co_return scheduled_upload{
      .result = std::move(background_upload),
      .inclusive_last_offset = meta.committed_offset,
      .meta = meta,
      .name = sname, // TODO: check correctness
      .delta = meta.committed_offset - meta.base_offset,
      .stop = ss::stop_iteration::no,
      .upload_kind = upload_kind,
    };
}

ss::future<ntp_archiver::scheduled_upload>
ntp_archiver::schedule_single_upload(const upload_context& upload_ctx) {
    auto start_upload_offset = upload_ctx.start_offset;
    auto last_stable_offset = upload_ctx.end_offset_exclusive;

    auto log = _parent.log();

    segment_collector_stream_result candidate_result{};

    switch (upload_ctx.upload_kind) {
    case segment_upload_kind::non_compacted:
        candidate_result = co_await _policy.get_next_segment(
          start_upload_offset,
          last_stable_offset,
          _flush_uploads_offset,
          log,
          manifest(),
          _conf->segment_upload_timeout());
        break;
    case segment_upload_kind::compacted:
        candidate_result = co_await _policy.get_next_compacted_segment(
          start_upload_offset,
          log,
          manifest(),
          _conf->segment_upload_timeout());
        break;
    }

    co_return co_await ss::visit(
      candidate_result,
      [](std::monostate) -> ss::future<scheduled_upload> {
          vunreachable("Unexpected monostate in candidate stream result");
      },
      [this, &upload_ctx](segment_collector_stream& strm) {
          return do_schedule_single_upload_streaming(
            std::move(strm), upload_ctx.archiver_term, upload_ctx.upload_kind);
      },
      [this, &upload_ctx, start_upload_offset, last_stable_offset](
        skip_offset_range& skip_offsets) {
          vassert(
            upload_ctx.upload_kind == segment_upload_kind::compacted,
            "Invalid request to skip offset range for non-compacted upload: "
            "{}, start upload offset: {}, last stable offset: {}",
            skip_offsets,
            start_upload_offset,
            last_stable_offset);
          const auto log_level = log_level_for_error(skip_offsets.reason);
          vlogl(
            _rtclog,
            log_level,
            "Failed to make upload candidate, skipping offset range: {}",
            skip_offsets);
          return ss::make_ready_future<scheduled_upload>(scheduled_upload{
            .result = std::nullopt,
            .inclusive_last_offset = skip_offsets.end_offset,
            .meta = std::nullopt,
            .name = std::nullopt,
            .delta = std::nullopt,
            .stop = ss::stop_iteration::yes,
            .upload_kind = upload_ctx.upload_kind,
          });
      },
      [this, &upload_ctx](candidate_creation_error& error) {
          const auto log_level = log_level_for_error(error);
          vlogl(
            _rtclog, log_level, "failed to make upload candidate: {}", error);
          return ss::make_ready_future<scheduled_upload>(scheduled_upload{
            .result = std::nullopt,
            .inclusive_last_offset = {},
            .meta = std::nullopt,
            .name = std::nullopt,
            .delta = std::nullopt,
            .stop = ss::stop_iteration::yes,
            .upload_kind = upload_ctx.upload_kind,
          });
      });
}

ss::future<std::vector<ntp_archiver::scheduled_upload>>
ntp_archiver::schedule_uploads(model::offset max_offset_exclusive) {
    // We have to increment last offset to guarantee progress.
    // The manifest's last offset contains dirty_offset of the
    // latest uploaded segment but '_policy' requires offset that
    // belongs to the next offset or the gap. No need to do this
    // if we haven't uploaded anything.
    //
    // When there are no segments but there is a non-zero 'last_offset', all
    // cloud segments have been removed for retention. In that case, we still
    // need to take into accout 'last_offset'.
    auto last_offset = manifest().get_last_offset();
    auto start_upload_offset = manifest().size() == 0
                                   && last_offset == model::offset(0)
                                 ? model::offset(0)
                                 : last_offset + model::offset(1);

    // If tiered storage was paused and gaps were allowed to be created
    // then we need to start from the first offset in the log.
    if (start_upload_offset < _parent.log()->offsets().start_offset) {
        vlog(
          _rtclog.warn,
          "Start upload offset {} is less than log start offset {}. Resetting "
          "start upload offset to log start offset.",
          start_upload_offset,
          _parent.log()->offsets().start_offset);
        start_upload_offset = _parent.log()->offsets().start_offset;
    }

    auto compacted_segments_upload_start = model::next_offset(
      manifest().get_last_uploaded_compacted_offset());

    std::vector<upload_context> params;

    params.push_back({
      .upload_kind = segment_upload_kind::non_compacted,
      .start_offset = start_upload_offset,
      .end_offset_exclusive = max_offset_exclusive,
      .allow_reuploads = allow_reuploads_t::no,
      .archiver_term = _start_term,
    });

    if (
      config::shard_local_cfg().cloud_storage_enable_compacted_topic_reupload()
      && _parent.get_ntp_config().is_locally_compacted()
      && compacted_segments_upload_start < start_upload_offset) {
        params.push_back({
          .upload_kind = segment_upload_kind::compacted,
          .start_offset = compacted_segments_upload_start,
          .end_offset_exclusive = model::offset::max(),
          .allow_reuploads = allow_reuploads_t::yes,
          .archiver_term = _start_term,
        });
    }

    co_return co_await schedule_uploads(std::move(params));
}

ss::future<std::vector<ntp_archiver::scheduled_upload>>
ntp_archiver::schedule_uploads(std::vector<upload_context> loop_contexts) {
    std::vector<scheduled_upload> scheduled_uploads;
    auto uploads_remaining = _concurrency;
    for (auto& ctx : loop_contexts) {
        if (uploads_remaining <= 0) {
            vlog(
              _rtclog.info,
              "no more upload slots remaining, skipping upload kind: {}, start "
              "offset: {}, last offset: {}, uploads remaining: {}",
              ctx.upload_kind,
              ctx.start_offset,
              ctx.end_offset_exclusive,
              uploads_remaining);
            break;
        }

        vlog(
          _rtclog.debug,
          "scheduling uploads, start offset: {}, last offset: {}, upload kind: "
          "{}, uploads remaining: {}",
          ctx.start_offset,
          ctx.end_offset_exclusive,
          ctx.upload_kind,
          uploads_remaining);

        // this metric is only relevant for non compacted uploads.
        if (ctx.upload_kind == segment_upload_kind::non_compacted) {
            _probe.value().upload_lag(
              ctx.end_offset_exclusive - ctx.start_offset);
        }

        std::exception_ptr ep;
        try {
            while (uploads_remaining > 0 && may_begin_uploads()) {
                auto scheduled = co_await schedule_single_upload(ctx);
                ctx.start_offset = model::next_offset(
                  scheduled.inclusive_last_offset);
                scheduled_uploads.push_back(std::move(scheduled));
                const auto& latest_scheduled = scheduled_uploads.back();
                if (latest_scheduled.stop == ss::stop_iteration::yes) {
                    break;
                }
                // Decrement remaining upload count if the last call actually
                // scheduled an upload.
                if (latest_scheduled.result.has_value()) {
                    uploads_remaining -= 1;
                }
            }
        } catch (...) {
            ep = std::current_exception();
        }
        if (ep) {
            vlog(_rtclog.warn, "Failed to schedule upload: {}", ep);
            std::vector<ss::future<>> inflight_uploads;
            for (auto& scheduled : scheduled_uploads) {
                if (scheduled.result.has_value()) {
                    inflight_uploads.emplace_back(
                      std::move(scheduled.result.value()).discard_result());
                }
            }
            auto futs = co_await ss::when_all(
              inflight_uploads.begin(), inflight_uploads.end());
            for (auto& f : futs) {
                if (f.failed()) {
                    auto ex = f.get_exception();
                    vlog(_rtclog.warn, "Upload failed: {}", ex);
                }
            }
            std::rethrow_exception(ep);
        }

        auto upload_segments_count = std::count_if(
          scheduled_uploads.begin(),
          scheduled_uploads.end(),
          [](const auto& upload) { return upload.result.has_value(); });
        vlog(
          _rtclog.debug,
          "scheduled {} uploads for upload kind: {}, uploads remaining: "
          "{}",
          upload_segments_count,
          ctx.upload_kind,
          uploads_remaining);
    }

    co_return scheduled_uploads;
}

ss::future<ntp_archiver::wait_uploads_complete_result>
ntp_archiver::wait_uploads_complete(
  std::vector<scheduled_upload> scheduled,
  segment_upload_kind segment_kind,
  bool inline_manifest) {
    wait_uploads_complete_result result{
      .inline_manifest = inline_manifest,
    };

    std::vector<ss::future<ntp_archiver_upload_result>> flist;
    std::vector<size_t> ixupload;
    for (size_t ix = 0; ix < scheduled.size(); ix++) {
        if (scheduled[ix].result) {
            flist.emplace_back(std::move(*scheduled[ix].result));
            ixupload.push_back(ix);
        }
    }

    if (flist.empty()) {
        vlog(
          _rtclog.debug,
          "no uploads started for segment upload kind: {}, returning",
          segment_kind);
        // The result is empty at this point
        co_return result;
    }

    // We may upload manifest in parallel with segments when using time-based
    // (interval) manifest uploads.  If we aren't using an interval, then the
    // manifest will always be immediately updated after segment uploads, so
    // there is no point doing it in parallel as well.
    bool upload_manifest_in_parallel
      = inline_manifest && _manifest_upload_interval().has_value();

    if (upload_manifest_in_parallel) {
        // Munge the output of maybe_upload_manifest into an upload result,
        // so that we can conveniently await it along with our segment
        // uploads.  The actual result is reflected in
        // _projected_manifest_clean_at if something was uploaded.
        flist.push_back(
          maybe_upload_manifest(concurrent_with_segs_ctx_label).then([](bool) {
              return ntp_archiver_upload_result{
                cloud_storage::upload_result::success};
          }));
    }

    auto segment_results = co_await ss::when_all_succeed(
      begin(flist), end(flist));

    if (upload_manifest_in_parallel) {
        // Drop the upload_result from manifest upload, we do not want to
        // count it in the subsequent success/failure counts.
        // The actual progress of the manifest uploads is tracked inside the
        // 'maybe_upload_manifest' method. The method updates projected clean
        // offset upon success. Later this value is used to replicate
        // 'mark_clean' command so we don't need to propagate the actual error
        // code.
        segment_results.pop_back();
    }

    if (!can_update_archival_metadata()) {
        // We exit early even if we have successfully uploaded some segments to
        // avoid interfering with an archiver that could have started on another
        // node.
        co_return wait_uploads_complete_result{};
    }

    absl::flat_hash_map<cloud_storage::upload_result, size_t> upload_results;
    for (const auto& result : segment_results) {
        ++upload_results[result.result()];
    }

    result.num_succeeded
      = upload_results[cloud_storage::upload_result::success];
    result.num_cancelled
      = upload_results[cloud_storage::upload_result::cancelled];
    result.num_failed = segment_results.size()
                        - (result.num_succeeded + result.num_cancelled);

    result.checks_disabled
      = config::shard_local_cfg()
          .cloud_storage_disable_upload_consistency_checks.value();
    auto skip_metadata_check = [this] {
        // Special handling of the situation when the gap was created
        // while the archiver was paused with 'redpanda.remote.allowgaps'
        // set to 'true'.
        // If the property is set to true and the local start offset
        // doesn't match the last uploaded offset we should skip the
        // check.
        auto manifest_last = manifest().get_last_offset();
        auto local_first = _parent.raft_start_offset();
        model::offset manifest_next = model::next_offset(manifest_last);
        bool gaps_allowed
          = _parent.log()->config().is_remote_allow_gaps_enabled();
        return gaps_allowed && !manifest().empty()
               && manifest_next < local_first;
    }();
    if (!skip_metadata_check && !result.checks_disabled) {
        // With read-write-fence it's guaranteed that the concurrent
        // updates are not a problem. But we still need this check
        // to prevent certain bugs from corrupting the cloud storage
        // metadata.
        // Overall, we're checking that the update makes sense here
        // (that the new segment lines up with the previous one). Then
        // we're checking that the segment actually matches its
        // metadata. And then we're replicating the metadata with the
        // fence that guarantees that no updates are made to the STM
        // state interim.
        // In other words, we're basing the decision to start an upload
        // on the precondition. Then we're validating the actual uploads
        // against this precondition and then we're discarding the
        // changes to the STM state if the precondition is no longer
        // valid.
        std::vector<cloud_storage::segment_meta> meta;
        for (size_t i = 0; i < segment_results.size(); i++) {
            meta.push_back(scheduled[ixupload[i]].meta.value());
        }
        size_t num_accepted = manifest().safe_segment_meta_to_add(
          std::move(meta));
        if (num_accepted < segment_results.size()) {
            vlog(
              _rtclog.warn,
              "Metadata inconsistency detected, {} segments uploaded but only "
              "{} can be added",
              segment_results.size(),
              num_accepted);
            _probe.value().gap_detected(
              model::offset(
                static_cast<int64_t>(segment_results.size() - num_accepted)));
        }
        vassert(
          num_accepted <= segment_results.size(),
          "Accepted {} segments but only {} segments are uploaded",
          num_accepted,
          segment_results.size());
        segment_results.resize(num_accepted);
    }
    for (size_t i = 0; i < segment_results.size(); i++) {
        if (
          segment_results[i].result()
          != cloud_storage::upload_result::success) {
            break;
        }
        const auto& upload = scheduled[ixupload[i]];
        if (!result.checks_disabled && segment_results[i].has_record_stats()) {
            // Validate metadata by comparing it to the segment stats
            // generated during index building process. The stats contains
            // "ground truth" about the uploaded segment because the code that
            // generates it was "looking" at every record batch before it was
            // sent out.
            //
            // By doing this incrementally we can build consistent log metadata
            // because every such check is based on previous state that was
            // also validated using the same procedure.
            auto stats = segment_results[i].record_stats();
            if (
              upload.upload_kind == segment_upload_kind::non_compacted
              && upload.meta.has_value()) {
                if (!segment_meta_matches_stats(
                      *upload.meta,
                      stats,
                      _rtclog,
                      _parent.get_ntp_config()
                        .is_remote_allow_gaps_enabled())) {
                    break;
                }
            }
        }

        if (segment_kind == segment_upload_kind::non_compacted) {
            _probe.value().uploaded(*upload.delta);
            _probe.value().uploaded_bytes(upload.meta->size_bytes);

            model::offset expected_base_offset;
            if (manifest().get_last_offset() < model::offset{0}) {
                expected_base_offset = model::offset{0};
            } else {
                expected_base_offset = manifest().get_last_offset()
                                       + model::offset{1};
            }
        }

        result.meta.push_back(*upload.meta);
    }
    if (result.num_succeeded > result.meta.size()) {
        vlog(
          _rtclog.info,
          "Some segments were discarded due to metadata consistency violation: "
          "{} uploaded vs {} accepted",
          result.num_succeeded,
          result.meta.size());
        auto num_discarded = result.num_succeeded - result.meta.size();
        result.num_succeeded -= num_discarded;
        result.num_failed += num_discarded;
    }
    co_return result;
}

/// Replicate archival metadata
ss::future<ntp_archiver::upload_group_result>
ntp_archiver::replicate_archival_metadata(
  archival_stm_fence fence,
  std::vector<wait_uploads_complete_result> finished_uploads) {
    upload_group_result total{};
    std::vector<cloud_storage::segment_meta> meta;
    bool checks_disabled = true;
    bool inline_manifest = false;
    for (const auto& it : finished_uploads) {
        total.num_cancelled += it.num_cancelled;
        total.num_failed += it.num_failed;
        total.num_succeeded += it.num_succeeded;
        for (const auto& s : it.meta) {
            meta.emplace_back(s);
        }
        checks_disabled = checks_disabled && it.checks_disabled;
        inline_manifest = inline_manifest || it.inline_manifest;
    }

    // Remember if we started with a clean STM: this will be used to decide
    // whether to maybe do an extra flush of manifest after upload, to get back
    // into a clean state.
    auto stm_was_clean = _parent.archival_meta_stm()->get_dirty(
                           _projected_manifest_clean_at)
                         == cluster::archival_metadata_stm::state_dirty::clean;

    if (meta.empty()) {
        vlog(_rtclog.debug, "No upload metadata collected, returning early");
        co_return total;
    }

    if (total.num_succeeded != 0) {
        vassert(
          _parent.archival_meta_stm(),
          "Archival metadata STM is not created for {} archiver",
          _ntp.path());

        auto deadline = ss::lowres_clock::now()
                        + _conf->manifest_upload_timeout();

        std::optional<model::offset> manifest_clean_offset;
        if (
          _projected_manifest_clean_at
          > _parent.archival_meta_stm()->get_last_clean_at()) {
            // If we have a projected clean offset, take this opportunity to
            // persist that to the stm.  This is equivalent to what
            // flush_manifest_clean_offset does, but we're doing it
            // inline with our segment-adding batch.
            manifest_clean_offset = _projected_manifest_clean_at;
        }
        auto highest_producer_id
          = _feature_table.local().is_active(
              features::feature::cloud_metadata_cluster_recovery)
              ? _parent.highest_producer_id()
              : model::producer_id{};

        auto is_validated = checks_disabled ? cluster::segment_validated::no
                                            : cluster::segment_validated::yes;
        cluster::emit_read_write_fence rw_fence = std::nullopt;
        if (fence.emit_rw_fence_cmd) {
            // The fence should be added first because it can only
            // affect commands which are following it in the same record
            // batch.
            vlog(
              archival_log.debug,
              "add_segments, read-write fence: {}, manifest "
              "last "
              "applied offset: {}, manifest in-sync offset: {}",
              fence.read_write_fence,
              _parent.archival_meta_stm()->manifest().get_applied_offset(),
              _parent.archival_meta_stm()->get_insync_offset());
            rw_fence = fence.read_write_fence;
        }
        auto error = co_await _parent.archival_meta_stm()->add_segments(
          meta,
          manifest_clean_offset,
          highest_producer_id,
          deadline,
          _as,
          is_validated,
          rw_fence);

        if (
          error != cluster::errc::success
          && error != cluster::errc::not_leader) {
            vlog(
              _rtclog.warn,
              "archival metadata STM update failed: {}",
              error.message());
        } else {
            // We have flushed projected clean offset if it was set
            if (_projected_manifest_clean_at.has_value()) {
                _last_marked_clean_time = ss::lowres_clock::now();
            }
            _projected_manifest_clean_at.reset();
        }

        vlog(
          _rtclog.debug,
          "successfully uploaded {} segments (failed {} uploads)",
          total.num_succeeded,
          total.num_failed);

        if (
          inline_manifest
          && (stm_was_clean || !_manifest_upload_interval().has_value())) {
            // This is the path for uploading manifests for infrequent*
            // segment uploads: we transitioned from clean to dirty, and the
            // manifest upload interval has expired.
            //
            // * infrequent means we're uploading a segment less often than the
            //   manifest upload interval, so can afford to upload manifest
            //   immediately after each segment upload.
            co_await maybe_upload_manifest(post_add_segs_ctx_label);
        }
    }

    co_return total;
}

ss::future<ntp_archiver::upload_group_result> ntp_archiver::wait_uploads(
  archival_stm_fence fence,
  std::vector<scheduled_upload> scheduled,
  segment_upload_kind segment_kind,
  bool inline_manifest) {
    auto wait_result = co_await wait_uploads_complete(
      std::move(scheduled), segment_kind, inline_manifest);
    co_return co_await replicate_archival_metadata(
      fence, {std::move(wait_result)});
}

ss::future<ntp_archiver::batch_result> ntp_archiver::wait_all_scheduled_uploads(
  archival_stm_fence fence,
  std::vector<ntp_archiver::scheduled_upload> scheduled) {
    // Split the set of scheduled uploads into compacted and non compacted
    // uploads, and then wait for them separately. They can also be waited on
    // together, but in the wait function we stop on the first failed upload.
    // If we wait on them together, a failed upload during compacted schedule
    // will stop any subsequent non-compacted uploads from being processed, and
    // as a result the upload offset will not be advanced for non-compacted
    // uploads.
    // Because the set of uploads advance two different offsets, this is
    // not ideal. A failed compacted segment upload should only stop the
    // compacted offset advance, so we split and wait on them separately.
    std::vector<ntp_archiver::scheduled_upload> non_compacted_uploads;
    std::vector<ntp_archiver::scheduled_upload> compacted_uploads;
    non_compacted_uploads.reserve(scheduled.size());
    compacted_uploads.reserve(scheduled.size());

    std::partition_copy(
      std::make_move_iterator(scheduled.begin()),
      std::make_move_iterator(scheduled.end()),
      std::back_inserter(non_compacted_uploads),
      std::back_inserter(compacted_uploads),
      [](const scheduled_upload& s) {
          return s.upload_kind == segment_upload_kind::non_compacted;
      });

    // Inline manifest upload in regular non-compacted uploads
    // if any were scheduled.
    bool inline_manifest_in_non_compacted_uploads = false;
    for (const auto& i : non_compacted_uploads) {
        if (i.result) {
            inline_manifest_in_non_compacted_uploads = true;
            break;
        }
    }

    auto [non_compacted_result, compacted_result]
      = co_await ss::when_all_succeed(
        wait_uploads_complete(
          std::move(non_compacted_uploads),
          segment_upload_kind::non_compacted,
          inline_manifest_in_non_compacted_uploads),
        wait_uploads_complete(
          std::move(compacted_uploads),
          segment_upload_kind::compacted,
          !inline_manifest_in_non_compacted_uploads));

    // Replicate the metadata
    auto final_result = co_await replicate_archival_metadata(
      fence, {non_compacted_result, compacted_result});

    if (final_result.num_succeeded > 0) {
        _last_segment_upload_time = ss::lowres_clock::now();
    }
    vlog(
      _rtclog.trace,
      "Segment uploads complete: {} successful uploads",
      final_result.num_succeeded);

    co_return batch_result{
      .non_compacted_upload_result = {
        .num_succeeded = non_compacted_result.num_succeeded,
        .num_failed = non_compacted_result.num_failed,
        .num_cancelled = non_compacted_result.num_cancelled,
      },
      .compacted_upload_result = {
        .num_succeeded = compacted_result.num_succeeded,
        .num_failed = compacted_result.num_failed,
        .num_cancelled = compacted_result.num_cancelled,
      },
      };
}

model::offset ntp_archiver::max_uploadable_offset_exclusive() const {
    // We impose an additional (LSO) constraint on the uploadable offset to
    // as we need to have a complete index of aborted transactions if any
    // before we can upload a segment.
    return std::min(
      _parent.last_stable_offset(),
      model::next_offset(_parent.committed_offset()));
}

ss::future<ntp_archiver::batch_result> ntp_archiver::upload_next_candidates(
  archival_stm_fence fence,
  std::optional<model::offset> unsafe_max_offset_override_exclusive) {
    auto max_offset_exclusive = unsafe_max_offset_override_exclusive
                                  ? *unsafe_max_offset_override_exclusive
                                  : max_uploadable_offset_exclusive();
    vlog(
      _rtclog.debug,
      "Uploading next candidates called for {} with max_offset_exclusive={}",
      _ntp,
      max_offset_exclusive);
    ss::gate::holder holder(_gate);
    try {
        auto units = co_await _mutex.get_units(_as);
        auto scheduled_uploads = co_await schedule_uploads(
          max_offset_exclusive);
        co_return co_await wait_all_scheduled_uploads(
          fence, std::move(scheduled_uploads));
    } catch (const ss::gate_closed_exception&) {
    } catch (const ss::abort_requested_exception&) {
    }
    co_return batch_result{
      .non_compacted_upload_result = {}, .compacted_upload_result = {}};
}

uint64_t ntp_archiver::estimate_backlog_size() {
    auto last_offset = manifest().get_last_offset();
    auto log = _parent.log();
    uint64_t total_size = std::accumulate(
      std::begin(log->segments()),
      std::end(log->segments()),
      0UL,
      [last_offset](
        uint64_t acc, const ss::lw_shared_ptr<storage::segment>& s) {
          if (s->offsets().get_dirty_offset() > last_offset) {
              return acc + s->size_bytes();
          }
          return acc;
      });
    // Note: we can safely ignore the fact that the last segment is not uploaded
    // before it's sealed because the size of the individual segment is small
    // compared to the capacity of the data volume.
    return total_size;
}

ss::future<std::optional<cloud_storage::partition_manifest>>
ntp_archiver::maybe_truncate_manifest() {
    retry_chain_node rtc(_as);
    ss::gate::holder gh(_gate);
    retry_chain_logger ctxlog(archival_log, rtc, _ntp.path());
    vlog(ctxlog.info, "archival metadata cleanup started");
    model::offset adjusted_start_offset = model::offset::min();
    const auto& m = manifest();
    for (const auto& meta : m) {
        retry_chain_node fib(
          _conf->manifest_upload_timeout(),
          _conf->upload_loop_initial_backoff(),
          &rtc);
        auto spath = m.generate_segment_path(meta, remote_path_provider());
        auto result = co_await _remote.segment_exists(
          get_bucket_name(), spath, fib);
        if (result == cloud_storage::download_result::notfound) {
            vlog(
              ctxlog.info,
              "archival metadata cleanup, found segment missing from the "
              "bucket: {}",
              spath);
            adjusted_start_offset = meta.committed_offset + model::offset(1);
        } else {
            break;
        }
    }
    std::optional<cloud_storage::partition_manifest> result;
    if (
      adjusted_start_offset != model::offset::min()
      && _parent.archival_meta_stm()) {
        vlog(
          ctxlog.info,
          "archival metadata cleanup, some segments will be removed from the "
          "manifest, start offset before cleanup: {}",
          manifest().get_start_offset());
        auto error = co_await _parent.archival_meta_stm()->truncate(
          adjusted_start_offset,
          ss::lowres_clock::now() + _conf->manifest_upload_timeout(),
          _as);
        if (error != cluster::errc::success) {
            vlog(
              ctxlog.warn,
              "archival metadata STM update failed: {}",
              error.message());
            throw std::system_error(error);
        } else {
            vlog(
              ctxlog.debug,
              "archival metadata STM update passed, re-uploading manifest");

            // The manifest upload will be retried after some time because the
            // STM will have the 'dirty' flag set. `upload_manifest` will log an
            // error so it won't be invisible.
            std::ignore = co_await upload_manifest(sync_local_state_ctx_label);
        }
        vlog(
          ctxlog.info,
          "archival metadata cleanup completed, start offset after cleanup: {}",
          manifest().get_start_offset());
    } else {
        // Nothing to cleanup, return empty manifest
        result = cloud_storage::partition_manifest(_ntp, _rev);
        vlog(
          ctxlog.info,
          "archival metadata cleanup completed, nothing to clean up");
    }
    co_return result;
}

std::ostream& operator<<(std::ostream& os, segment_upload_kind upload_kind) {
    switch (upload_kind) {
    case segment_upload_kind::non_compacted:
        fmt::print(os, "non-compacted");
        break;
    case segment_upload_kind::compacted:
        fmt::print(os, "compacted");
        break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, flush_response fr) {
    switch (fr) {
    case flush_response::accepted:
        fmt::print(os, "accepted");
        break;
    case flush_response::rejected:
        fmt::print(os, "rejected");
        break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, flush_result fr) {
    fmt::print(os, "response: {}, offset: {}", fr.response, fr.offset);
    return os;
}

std::ostream& operator<<(std::ostream& os, wait_result fr) {
    switch (fr) {
    case wait_result::not_in_progress:
        return os << "not in progress";
    case wait_result::complete:
        return os << "complete";
    case wait_result::lost_leadership:
        return os << "lost leadership";
    case wait_result::failed:
        return os << "failed";
    }
    return os;
}

ss::future<ntp_archiver::housekeeping_result> ntp_archiver::housekeeping() {
    auto result = housekeeping_result::complete;
    try {
        if (may_begin_uploads()) {
            // Acquire mutex to prevent concurrency between
            // external housekeeping jobs from upload_housekeeping_service
            // and retention/GC
            auto units = co_await _mutex.get_units(_as);
            if (stm_retention_needed()) {
                co_await apply_retention();
                co_await garbage_collect();
            } else {
                co_await apply_archive_retention();
                result = co_await garbage_collect_archive();
                co_await garbage_collect();
            }
            co_await apply_spillover();
        }
    } catch (const ss::abort_requested_exception&) {
    } catch (const ss::gate_closed_exception&) {
    } catch (const ss::broken_semaphore&) {
    } catch (const ss::semaphore_timed_out&) {
        // Shutdown-type exceptions are thrown, to promptly drop out
        // of the upload loop.
        throw;
    } catch (const std::exception& e) {
        // Unexpected exceptions are logged, and suppressed: we do not
        // want to stop who upload loop because of issues in housekeeping
        result = housekeeping_result::error;
        vlog(_rtclog.warn, "Error occurred during housekeeping: {}", e.what());
    }

    co_return result;
}

ss::future<> ntp_archiver::apply_archive_retention() {
    if (!may_begin_uploads()) {
        co_return;
    }

    const auto& ntp_conf = _parent.get_ntp_config();
    if (!ntp_conf.is_remotely_collectable()) {
        vlog(_rtclog.trace, "NTP is not collectable");
        co_return;
    }

    auto fence = emit_rw_fence();

    std::optional<size_t> retention_bytes = ntp_conf.retention_bytes();
    std::optional<std::chrono::milliseconds> retention_ms
      = ntp_conf.retention_duration();

    auto pinned_offset
      = _parent.raft()->log()->stm_hookset()->lowest_pinned_data_offset();
    auto res = co_await _manifest_view->compute_retention(
      retention_bytes, retention_ms, pinned_offset);

    if (res.has_error()) {
        if (res.error() == cloud_storage::error_outcome::shutting_down) {
            vlog(
              _rtclog.debug,
              "Search for archive retention point failed as Redpanda is "
              "shutting down");
            co_return;
        } else {
            vlog(
              _rtclog.error,
              "Failed to compute archive retention: {}",
              res.error());
            throw std::system_error(res.error());
        }
    }

    if (
      res.value().offset == model::offset{}
      || res.value().offset
           <= _manifest_view->stm_manifest().get_archive_start_offset()) {
        co_return;
    }

    // Replicate metadata
    auto sync_timeout = config::shard_local_cfg()
                          .cloud_storage_metadata_sync_timeout_ms.value();
    auto deadline = ss::lowres_clock::now() + sync_timeout;

    auto batch = _parent.archival_meta_stm()->batch_start(deadline, _as);
    if (fence.emit_rw_fence_cmd) {
        vlog(
          _rtclog.debug,
          "truncate_archive_init, read-write fence: {}",
          fence.read_write_fence);
        batch.read_write_fence(fence.read_write_fence);
    }
    batch.truncate_archive_init(res.value().offset, res.value().delta);
    auto error = co_await batch.replicate();

    if (error != cluster::errc::success) {
        vlog(
          _rtclog.warn,
          "Failed to replicate archive truncation command: {}",
          error.message());

        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Failed to apply archive retention: {}", error));
    } else {
        vlog(
          _rtclog.info,
          "Archive truncated to offset {} (delta: {})",
          res.value().offset,
          res.value().delta);
    }
}

ss::future<ntp_archiver::housekeeping_result>
ntp_archiver::garbage_collect_archive() {
    if (!may_begin_uploads()) {
        co_return housekeeping_result::complete;
    }
    auto fence = emit_rw_fence();
    auto backlog = co_await _manifest_view->get_retention_backlog();
    if (backlog.has_failure()) {
        if (backlog.error() == cloud_storage::error_outcome::shutting_down) {
            vlog(
              _rtclog.debug,
              "Skipping archive GC as Redpanda is shutting down");
            co_return housekeeping_result::complete;
        }

        vlog(
          _rtclog.error,
          "Failed to create GC backlog for the archive: {}",
          backlog.error());
        throw std::system_error(backlog.error());
    }

    std::deque<cloud_storage_clients::object_key> objects_to_remove;
    std::deque<cloud_storage_clients::object_key> manifests_to_remove;

    const auto clean_offset = manifest().get_archive_clean_offset();
    const auto start_offset = manifest().get_archive_start_offset();
    const size_t max_segments = _gc_max_segments();

    vlog(
      _rtclog.info,
      "Garbage collecting archive segments in offset range [{}, {}) with batch "
      "limit {}",
      clean_offset,
      start_offset,
      max_segments);

    if (clean_offset == start_offset) {
        vlog(
          _rtclog.debug,
          "Garbage collection in the archive not required as clean offset "
          "equals the start offset ({})",
          clean_offset);
        co_return housekeeping_result::complete;
    } else if (clean_offset > start_offset) {
        vlog(
          _rtclog.error,
          "Garbage collection requested until offset {}, but start offset is "
          "at {}. Skipping garbage collection.",
          clean_offset,
          start_offset);
        co_return housekeeping_result::error;
    }

    model::offset new_clean_offset{clean_offset};
    // Value includes segments but doesn't include manifests
    size_t bytes_to_remove = 0;
    size_t segments_to_remove_count = 0;

    auto cursor = std::move(backlog.value());

    using eof = cloud_storage::async_manifest_view_cursor::eof;
    while (cursor->get_status()
           == cloud_storage::async_manifest_view_cursor_status::
             materialized_spillover) {
        auto stop = co_await cursor->with_manifest(
          [&](const cloud_storage::partition_manifest& manifest) {
              for (const auto& meta : manifest) {
                  if (meta.committed_offset < clean_offset) {
                      // The manifest is only removed if all segments are
                      // deleted. Because of that we may end up in a situation
                      // when some of the segments are deleted and the rest are
                      // not. The spillover manifest is never adjusted
                      //  and reuploaded after GC.
                      continue;
                  }
                  if (meta.committed_offset < start_offset) {
                      if (segments_to_remove_count >= max_segments) {
                          return true;
                      }
                      const auto path = manifest.generate_segment_path(
                        meta, remote_path_provider());
                      vlog(
                        _rtclog.info,
                        "Enqueuing spillover segment delete from cloud "
                        "storage: {}",
                        path);
                      objects_to_remove.emplace_back(path);
                      new_clean_offset = model::next_offset(
                        meta.committed_offset);
                      bytes_to_remove += meta.size_bytes;
                      ++segments_to_remove_count;
                      // Add index and tx-manifest
                      if (
                        meta.sname_format
                          == cloud_storage::segment_name_format::v3
                        && meta.metadata_size_hint != 0) {
                          objects_to_remove.emplace_back(
                            cloud_storage::generate_remote_tx_path(path));
                      }
                      objects_to_remove.emplace_back(
                        cloud_storage::generate_index_path(path));
                  } else {
                      // This indicates that we need to remove only some of the
                      // segments from the manifest. In this case the outer loop
                      // needs to stop and the current manifest shouldn't be
                      // marked for deletion.
                      return true;
                  }
              }
              return false;
          });

        if (stop) {
            break;
        }
        const auto path = cursor->manifest()->get_manifest_path(
          remote_path_provider());
        vlog(
          _rtclog.info,
          "Enqueuing spillover manifest delete from cloud "
          "storage: {}",
          path);
        manifests_to_remove.emplace_back(path);

        if (segments_to_remove_count >= max_segments) {
            break;
        }

        auto res = co_await cursor->next();
        if (res.has_failure()) {
            if (res.error() == cloud_storage::error_outcome::shutting_down) {
                vlog(
                  _rtclog.debug,
                  "Stopping archive GC as Redpanda is shutting down");
                co_return housekeeping_result::complete;
            }

            vlog(
              _rtclog.error,
              "Failed to load next spillover manifest: {}",
              res.error());
            break;
        } else if (res.value() == eof::yes) {
            // End of stream
            break;
        }
    }

    if (segments_to_remove_count >= max_segments) {
        vlog(
          _rtclog.trace,
          "Archive GC batch limit reached, initial clean offset {}, new clean "
          "offset {}, start offset {}, segments to remove {}",
          clean_offset,
          new_clean_offset,
          start_offset,
          segments_to_remove_count);
    }

    // Drop out if we have no work to do, avoid doing things like the following
    // manifest flushing unnecessarily. This is problematic since, we've already
    // checked that the clean offset is greater than the start offset.
    if (objects_to_remove.empty() && manifests_to_remove.empty()) {
        vlog(_rtclog.error, "Nothing to remove in archive GC");
        co_return housekeeping_result::error;
    }

    if (
      _parent.archival_meta_stm()->get_dirty(_projected_manifest_clean_at)
      != cluster::archival_metadata_stm::state_dirty::clean) {
        auto result = co_await upload_manifest("pre-garbage-collect-archive");
        if (result != cloud_storage::upload_result::success) {
            co_return housekeeping_result::error;
        }
    }

    retry_chain_node fib(
      _conf->garbage_collect_timeout(),
      _conf->cloud_storage_initial_backoff(),
      &_rtcnode);
    const auto delete_result = co_await _remote.delete_objects(
      get_bucket_name(), objects_to_remove, fib);
    const auto all_deletes_succeeded = delete_result
                                       == cloud_storage::upload_result::success;

    if (!all_deletes_succeeded) {
        vlog(
          _rtclog.info,
          "Failed to delete all selected segments from cloud storage. Will "
          "retry on the next housekeeping run.");
        co_return housekeeping_result::error;
    } else {
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;
        auto builder = _parent.archival_meta_stm()->batch_start(deadline, _as);
        if (fence.emit_rw_fence_cmd) {
            vlog(
              _rtclog.debug,
              "cleanup_archive, read-write fence: {}",
              fence.read_write_fence);
            builder.read_write_fence(fence.read_write_fence);
        }
        builder.cleanup_archive(new_clean_offset, bytes_to_remove);
        auto error = co_await builder.replicate();

        if (error != cluster::errc::success) {
            vlog(
              _rtclog.info,
              "Failed to clean up metadata after garbage collection: {}",
              error);
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Failed to clean up metadata after archive GC: {}",
              error));
        } else {
            std::ignore = co_await _remote.delete_objects(
              get_bucket_name(), manifests_to_remove, fib);
        }

        _probe.value().segments_deleted(
          static_cast<int64_t>(segments_to_remove_count));
        vlog(
          _rtclog.info,
          "Archive GC deleted {} segments, clean offset {}, start offset {}",
          segments_to_remove_count,
          new_clean_offset,
          start_offset);

        co_return new_clean_offset < start_offset
          ? housekeeping_result::partial
          : housekeeping_result::complete;
    }
}

ss::future<> ntp_archiver::apply_spillover() {
    const auto manifest_size_limit
      = config::shard_local_cfg().cloud_storage_spillover_manifest_size.value();
    const auto manifest_max_segments
      = config::shard_local_cfg()
          .cloud_storage_spillover_manifest_max_segments.value();
    if (
      manifest_size_limit.has_value() == false
      && manifest_max_segments.has_value() == false) {
        co_return;
    }

    if (!may_begin_uploads()) {
        co_return;
    }
    archival_stm_fence fence = emit_rw_fence();
    const auto manifest_upload_timeout = _conf->manifest_upload_timeout();
    const auto manifest_upload_backoff = _conf->cloud_storage_initial_backoff();

    // Check the spillover invariant.
    // The start_offset of the manifest must be equal to the begin_offset of
    // the first segment in the manifest.
    if (auto so = manifest().get_start_offset();
        so.has_value() && !manifest().empty()) {
        auto fo = manifest().begin()->base_offset;
        if (fo != so.value()) {
            vlog(
              _rtclog.warn,
              "Spillover invariant violated: manifest start_offset {}, first "
              "segment base_offset {}",
              so.value(),
              fo);
            co_return;
        }
    }

    if (manifest_size_limit.has_value()) {
        vlog(
          _rtclog.debug,
          "Manifest size: {}, manifest size limit (x2): {}",
          manifest().segments_metadata_bytes(),
          manifest_size_limit.value() * 2);
    } else {
        vlog(
          _rtclog.debug,
          "Manifest size: {}, manifest number of segments limit (x2): {}",
          manifest().size(),
          manifest_max_segments.value() * 2);
    }
    auto stop_condition = [&] {
        if (manifest_size_limit.has_value()) {
            return manifest().segments_metadata_bytes()
                   < manifest_size_limit.value() * 2;
        }
        return manifest().size() < manifest_max_segments.value() * 2;
    };
    auto spillover_complete = [&](
                                const cloud_storage::spillover_manifest& tail) {
        // Don't allow empty spillover manifests even if the limit
        // is too low.
        if (manifest_size_limit.has_value()) {
            return tail.segments_metadata_bytes() >= manifest_size_limit.value()
                   && tail.size() > 0;
        }
        return tail.size() >= manifest_max_segments.value() && tail.size() > 0;
    };
    while (!stop_condition()) {
        auto tail = [&]() {
            cloud_storage::spillover_manifest tail(_ntp, _rev);
            for (const auto& meta : manifest()) {
                vlog(
                  _rtclog.trace,
                  "Adding segment {} to the spillover manifest that starts at "
                  "{}",
                  meta,
                  tail.get_start_offset().value_or(model::offset{}));
                tail.add(meta);
                // No performance impact since all writes here are
                // sequential.
                tail.flush_write_buffer();
                if (spillover_complete(tail)) {
                    break;
                }
            }
            return tail;
        }();
        vlog(
          _rtclog.info,
          "Preparing spillover: manifest has {} segments and {} bytes, "
          "spillover manifest num elements: {}, size: {} bytes, base: {}, "
          "last: {}",
          manifest().size(),
          manifest().segments_metadata_bytes(),
          tail.size(),
          tail.segments_metadata_bytes(),
          tail.get_start_offset().value_or(model::offset{}),
          tail.get_last_offset());

        const auto first = *tail.begin();
        const auto last = tail.last_segment();
        const auto spillover_meta = tail.make_manifest_metadata();
        vassert(last.has_value(), "Spillover manifest can't be empty");
        vlog(
          _rtclog.info,
          "First batch of the spillover manifest: {}, Last batch of the "
          "spillover manifest: {}, spillover metadata: {}",
          first,
          last,
          spillover_meta);

        retry_chain_node upload_rtc(
          manifest_upload_timeout, manifest_upload_backoff, &_rtcnode);
        const auto path = tail.get_manifest_path(remote_path_provider());
        auto res = co_await _remote.upload_manifest(
          get_bucket_name(), tail, path, upload_rtc);
        if (res != cloud_storage::upload_result::success) {
            vlog(_rtclog.error, "Failed to upload spillover manifest {}", res);
            co_return;
        }
        auto [str, len] = co_await tail.serialize();
        // Put manifest into cache to avoid roundtrip to the cloud storage
        auto reservation = co_await _cache.reserve_space(len, 1);
        co_await _cache.put(
          tail.get_manifest_path(remote_path_provider())(), str, reservation);

        // Spillover manifests were uploaded to S3
        // Replicate metadata
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;

        auto batch = _parent.archival_meta_stm()->batch_start(deadline, _as);
        if (fence.emit_rw_fence_cmd) {
            vlog(
              _rtclog.debug,
              "spillover, read-write fence: {}",
              fence.read_write_fence);
            batch.read_write_fence(fence.read_write_fence);
        }
        batch.spillover(spillover_meta);
        if (manifest().get_archive_start_offset() == model::offset{}) {
            vlog(
              _rtclog.debug,
              "Archive is empty, have to set start archive/clean offset: {}, "
              "and delta: {}",
              first.base_offset,
              first.delta_offset);
            // Enable archive if this is the first spillover manifest. In this
            // case we need to set initial values for
            // archive_start_offset/archive_clean_offset which will be advanced
            // by housekeeping further on.
            batch.truncate_archive_init(first.base_offset, first.delta_offset);
            batch.cleanup_archive(first.base_offset, 0);
        }
        auto error = co_await batch.replicate();
        if (error != cluster::errc::success) {
            vlog(
              _rtclog.warn,
              "Failed to replicate spillover command: {}",
              error.message());
            break;
        } else {
            vlog(
              _rtclog.info,
              "Uploaded spillover manifest: {}",
              tail.get_manifest_path(remote_path_provider()));
        }
        // Reset fence for the next iteration
        fence = emit_rw_fence();
    }
}

flush_result ntp_archiver::flush() {
    // Return early if we are not the leader, or if we are a read replica.
    if (!_parent.is_leader() || _parent.is_read_replica_mode_enabled()) {
        vlog(
          _rtclog.debug,
          "Flush request not accepted, node is not the leader for the "
          "partition, or the node is a read replica");
        return flush_result{
          .response = flush_response::rejected, .offset = std::nullopt};
    }

    _flush_uploads_offset = model::prev_offset(
      max_uploadable_offset_exclusive());
    _wakeup_event.set();
    vlog(
      _rtclog.debug,
      "Accepted flush, flush offset is {}",
      _flush_uploads_offset.value());
    return flush_result{
      .response = flush_response::accepted, .offset = _flush_uploads_offset};
}

ss::future<wait_result> ntp_archiver::wait(model::offset o) {
    ss::gate::holder holder(_gate);

    if (_parent.is_read_replica_mode_enabled()) {
        vlog(
          _rtclog.debug,
          "Cannot wait on a flush in ntp_archiver, node is read replica");
        co_return wait_result::failed;
    }

    // Currently we tie wait() to flush() state. If there is no flush in
    // progress, we return.
    if (!flush_in_progress()) {
        vlog(
          _rtclog.debug,
          "Cannot wait on a flush in ntp_archiver, as no flush in progress");
        co_return wait_result::not_in_progress;
    }

    // If o is outside bounds of _flush_uploads_offset, we indicate so.
    if (o > _flush_uploads_offset.value()) {
        vlog(
          _rtclog.debug,
          "Passed offset {} is outside bounds of current flush offset {}",
          o,
          _flush_uploads_offset.value());
        co_return wait_result::not_in_progress;
    }

    // Save the _start_term before entering wait loop, where we will suspend on
    // condition variable.
    auto wait_issued_term = _start_term;

    while (!uploaded_and_clean_past_offset(o)) {
        if (
          !_parent.is_leader() || wait_issued_term != _start_term
          || !flush_in_progress()) {
            vlog(
              _rtclog.debug,
              "Leadership was lost during flush operation in ntp_archiver");
            co_return wait_result::lost_leadership;
        }

        try {
            vlog(_rtclog.trace, "Waiting on _flush_cond in ntp_archiver");
            co_await _flush_cond.wait();
        } catch (const ss::broken_condition_variable&) {
            // We shutdown while waiting for a flush() to finish.
            // Return a failure result.
            vlog(
              _rtclog.debug,
              "Shutdown was issued during flush operation in ntp_archiver");
            co_return wait_result::failed;
        }
    }

    vlog(_rtclog.debug, "Flush was successfully completed in ntp_archiver.");
    co_return wait_result::complete;
}

bool ntp_archiver::stm_retention_needed() const {
    auto arch_so = manifest().get_archive_start_offset();
    // Return true if there is no archive
    return arch_so == model::offset{};
}

ss::future<> ntp_archiver::apply_retention() {
    if (!may_begin_uploads()) {
        co_return;
    }
    auto fence = emit_rw_fence();
    auto arch_so = manifest().get_archive_start_offset();
    auto stm_so = manifest().get_start_offset();
    if (arch_so != model::offset{} && arch_so != stm_so) {
        // We shouldn't do retention in the part of the log controlled by
        // the archival STM if archive region is not empty. It's unlikely for
        // STM retention and archive retention to work together. Most of the
        // time either STM retention will be happening without spillover or
        // archive retention will be happening alongside spillover from the STM.
        // This is just a safety check that prevents situation when the method
        // is called for log with not empty archive. In this case the retention
        // will remove too much data from the STM region of the log.
        vlog(
          _rtclog.warn,
          "Archive start offset {} is not equal to STM start offset {}, "
          "skipping STM retention",
          arch_so,
          stm_so);
        co_return;
    }

    if (manifest().archive_size_bytes() != 0) {
        vlog(
          _rtclog.error,
          "Size of the archive is not 0, but archival and STM start offsets "
          "are equal ({}). Skipping retention within STM region.",
          arch_so);
        co_return;
    }

    auto pinned_offset
      = _parent.raft()->log()->stm_hookset()->lowest_pinned_data_offset();
    auto retention_calculator = retention_calculator::factory(
      manifest(), _parent.get_ntp_config(), pinned_offset);
    if (!retention_calculator) {
        co_return;
    }

    auto next_start_offset = retention_calculator->next_start_offset();
    if (next_start_offset) {
        vlog(
          _rtclog.info,
          "{} Advancing start offset to {} satisfy retention policy",
          retention_calculator->strategy_name(),
          *next_start_offset);

        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;

        auto builder = _parent.archival_meta_stm()->batch_start(deadline, _as);
        if (fence.emit_rw_fence_cmd) {
            // Currently, the 'unsafe_add' is always set to 'false'
            // because the fence is generated inside this method. It's still
            // good to have this condition in case if this will be changed.
            vlog(
              _rtclog.debug,
              "truncate, read-write fence: {}",
              fence.read_write_fence);
            builder.read_write_fence(fence.read_write_fence);
        }
        builder.truncate(*next_start_offset);

        auto error = co_await builder.replicate();

        if (error != cluster::errc::success) {
            vlog(
              _rtclog.warn,
              "Failed to update archival metadata STM start offset according "
              "to retention policy: {}",
              error);
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Failed to update start offset: {}", error));
        }
    } else {
        vlog(
          _rtclog.debug,
          "{} Retention policies are already met.",
          retention_calculator->strategy_name());
    }
}

ss::future<> ntp_archiver::garbage_collect() {
    if (!may_begin_uploads()) {
        co_return;
    }

    const auto to_remove
      = _parent.archival_meta_stm()->get_segments_to_cleanup();

    // Avoid replicating 'cleanup_metadata_cmd' if there's nothing to remove.
    if (to_remove.size() == 0) {
        co_return;
    }

    archival_stm_fence fence = emit_rw_fence();

    // If we are about to delete segments, we must ensure that the remote
    // manifest is fully up to date, so that it is definitely not referring
    // to any of the segments we will delete in its list of active segments.
    //
    // This is so that read replicas can be sure that if they get a 404
    // on a segment and go re-read the manifest, the latest manifest will
    // not refer to the non-existent segment (apart from in its 'replaced'
    // list)
    if (
      _parent.archival_meta_stm()->get_dirty(_projected_manifest_clean_at)
      != cluster::archival_metadata_stm::state_dirty::clean) {
        // Intentionally not using maybe_upload_manifest, because  that would
        // skip the upload if manifest_upload_interval was not satisfied.
        auto result = co_await upload_manifest("pre-garbage-collect");
        if (result != cloud_storage::upload_result::success) {
            // If we could not write the  manifest, it is not safe to remove
            // segments.
            co_return;
        }
    }

    std::deque<cloud_storage_clients::object_key> objects_to_remove;
    for (const auto& meta : to_remove) {
        const auto path = manifest().generate_segment_path(
          meta, remote_path_provider());
        vlog(_rtclog.info, "Deleting segment from cloud storage: {}", path);

        objects_to_remove.emplace_back(path);
        objects_to_remove.emplace_back(
          cloud_storage::generate_remote_tx_path(path));
        objects_to_remove.emplace_back(
          cloud_storage::generate_index_path(path));
    }

    retry_chain_node fib(
      _conf->garbage_collect_timeout(),
      _conf->cloud_storage_initial_backoff(),
      &_rtcnode);
    const auto delete_result = co_await _remote.delete_objects(
      get_bucket_name(), objects_to_remove, fib);

    const auto backlog_size_exceeded = to_remove.size()
                                       > _max_segments_pending_deletion();
    const auto all_deletes_succeeded = delete_result
                                       == cloud_storage::upload_result::success;
    if (!all_deletes_succeeded && backlog_size_exceeded) {
        vlog(
          _rtclog.warn,
          "The current number of segments pending deletion has exceeded the "
          "configurable limit ({} > {}) and deletion of some segments failed. "
          "Metadata for all remaining segments pending deletion will be "
          "removed and these segments will have to be removed manually.",
          to_remove.size(),
          _max_segments_pending_deletion());
    }

    if (all_deletes_succeeded || backlog_size_exceeded) {
        auto sync_timeout = config::shard_local_cfg()
                              .cloud_storage_metadata_sync_timeout_ms.value();
        auto deadline = ss::lowres_clock::now() + sync_timeout;

        auto builder = _parent.archival_meta_stm()->batch_start(deadline, _as);
        if (fence.emit_rw_fence_cmd) {
            vlog(
              _rtclog.debug,
              "cleanup_metadata, read-write fence: {}",
              fence.read_write_fence);
            builder.read_write_fence(fence.read_write_fence);
        }
        builder.cleanup_metadata();
        auto error = co_await builder.replicate();

        if (error != cluster::errc::success) {
            vlog(
              _rtclog.info,
              "Failed to clean up metadata after garbage collection: {}",
              error);

            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Failed to clean up metadata after GC: {}", error));
        }
    } else {
        vlog(
          _rtclog.info,
          "Failed to delete all selected segments from cloud storage. Will "
          "retry on the next housekeeping run.");
    }

    _probe.value().segments_deleted(
      static_cast<int64_t>(all_deletes_succeeded ? to_remove.size() : 0));
    vlog(
      _rtclog.debug,
      "Deleted {} segments from the cloud",
      all_deletes_succeeded ? to_remove.size() : 0);
}

const cloud_storage_clients::bucket_name&
ntp_archiver::get_bucket_name() const {
    if (_bucket_override) {
        return *_bucket_override;
    } else {
        return _conf->bucket_name;
    }
}

std::vector<std::reference_wrapper<housekeeping_job>>
ntp_archiver::get_housekeeping_jobs() {
    std::vector<std::reference_wrapper<housekeeping_job>> res;
    if (_local_segment_merger) {
        res.emplace_back(std::ref(*_local_segment_merger));
    }

    if (_scrubber) {
        res.emplace_back(std::ref(*_scrubber));
    }

    return res;
}

ss::future<ntp_archiver::find_reupload_candidate_result>
ntp_archiver::find_reupload_candidate(
  manifest_scanner_t scanner, ss::abort_source& caller_as) {
    ss::gate::holder holder(_gate);

    ssx::composite_abort_source cas{caller_as, _as};

    archival_stm_fence rw_fence = emit_rw_fence();

    if (!may_begin_uploads()) {
        co_return find_reupload_candidate_result{};
    }
    auto run = scanner(_parent.raft_start_offset(), manifest());
    if (!run.has_value()) {
        vlog(_rtclog.debug, "Scan didn't resulted in upload candidate");
        co_return find_reupload_candidate_result{};
    } else {
        vlog(_rtclog.debug, "Scan result: {}", run);
    }
    auto units = co_await _mutex.get_units(cas.as());
    if (run->meta.base_offset >= _parent.raft_start_offset()) {
        auto log_generic = _parent.log();
        auto& log = *log_generic;
        segment_collector collector(
          segment_collector_mode::non_compacted_reupload,
          run->meta.base_offset,
          manifest(),
          log,
          // We want to upload exactly the same range as in the run we got based
          // on the manifest so do not limit collected range on the size.
          std::numeric_limits<size_t>::max(),
          run->meta.committed_offset);
        collector.collect_segments();
        auto candidate = co_await collector.make_upload_candidate_stream(
          _conf->segment_upload_timeout());

        co_return ss::visit(
          candidate,
          [](std::monostate) -> find_reupload_candidate_result {
              vassert(
                false,
                "unexpected default re-upload candidate creation result");
          },
          [this, &run, &rw_fence, units = std::move(units)](
            segment_collector_stream& collector_stream) mutable
            -> find_reupload_candidate_result {
              if (
                collector_stream.start_offset != run->meta.base_offset
                || collector_stream.end_offset != run->meta.committed_offset) {
                  vlog(
                    _rtclog.error,
                    "Failed to make reupload candidate to match the run, "
                    "candidate: {} run: {}",
                    collector_stream,
                    run->meta);
                  return {};
              }
              if (collector_stream.size != run->meta.size_bytes) {
                  vlog(
                    _rtclog.debug,
                    "Failed to make reupload candidate due to size mismatch, "
                    "skip this range: expected size: {}, actual size: {}",
                    human::bytes(run->meta.size_bytes),
                    human::bytes(collector_stream.size));
                  return {.skip_to = collector_stream.end_offset};
              }
              return {
                .units = std::move(units),
                .upload_stream = std::move(collector_stream),
                .read_write_fence = rw_fence};
          },
          [this](
            skip_offset_range& skip_offsets) -> find_reupload_candidate_result {
              const auto log_level = log_level_for_error(skip_offsets.reason);
              vlogl(
                _rtclog,
                log_level,
                "Failed to make reupload candidate: {}",
                skip_offsets.reason);
              return {};
          },
          [this](
            candidate_creation_error& error) -> find_reupload_candidate_result {
              const auto log_level = log_level_for_error(error);
              vlogl(
                _rtclog,
                log_level,
                "Failed to make reupload candidate: {}",
                error);
              return {};
          });
    }
    // OTHERWISE WE'RE REUPLOADING REMOTE SEGMENTS
    // This is not currently supported, and is tricky to wire up to the
    // stream-based interface, so just elide all this code for now and return an
    // empty result
    co_return find_reupload_candidate_result{};
}

cloud_storage::segment_name ntp_archiver::segment_name_for_stream(
  const segment_collector_stream& strm,
  std::optional<model::term_id> archiver_term) {
    auto term = archiver_term.value_or(_start_term);
    auto meta = convert_segment_meta(strm, _parent, _rev, term);
    return cloud_storage::partition_manifest::generate_remote_segment_name(
      meta);
}

ss::future<bool> ntp_archiver::upload(
  find_reupload_candidate_result find_res,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    ss::gate::holder holder(_gate);
    if (!find_res.upload_stream.has_value() || !find_res.units.has_value()) {
        // The method shouldn't be called if this is the case
        co_return false;
    }
    auto units = std::move(find_res.units);
    if (find_res.upload_stream.value().size > 0) {
        co_return co_await do_upload_local(
          find_res.read_write_fence,
          std::move(find_res.upload_stream).value(),
          source_rtc);
    }
    // Currently, the uploading of remote segments is disabled and
    // the only reason why the list of locks is empty is truncation.
    // The log could be truncated right after we scanned the manifest to
    // find upload candidate. In this case we will get an empty candidate
    // which is not a failure so we shuld return 'true'.
    co_return true;
}

ss::future<bool> ntp_archiver::do_upload_local(
  archival_stm_fence fence,
  segment_collector_stream strm,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    if (!may_begin_uploads()) {
        co_return false;
    }
    if (!config::shard_local_cfg().cloud_storage_enable_segment_uploads()) {
        co_return false;
    }

    auto sname = segment_name_for_stream(strm);

    if (strm.is_compacted) {
        vlog(
          _rtclog.warn,
          "Upload of {} requested but sources are compacted",
          sname);
        co_return false;
    }

    if (strm.size == 0) {
        vlog(
          _rtclog.warn,
          "Upload of the {} requested but sources are empty",
          sname);
        co_return false;
    }

    auto meta = convert_segment_meta(strm, _parent, _rev, _start_term);
    auto [tx_ranges, tx_size] = co_await get_aborted_transactions(strm, sname);
    meta.metadata_size_hint = tx_size;
    vlog(
      _rtclog.debug,
      "Starting segment upload in the background, name: {}, meta: {}",
      sname,
      meta);

    auto upl_res = co_await upload_segment(
      std::move(strm), meta, std::move(tx_ranges));

    if (upl_res.result() != cloud_storage::upload_result::success) {
        vlog(
          _rtclog.warn,
          "Failed to upload segment: {}, error: {}",
          sname,
          upl_res.result());
        co_return false;
    }

    const bool checks_disabled
      = config::shard_local_cfg()
          .cloud_storage_disable_upload_consistency_checks.value();

    if (!checks_disabled && upl_res.has_record_stats()) {
        auto stats = upl_res.record_stats();
        // Validate segment content. The 'stats' is computed when
        // the actual segment is scanned and represents the 'ground truth' about
        // its content. The 'meta' is the expected segment metadata. We
        // shouldn't replicate it if it doesn't match the 'stats'.
        if (!segment_meta_matches_stats(
              meta,
              stats,
              _rtclog,
              _parent.get_ntp_config().is_remote_allow_gaps_enabled())) {
            co_return false;
        }
    }
    if (!checks_disabled) {
        // Validate metadata using the STM state
        if (!manifest().safe_segment_meta_to_add(meta)) {
            co_return false;
        }
    }

    auto highest_producer_id
      = _feature_table.local().is_active(
          features::feature::cloud_metadata_cluster_recovery)
          ? _parent.highest_producer_id()
          : model::producer_id{};
    auto deadline = ss::lowres_clock::now() + _conf->manifest_upload_timeout();

    auto is_validated = checks_disabled ? cluster::segment_validated::no
                                        : cluster::segment_validated::yes;
    cluster::emit_read_write_fence rw_fence = std::nullopt;
    if (fence.emit_rw_fence_cmd) {
        vlog(
          archival_log.debug,
          "(2) fence value is: {}, manifest last applied "
          "offset: {}, manifest in-sync offset: {}",
          fence.read_write_fence,
          _parent.archival_meta_stm()->manifest().get_applied_offset(),
          _parent.archival_meta_stm()->get_insync_offset());
        rw_fence = fence.read_write_fence;
    }

    auto error = co_await _parent.archival_meta_stm()->add_segments(
      {meta},
      std::nullopt,
      highest_producer_id,
      deadline,
      _as,
      is_validated,
      rw_fence);

    if (error != cluster::errc::success && error != cluster::errc::not_leader) {
        vlog(
          _rtclog.warn,
          "archival metadata STM update failed: {}",
          error.message());
        co_return false;
    }

    if (
      co_await upload_manifest(segment_merger_ctx_label, source_rtc)
      != cloud_storage::upload_result::success) {
        vlog(
          _rtclog.info,
          "archival metadata replicated but manifest is not re-uploaded");
    } else {
        // Write to archival_metadata_stm to mark our updated clean offset
        // as a result of uploading the manifest successfully.
        co_await flush_manifest_clean_offset();
    }
    co_return true;
}

ss::future<bool> ntp_archiver::do_upload_remote(
  upload_candidate_with_locks candidate,
  std::optional<std::reference_wrapper<retry_chain_node>> source_rtc) {
    std::ignore = candidate;
    std::ignore = source_rtc;
    throw std::runtime_error("Not implemented");
}

size_t ntp_archiver::get_local_segment_size() const {
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(
      *_parent.raft()->log());

    return disk_log.max_segment_size();
}

ss::future<bool>
ntp_archiver::prepare_transfer_leadership(ss::lowres_clock::duration timeout) {
    _paused = true;

    ss::gate::holder holder(_gate);
    try {
        auto units = co_await _uploads_active.get_units(timeout);
        vlog(
          _rtclog.trace,
          "prepare_transfer_leadership: got units (current {})",
          _uploads_active.has_units());
    } catch (const ss::semaphore_timed_out&) {
        // In this situation, it is possible that the old leader (this node)
        // will leave an orphan object behind in object storage, because
        // the next manifest written by the new leader will not refer to
        // this object.
        //
        // This is not a correctness issue, but consumes some disk space,
        // and these objects may also be left behind when the topic is later
        // deleted.
        co_return false;
    }

    // Attempt to flush our clean offset, to avoid the new leader redundantly
    // uploading a copy of the manifest based on a stale clean offset in
    // the stm.  This is an optimization: if it fails then the leader transfer
    // will still proceed smoothly, there just may be an extra manifest upload
    // on the new leader.
    co_await flush_manifest_clean_offset();

    co_return true;
}

const storage::ntp_config& ntp_archiver::ntp_config() const {
    return _parent.log()->config();
}

const cloud_storage::remote_path_provider&
ntp_archiver::remote_path_provider() const {
    return _parent.archival_meta_stm()->path_provider();
}

void ntp_archiver::complete_transfer_leadership() {
    vlog(
      _rtclog.trace,
      "complete_transfer_leadership: current units (current {})",
      _uploads_active.has_units());
    _paused = false;
    _leader_cond.signal();
}

bool ntp_archiver::lost_leadership() const {
    return !_parent.is_leader() || _parent.term() != _start_term;
}

bool ntp_archiver::local_storage_pressure() const {
    auto eviction_offset = _parent.eviction_requested_offset();

    return eviction_offset.has_value()
           && _parent.archival_meta_stm()->get_last_clean_at()
                <= eviction_offset.value();
}

bool ntp_archiver::flush_in_progress() const {
    return _flush_uploads_offset.has_value();
}

bool ntp_archiver::uploaded_and_clean_past_offset(model::offset o) const {
    vlog(
      _rtclog.trace,
      "In uploaded_and_clean_past_offset(), manifest last "
      "offset: {}, last clean offset: {}, query offset: {}.",
      manifest().get_last_offset(),
      _parent.archival_meta_stm()->get_last_clean_at(),
      o);
    return std::min(
             manifest().get_last_offset(),
             _parent.archival_meta_stm()->get_last_clean_at())
           >= o;
}

bool ntp_archiver::uploaded_data_past_flush_offset() const {
    return flush_in_progress()
           && manifest().get_last_offset() >= _flush_uploads_offset.value();
}

void ntp_archiver::initialize_probe() {
    _probe.emplace(
      _conf->ntp_metrics_disabled, _ntp, _parent.archival_meta_stm());
}

} // namespace archival
