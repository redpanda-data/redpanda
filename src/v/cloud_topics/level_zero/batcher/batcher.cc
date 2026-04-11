/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/batcher/batcher.h"

#include "cloud_io/remote.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/batcher/aggregator.h"
#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "ssx/sformat.h"
#include "utils/human.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>
#include <limits>
#include <variant>

using namespace std::chrono_literals;

namespace cloud_topics::l0 {

template<class Clock>
batcher<Clock>::batcher(
  write_pipeline<Clock>::stage stage,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<Clock>& remote_api,
  cloud_topics::cluster_services* cluster_services)
  : _cluster_services(cluster_services)
  , _remote(remote_api)
  , _bucket(std::move(bucket))
  , _upload_timeout(
      config::shard_local_cfg().cloud_storage_segment_upload_timeout_ms.bind())
  , _upload_backoff_interval(
      config::shard_local_cfg()
        .cloud_storage_upload_loop_initial_backoff_ms.bind())
  , _rtc(_as)
  , _logger(cd_log, _rtc)
  , _stage(std::move(stage))
  , _probe(config::shard_local_cfg().disable_metrics())
  , _upload_sem(
      config::shard_local_cfg().cloud_storage_max_connections(), "l0/batcher") {
}

template<class Clock>
ss::future<> batcher<Clock>::start() {
    vlog(cd_log.debug, "Batcher start");
    ssx::spawn_with_gate(_gate, [this] { return bg_controller_loop(); });
    return ss::now();
}

template<class Clock>
ss::future<> batcher<Clock>::stop() {
    vlog(cd_log.debug, "Batcher stop");
    _as.request_abort();
    co_await _gate.close();
}

template<class Clock>
ss::future<std::expected<size_t, errc>>
batcher<Clock>::upload_object(object_id id, iobuf payload) {
    auto content_length = payload.size_bytes();
    vlog(
      _logger.trace,
      "upload_object is called, upload size: {} ({} bytes)",
      human::bytes(content_length),
      content_length);

    auto err = errc::success;
    try {
        // Clock type is not parametrized further down the call chain.
        basic_retry_chain_node<Clock> local_rtc(
          Clock::now() + _upload_timeout(),
          _upload_backoff_interval(),
          retry_strategy::backoff,
          &_rtc);

        auto path = object_path_factory::level_zero_path(id);

        micro_probe probe;

        cloud_io::basic_transfer_details<Clock> td{
          .bucket = _bucket,
          .key = path,
          .parent_rtc = local_rtc,
          .success_cb =
            [&probe, sz = payload.size_bytes()] {
                probe.num_cloud_writes++;
                probe.cloud_write_bytes += sz;
            },
          .backoff_cb = [&probe] { probe.num_cloud_writes++; }};

        auto upl_result = co_await _remote.upload_object({
          .transfer_details = std::move(td),
          .display_str = "L0_object",
          .payload = std::move(payload),
        });

        _stage.register_micro_probe(probe);

        switch (upl_result) {
        case cloud_io::upload_result::success:
            break;
        case cloud_io::upload_result::cancelled:
            err = errc::shutting_down;
            break;
        case cloud_io::upload_result::timedout:
            err = errc::timeout;
            break;
        case cloud_io::upload_result::failed:
            err = errc::upload_failure;
        }
    } catch (...) {
        auto e = std::current_exception();
        if (ssx::is_shutdown_exception(e)) {
            co_return std::unexpected{errc::shutting_down};
        } else {
            vlog(_logger.error, "Unexpected L0 upload error {}", e);
            // Return early to prevent the double logging below
            co_return std::unexpected{errc::unexpected_failure};
        }
    }

    if (err != errc::success) {
        vlog(_logger.warn, "L0 upload error: {}", err);
        co_return std::unexpected{err};
    }

    co_return content_length;
}

template<class Clock>
ss::future<std::expected<std::monostate, errc>> batcher<Clock>::run_once(
  write_pipeline<Clock>::write_requests_list list) noexcept {
    try {
        // NOTE: the main workflow looks like this:
        // - remove expired write requests
        // - collect write requests which can be aggregated/uploaded as L0
        //   object
        // - create 'aggregator' and fill it with write requests (the
        //   requests which are added to the aggregator shouldn't be removed
        //   from _pending list)
        // - the 'aggregator' is used to generate L0 object and upload it
        // - the 'aggregator' is used to acknowledge (either success or
        //   failure) all aggregated write requests
        //
        // The invariants here are:
        // 1. expired write requests shouldn't be added to the 'aggregator'
        // 2. if the request is added to the 'aggregator' its promise
        //    shouldn't be set
        //
        // The first invariant is enforced by calling
        // 'remote_timed_out_write_requests' in the same time slice as
        // collecting the write requests. The second invariant is enforced
        // by the strict order in which the ack() method is called
        // explicitly after the operation is either committed or failed.

        if (list.requests.empty()) {
            vlog(_logger.trace, "No write requests to process");
            co_return std::monostate{};
        }

        auto epoch_fut = co_await ss::coroutine::as_future<cluster_epoch>(
          _cluster_services->current_epoch(&_as));

        if (epoch_fut.failed()) {
            auto ex = epoch_fut.get_exception();
            vlog(_logger.warn, "Failed to get cluster epoch: {}", ex);
            while (!list.requests.empty()) {
                auto& wr = list.requests.back();
                wr.set_value(errc::failed_to_get_epoch);
                list.requests.pop_back();
            }
            _probe.register_epoch_error();
            co_return std::unexpected(errc::failed_to_get_epoch);
        }

        aggregator<Clock> aggregator;
        while (!list.requests.empty()) {
            auto& wr = list.requests.back();
            wr._hook.unlink();
            aggregator.add(wr);
        }

        /*
         * L0 object is named using an epoch calculated as:
         *
         *    start_epoch = max(p.highest_topic_start_epoch() for all p in L0)
         *    epoch = max(start_epoch, cached_cluster_epoch)
         */
        auto object_epoch = std::max(
          aggregator.highest_topic_start_epoch(), epoch_fut.get());

        // TODO: skip waiting if list.completed is not true
        auto object = aggregator.prepare(object_id::create(object_epoch));
        auto size_bytes = object.payload.size_bytes();
        auto result = co_await upload_object(
          object.id, std::move(object.payload));
        if (!result) {
            // TODO: fix the error
            // NOTE: it should be possible to translate the
            // error to kafka error at the call site but I
            // don't want to depend on kafka layer directly.
            // Timeout should work well at this point.
            aggregator.ack_error(errc::timeout);
            _probe.register_error();
            co_return std::unexpected{result.error()};
        }
        aggregator.ack();
        _probe.register_upload(size_bytes);
        co_return std::monostate{};
    } catch (...) {
        auto err = std::current_exception();
        if (ssx::is_shutdown_exception(err)) {
            vlog(_logger.debug, "Batcher shutdown error: {}", err);
            co_return std::unexpected{errc::shutting_down};
        }
        vlog(_logger.error, "Unexpected batcher error: {}", err);
        co_return std::unexpected{errc::unexpected_failure};
    }
}

template<class Clock>
ss::future<> batcher<Clock>::bg_controller_loop() {
    auto h = _gate.hold();
    while (!_as.abort_requested()) {
        auto wait_res = co_await _stage.wait_next(&_as);
        if (!wait_res.has_value()) {
            vlog(
              _logger.info,
              "Batcher upload loop is shutting down {}",
              wait_res.error());
            co_return;
        }
        if (_as.abort_requested()) {
            vlog(_logger.info, "Batcher upload loop is shutting down");
            co_return;
        }

        // Pull all available write requests at once.
        auto all = _stage.pull_write_requests(
          std::numeric_limits<size_t>::max(),
          std::numeric_limits<size_t>::max());

        if (all.requests.empty()) {
            continue;
        }

        // Calculate total size and split into evenly-sized chunks,
        // each close to the configured threshold.
        size_t total_size = 0;
        for (const auto& wr : all.requests) {
            total_size += wr.size_bytes();
        }

        auto threshold = config::shard_local_cfg()
                           .cloud_topics_produce_batching_size_threshold();

        // If we will allow every chunk to be 'threshold' size
        // then the last chunk in the list has an opportunity to be
        // much smaller than the rest. Consider a case when the threshold
        // is 4MiB and we got 8MiB + 1KiB in one iteration. If we will
        // upload two 4MiB objects we will have to upload 1KiB object next.
        // The solution is to allow upload size to deviate more if it allows
        // us to avoid overly small objects (which will impact TCO).
        //
        // The computation below can yield small target_chunk_size in case
        // if total_size is below the threshold. If the total_size exceeds
        // the threshold the target_chunk_size can either overshoot the
        // threshold or undershoot. In both cases the error is bounded by
        // about 50%. The largest error is an overshoot in case if
        // the total_size is 6MiB - 1 byte and the threshold is 4MiB. The
        // num_chunks in this case is 1 and the target_chunk_size is approx.
        // 6MiB which is 2MiB over the threshold (50% of 4MiB threshold).
        // If the total_size is 6MiB the num_chunks will be 2 and the
        // target_chunk_size will be 3MiB which is 1MiB below the threshold
        // (or 25% of 4MiB).
        size_t num_chunks = std::max(
          size_t{1}, (total_size + threshold / 2) / threshold);
        size_t target_chunk_size = total_size / num_chunks;

        vlog(
          _logger.trace,
          "Splitting {} ({} bytes) into {} chunks, target chunk size: {} "
          "({})",
          human::bytes(total_size),
          total_size,
          num_chunks,
          human::bytes(target_chunk_size),
          target_chunk_size);

        for (size_t i = 0; i < num_chunks && !all.requests.empty(); ++i) {
            auto units_fut = co_await ss::coroutine::as_future(
              ss::get_units(_upload_sem, 1, _as));

            if (units_fut.failed()) {
                auto ex = units_fut.get_exception();
                vlog(
                  _logger.info, "Batcher upload loop is shutting down: {}", ex);
                co_return;
            }
            auto units = std::move(units_fut.get());

            // Build a chunk by moving requests from the pulled list
            // until we reach the target size (unless it's the last
            // chunk, upload everything in this case).
            typename write_pipeline<Clock>::write_requests_list chunk(
              all._parent, all._ps);

            size_t chunk_size = 0;
            bool is_last = (i == num_chunks - 1);
            while (!all.requests.empty()) {
                auto& wr = all.requests.front();
                auto sz = wr.size_bytes();
                chunk_size += sz;
                wr._hook.unlink();
                chunk.requests.push_back(wr);
                // Allow the last chunk to be larger than the target
                // to avoid small objects.
                if (!is_last && chunk_size >= target_chunk_size) {
                    break;
                }
            }

            ssx::spawn_with_gate(
              _gate,
              [this,
               chunk = std::move(chunk),
               units = std::move(units)]() mutable {
                  return run_once(std::move(chunk))
                    .then([this](std::expected<std::monostate, errc> res) {
                        if (!res.has_value()) {
                            if (res.error() == errc::shutting_down) {
                                vlog(
                                  _logger.info,
                                  "Batcher upload loop is shutting down");
                            } else {
                                vlog(
                                  _logger.info,
                                  "Batcher upload loop error: {}",
                                  res.error());
                            }
                        }
                    })
                    .finally([u = std::move(units)] {});
              });
        }
    }
}

template class batcher<ss::lowres_clock>;
template class batcher<ss::manual_clock>;

} // namespace cloud_topics::l0
