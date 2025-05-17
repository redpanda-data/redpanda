/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/batcher.h"

#include "base/unreachable.h"
#include "cloud_io/remote.h"
#include "cloud_topics/batcher/aggregator.h"
#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/core/serializer.h"
#include "cloud_topics/core/write_request.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "ssx/sformat.h"
#include "utils/human.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace experimental::cloud_topics {

template<class Clock>
batcher<Clock>::batcher(
  core::write_pipeline<Clock>::stage stage,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<Clock>& remote_api)
  : _remote(remote_api)
  , _bucket(std::move(bucket))
  , _upload_timeout(
      config::shard_local_cfg().cloud_storage_segment_upload_timeout_ms.bind())
  , _upload_interval(config::shard_local_cfg()
                       .cloud_storage_upload_loop_initial_backoff_ms
                       .bind()) // TODO: use different config
  , _rtc(_as)
  , _logger(cd_log, _rtc)
  , _stage(std::move(stage)) {}

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
ss::future<result<size_t>>
batcher<Clock>::upload_object(object_id id, iobuf payload) {
    auto content_length = payload.size_bytes();
    vlog(
      _logger.trace,
      "upload_object is called, upload size: {}",
      human::bytes(content_length));

    // TODO: this should be replaced with the proper name
    auto name = ssx::sformat("{}", id);

    auto err = errc::success;
    try {
        // Clock type is not parametrized further down the call chain.
        basic_retry_chain_node<Clock> local_rtc(
          Clock::now() + _upload_timeout(),
          // Backoff doesn't matter, the operation never retries
          100ms,
          retry_strategy::disallow,
          &_rtc);

        auto path = cloud_storage_clients::object_key(name);

        cloud_io::basic_transfer_details<Clock> td{
          .bucket = _bucket,
          .key = path,
          .parent_rtc = local_rtc,
        };

        auto upl_result = co_await _remote.upload_object({
          .transfer_details = std::move(td),
          .display_str = "L0_object",
          .payload = std::move(payload),
        });

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
            err = errc::shutting_down;
        } else {
            vlog(_logger.error, "Unexpected L0 upload error {}", e);
            err = errc::unexpected_failure;
        }
    }

    if (err != errc::success) {
        vlog(_logger.error, "L0 upload error: {}", err);
        co_return err;
    }

    co_return content_length;
    ;
}

template<class Clock>
ss::future<result<bool>> batcher<Clock>::run_once() noexcept {
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

        auto list = _stage.pull_write_requests(
          10_MiB); // TODO: use configuration parameter

        if (list.requests.empty()) {
            vlog(_logger.debug, "No write requests to process");
            co_return true;
        }

        aggregator<Clock> aggregator;
        while (!list.requests.empty()) {
            auto& wr = list.requests.back();
            wr._hook.unlink();
            aggregator.add(wr);
        }
        // TODO: skip waiting if list.completed is not true
        auto payload = aggregator.prepare();
        auto result = co_await upload_object(
          aggregator.get_object_id(), std::move(payload));
        if (result.has_error()) {
            // TODO: fix the error
            // NOTE: it should be possible to translate the
            // error to kafka error at the call site but I
            // don't want to depend on kafka layer directly.
            // Timeout should work well at this point.
            aggregator.ack_error(errc::timeout);
            co_return result.error();
        }
        aggregator.ack();
        co_return list.complete;
    } catch (...) {
        auto err = std::current_exception();
        if (ssx::is_shutdown_exception(err)) {
            vlog(_logger.debug, "Batcher shutdown error: {}", err);
            co_return errc::shutting_down;
        }
        vlog(_logger.error, "Unexpected batcher error: {}", err);
        co_return errc::unexpected_failure;
    }
    unreachable();
}

template<class Clock>
ss::future<> batcher<Clock>::bg_controller_loop() {
    auto h = _gate.hold();
    bool more_work = false;
    while (!_as.abort_requested()) {
        if (!more_work) {
            auto wait_res = co_await _stage.wait_until(
              10_MiB, Clock::now() + _upload_interval(), &_as);
            if (wait_res.has_error()) {
                // Shutting down
                vlog(
                  _logger.info,
                  "Batcher upload loop is shutting down {}",
                  wait_res.error());
                co_return;
            }
        }
        if (_as.abort_requested()) {
            vlog(_logger.info, "Batcher upload loop is shutting down");
            co_return;
        }
        auto res = co_await run_once();
        if (res.has_error()) {
            if (res.error() == errc::shutting_down) {
                vlog(_logger.info, "Batcher upload loop is shutting down");
                co_return;
            } else {
                // Some other (most likely upload) error
                vlog(
                  _logger.info, "Batcher upload loop error: {}", res.error());
                more_work = true;
            }
        } else {
            more_work = !res.value();
        }
    }
}

template class batcher<ss::lowres_clock>;
template class batcher<ss::manual_clock>;

} // namespace experimental::cloud_topics
