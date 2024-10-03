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
#include "cloud_topics/batcher/serializer.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "ssx/sformat.h"
#include "utils/human.h"

#include <seastar/core/condition-variable.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace experimental::cloud_topics {

template<class Clock>
batcher<Clock>::batcher(
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<Clock>& remote)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _upload_timeout(
      config::shard_local_cfg().cloud_storage_segment_upload_timeout_ms.bind())
  , _upload_interval(config::shard_local_cfg()
                       .cloud_storage_upload_loop_initial_backoff_ms
                       .bind()) // TODO: use different config
  , _rtc(_as)
  , _logger(cd_log, _rtc) {}

template<class Clock>
ss::future<> batcher<Clock>::start() {
    ssx::spawn_with_gate(_gate, [this] { return bg_controller_loop(); });
    return ss::now();
}

template<class Clock>
ss::future<> batcher<Clock>::stop() {
    _as.request_abort();
    co_await _gate.close();
}

template<class Clock>
typename batcher<Clock>::size_limited_write_req_list
batcher<Clock>::get_write_requests(size_t max_bytes) {
    vlog(
      _logger.debug,
      "get_write_requests called with max_bytes = {}",
      max_bytes);
    size_limited_write_req_list result;
    size_t acc_size = 0;
    // The elements in the list are in the insertion order.
    auto it = _pending.begin();
    for (; it != _pending.end(); it++) {
        auto sz = it->data_chunk.payload.size_bytes();
        acc_size += sz;
        if (acc_size >= max_bytes) {
            // Include last element
            it++;
            break;
        }
    }
    result.ready.splice(result.ready.end(), _pending, _pending.begin(), it);
    result.size_bytes = acc_size;
    result.complete = _pending.empty();
    vlog(
      _logger.debug,
      "get_write_requests returned {} elements, containing {}",
      result.ready.size(),
      human::bytes(result.size_bytes));
    return result;
}

template<class Clock>
void batcher<Clock>::remove_timed_out_write_requests() {
    chunked_vector<ss::weak_ptr<details::write_request<Clock>>> expired;
    for (auto& wr : _pending) {
        if (wr.has_expired()) {
            expired.push_back(wr.weak_from_this());
        }
    }
    if (!expired.empty()) {
        vlog(_logger.debug, "{} write requests have expired", expired.size());
    }
    for (auto& wp : expired) {
        if (wp != nullptr) {
            wp->_hook.unlink();
            wp->set_value(errc::timeout);
        }
    }
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
ss::future<errc> batcher<Clock>::wait_for_next_upload() noexcept {
    try {
        co_await _cv.wait<Clock>(Clock::now() + _upload_interval(), [this] {
            return _current_size >= 10_MiB; // TODO: use configuration parameter
        });
    } catch (const ss::broken_condition_variable&) {
        co_return errc::shutting_down;
    } catch (const ss::condition_variable_timed_out&) {
    }
    co_return errc::success;
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

        remove_timed_out_write_requests();

        auto list = get_write_requests(
          10_MiB); // TODO: use configuration parameter

        if (list.ready.empty()) {
            co_return true;
        }

        details::aggregator<Clock> aggregator;
        while (!list.ready.empty()) {
            auto& wr = list.ready.back();
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
            auto wait_res = co_await wait_for_next_upload();
            if (wait_res != errc::success) {
                // Shutting down
                vlog(
                  _logger.info,
                  "Batcher upload loop is shutting down {}",
                  wait_res);
                co_return;
            }
        }
        // TODO: implement rate limiting
        // take units from upload rate tb here and from failure tb
        // in case of failure
        auto res = co_await run_once();
        if (res.has_error()) {
            if (res.error() == errc::shutting_down) {
                vlog(_logger.info, "Batcher upload loop is shutting down");
                co_return;
            } else {
                // Some other (most likely upload) error
                vlog(
                  _logger.info, "Batcher upload loop error: {}", res.error());
                // TODO: implement throttling/circuit breaking.
                // The batcher should use two token buckets:
                // - upload bucket should limit request rate and the limit
                // should be higher
                //   than configured upload frequency (to account for the case
                //   when more than one L0 object is upload on every iteration)
                // - failure bucket should limit failure rate. It should be set
                // up to allow
                //   lower frequency then the expected upload rate.
                // So basically in case of error we should retry immediately but
                // eventually we should be throttled.
                more_work = true;
            }
        }
    }
}

template<class Clock>
ss::future<result<model::record_batch_reader>>
batcher<Clock>::write_and_debounce(
  model::ntp ntp,
  model::record_batch_reader r,
  std::chrono::milliseconds timeout) {
    auto h = _gate.hold();
    auto index = _index++;
    auto layout = maybe_get_data_layout(r);
    if (!layout.has_value()) {
        // We expect to get in-memory record batch reader here so
        // we will be able to estimate the size.
        co_return errc::timeout;
    }
    // The write request is stored on the stack of the
    // fiber until the 'response' promise is set. The
    // promise can be set by any fiber that uploaded the
    // data from the write request.
    auto data_chunk = co_await details::serialize_in_memory_record_batch_reader(
      std::move(r));
    _current_size += data_chunk.payload.size_bytes();
    details::write_request<Clock> request(
      std::move(ntp), index, std::move(data_chunk), timeout);
    auto fut = request.response.get_future();
    _pending.push_back(request);
    if (_current_size > 10_MiB) { // NOLINT
        _cv.signal();
    }
    // TODO: The MT-version of this could be implemented as an external
    // load balancer that submits request to chosen shard
    // and awaits the result. This method will be an entry point into the
    // load balancer.
    auto res = co_await std::move(fut);
    if (res.has_error()) {
        co_return res.error();
    }
    // At this point the request is no longer referenced
    // by any other shard
    auto rdr = model::make_memory_record_batch_reader(std::move(res.value()));
    co_return std::move(rdr);
}

template class batcher<ss::lowres_clock>;
template class batcher<ss::manual_clock>;

} // namespace experimental::cloud_topics
