/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "kafka/protocol/types.h"
#include "kafka/protocol/wire.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace kafka {

using flex_enabled = ss::bool_class<struct flex_response_tag>;

class response {
public:
    explicit response(flex_enabled flex) noexcept
      : _flex(flex)
      , _tags(
          flex ? std::optional<tagged_fields>(tagged_fields{}) : std::nullopt)
      , _writer(_buf) {}

    protocol::encoder& writer() { return _writer; }

    const iobuf& buf() const { return _buf; }
    iobuf& buf() { return _buf; }
    iobuf release() && { return std::move(_buf); }

    correlation_id correlation() const { return _correlation; }
    void set_correlation(correlation_id c) { _correlation = c; }

    flex_enabled is_flexible() const { return _flex; }

    const std::optional<tagged_fields>& tags() const { return _tags; }
    std::optional<tagged_fields>& tags() { return _tags; }

    /*
     * Marking a response as a noop means that it will be processed like any
     * other response (e.g. quota accounting) but the response won't written to
     * the connection. This is used by kafka producer with acks=0 in which the
     * client does not expect the broker to respond.
     */
    bool is_noop() const { return _noop; }
    void mark_noop() { _noop = true; }

private:
    bool _noop{false};
    correlation_id _correlation;
    flex_enabled _flex{flex_enabled::no};
    std::optional<tagged_fields> _tags;
    iobuf _buf;
    protocol::encoder _writer;
};

using response_ptr = ss::foreign_ptr<std::unique_ptr<response>>;

struct process_result_stages {
    process_result_stages(
      ss::future<> dispatched_f, ss::future<response_ptr> response_f)
      : dispatched(std::move(dispatched_f))
      , response(std::move(response_f)) {}

    explicit process_result_stages(response_ptr response)
      : dispatched(ss::now())
      , response(ss::make_ready_future<response_ptr>(std::move(response))) {}

    /**
     * Single stage method is a helper to execute whole request in foreground.
     * The dispatch phase if finished after response phase this way when
     * response is processed in background it is already resolved future.
     */
    static process_result_stages single_stage(ss::future<response_ptr> f) {
        ss::promise<response_ptr> response;
        auto response_f = response.get_future();
        auto dispatch = f.then_wrapped(
          [response = std::move(response)](ss::future<response_ptr> f) mutable {
              try {
                  auto r = f.get();
                  response.set_value(std::move(r));
              } catch (...) {
                  response.set_exception(std::current_exception());
              }
          });

        return process_result_stages(
          std::move(dispatch), std::move(response_f));
    }

    // after this future resolved request is dispatched for processing and
    // processing order is set
    ss::future<> dispatched;
    // the response future is intended to be executed in background
    ss::future<response_ptr> response;
};

/**
 * @brief The default memory size estimate.
 *
 * Request must make an up-front estimate of the amount of memory they will use,
 * in order to obtain the corresponding number of units from the memory
 * semaphore (blocking if they are not available). Each request type can use
 * their own estimation approach, but if not specified this default estimator
 * will be used.
 *
 * Now, this estimator is very poor for many request types: it only applies a
 * multiplier to the request size, so only makes
 * sense for requests (such as produce) where the size of the request is a
 * good indicator of the total memory size. For requests with a small request
 * but a large response (fetch, metadata, etc), it is not appropriate.
 *
 * @return size_t the estimated size required to process the request
 */
constexpr size_t default_memory_estimate(size_t request_size) {
    // Allow for extra copies and bookkeeping
    return request_size * 2 + 8000; // NOLINT
}

} // namespace kafka
