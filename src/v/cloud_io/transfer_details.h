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

#include "cloud_storage_clients/client.h"
#include "utils/log_hist.h"
#include "utils/retry_chain_node.h"

#include <seastar/util/noncopyable_function.hh>

namespace cloud_io {

using probe_callback_t = ss::noncopyable_function<void()>;
// Callback to run before sending the next request (either as a retry or for
// the first time). The input is the attempt number of the next request, such
// that the first request will this method with 1.
using on_req_callback_t = ss::noncopyable_function<void(size_t)>;
using size_callback_t = ss::noncopyable_function<void(size_t)>;

using latency_measurement_t = std::unique_ptr<log_hist_internal::measurement>;
using latency_callback_t = ss::noncopyable_function<latency_measurement_t()>;

template<class Clock>
struct basic_transfer_details {
    cloud_storage_clients::bucket_name bucket;
    cloud_storage_clients::object_key key;

    basic_retry_chain_node<Clock>& parent_rtc;

    std::optional<probe_callback_t> success_cb{std::nullopt};
    std::optional<size_callback_t> success_size_cb{std::nullopt};

    std::optional<probe_callback_t> failure_cb{std::nullopt};
    std::optional<probe_callback_t> backoff_cb{std::nullopt};
    std::optional<probe_callback_t> client_acquire_cb{std::nullopt};

    std::optional<on_req_callback_t> on_req_cb{std::nullopt};

    std::optional<latency_callback_t> measure_latency_cb{std::nullopt};

    void on_success();
    void on_success_size(size_t size_bytes);
    void on_failure();
    void on_backoff();
    void on_client_acquire();
    void on_request(size_t attempt_num);
    latency_measurement_t scoped_latency_measurement();
};

using transfer_details = basic_transfer_details<ss::lowres_clock>;

} // namespace cloud_io
