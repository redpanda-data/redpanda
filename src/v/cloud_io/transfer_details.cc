/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/transfer_details.h"

#include <seastar/core/manual_clock.hh>

namespace {
void run_callback(std::optional<cloud_io::probe_callback_t>& cb) {
    if (cb.has_value()) {
        cb.value()();
    }
}
} // namespace

namespace cloud_io {

template<class Clock>
void basic_transfer_details<Clock>::on_success() {
    run_callback(success_cb);
}
template<class Clock>
void basic_transfer_details<Clock>::on_success_size(size_t size_bytes) {
    on_success();
    if (success_size_cb.has_value()) {
        success_size_cb.value()(size_bytes);
    }
}
template<class Clock>
void basic_transfer_details<Clock>::on_failure() {
    run_callback(failure_cb);
}
template<class Clock>
void basic_transfer_details<Clock>::on_backoff() {
    run_callback(backoff_cb);
}
template<class Clock>
void basic_transfer_details<Clock>::on_client_acquire() {
    run_callback(client_acquire_cb);
}
template<class Clock>
void basic_transfer_details<Clock>::on_request(size_t attempt_num) {
    if (on_req_cb.has_value()) {
        on_req_cb.value()(attempt_num);
    }
}
template<class Clock>
latency_measurement_t
basic_transfer_details<Clock>::scoped_latency_measurement() {
    if (measure_latency_cb.has_value()) {
        return measure_latency_cb.value()();
    }
    return nullptr;
}

template struct basic_transfer_details<ss::lowres_clock>;
template struct basic_transfer_details<ss::manual_clock>;

} // namespace cloud_io
