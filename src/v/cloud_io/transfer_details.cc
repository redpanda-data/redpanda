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

namespace {
void run_callback(std::optional<cloud_io::probe_callback_t>& cb) {
    if (cb.has_value()) {
        cb.value()();
    }
}
} // namespace

namespace cloud_io {

void transfer_details::on_success() { run_callback(success_cb); }
void transfer_details::on_success_size(size_t size_bytes) {
    on_success();
    if (success_size_cb.has_value()) {
        success_size_cb.value()(size_bytes);
    }
}
void transfer_details::on_failure() { run_callback(failure_cb); }
void transfer_details::on_backoff() { run_callback(backoff_cb); }
void transfer_details::on_client_acquire() { run_callback(client_acquire_cb); }
void transfer_details::on_request(size_t attempt_num) {
    if (on_req_cb.has_value()) {
        on_req_cb.value()(attempt_num);
    }
}
latency_measurement_t transfer_details::scoped_latency_measurement() {
    if (measure_latency_cb.has_value()) {
        return measure_latency_cb.value()();
    }
    return nullptr;
}

} // namespace cloud_io
