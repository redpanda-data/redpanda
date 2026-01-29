/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/replication/tests/deps_test_impl.h"

#include "kafka/protocol/errors.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace cluster_link::replication::tests {

ss::future<kafka::offset>
test_config_provider::start_offset(const ::model::ntp&, ss::abort_source&) {
    co_return kafka::offset{0};
}

ss::future<> accounting_sink::start() { return ss::now(); }

ss::future<> accounting_sink::reset() {
    _records_consumed = 0;
    _last_offset = kafka::offset{0};
    _start_offset = {};
    return ss::now();
}

ss::future<> accounting_sink::stop() noexcept { return _gate.close(); };

kafka::offset accounting_sink::last_replicated_offset() const {
    return _last_offset;
}

raft::replicate_stages accounting_sink::replicate(
  chunked_vector<model::record_batch> batches,
  model::timeout_clock::duration,
  ss::abort_source& as) {
    auto holder = _gate.hold();
    for (const auto& batch : batches) {
        _records_consumed += batch.header().record_count;
    }
    auto sleep_duration = std::chrono::milliseconds{
      random_generators::get_int(1, 3)};
    _last_offset = kafka::offset(
      batches.back().base_offset()()
      + batches.back().header().last_offset_delta);
    auto replicate_result = result<raft::replicate_result>(
      raft::replicate_result{kafka::offset_cast(_last_offset)});
    return raft::replicate_stages{
      ss::now(),
      ss::sleep_abortable(sleep_duration, as)
        .then([replicate_result] { return replicate_result; })
        .finally([holder = std::move(holder)] {})};
}

void accounting_sink::notify_replicator_failure(model::term_id) {}

kafka::offset accounting_sink::high_watermark() const { return _last_offset; }

bool accounting_sink::can_prefix_truncate() const { return true; }

ss::future<kafka::error_code> accounting_sink::prefix_truncate(
  kafka::offset truncation_offset, ss::lowres_clock::time_point) {
    if (truncation_offset <= start_offset()) {
        co_return kafka::error_code::none;
    }

    if (truncation_offset > high_watermark()) {
        co_return kafka::error_code::offset_out_of_range;
    }

    _start_offset = truncation_offset;

    co_return kafka::error_code::none;
}

kafka::offset accounting_sink::start_offset() { return _start_offset; }

ss::future<> random_data_source::start(kafka::offset offset) noexcept {
    _next = offset;
    return ss::now();
}

ss::future<> random_data_source::stop() noexcept { return _gate.close(); }
ss::future<> random_data_source::reset(kafka::offset offset) {
    _next = offset;
    return ss::now();
}
ss::future<fetch_data> random_data_source::fetch_next(ss::abort_source& as) {
    auto holder = _gate.hold();
    auto batch = ::model::test::make_random_batch(
      kafka::offset_cast(_next), 5, true, model::record_batch_type::raft_data);
    chunked_vector<model::record_batch> batches;
    batches.push_back(std::move(batch));
    co_await ss::sleep_abortable(
      std::chrono::milliseconds{random_generators::get_int(1, 3)}, as);
    co_return fetch_data{std::move(batches), ssx::semaphore_units{}};
}

std::optional<data_source::source_partition_offsets_report>
random_data_source::get_offsets() {
    return std::nullopt;
}

std::unique_ptr<data_source>
random_data_source_factory::make_source(const ::model::ntp&) {
    return std::make_unique<random_data_source>();
}

std::unique_ptr<data_sink>
accounting_sink_factory::make_sink(const ::model::ntp&) {
    return std::make_unique<accounting_sink>();
}

} // namespace cluster_link::replication::tests
