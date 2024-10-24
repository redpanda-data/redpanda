/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/tests/test_fixture.h"

#include "model/fundamental.h"
#include "model/record.h"
#include "model/transform.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/lowres_clock.hh>

#include <gtest/gtest.h>

#include <stdexcept>

using namespace std::chrono_literals;

namespace transform::testing {
ss::future<> fake_sink::write(ss::chunked_fifo<model::record_batch> batches) {
    co_await _cork.wait();
    for (auto& batch : batches) {
        for (auto& r : batch.copy_records()) {
            _records.push_back(std::move(r));
        }
    }
    _cond_var.broadcast();
}

class read_timed_out : public ss::condition_variable_timed_out {
    const char* what() const noexcept override {
        return "waiting for read timed out";
    }
};

ss::future<model::record> fake_sink::read() {
    co_await _cond_var.wait(1s, [this] { return !_records.empty(); })
      .handle_exception([](auto) { throw read_timed_out(); });
    auto record = std::move(_records.front());
    _records.pop_front();
    co_return record;
}

void fake_sink::uncork() {
    _cork.signal(_cork.max_counter() - _cork.available_units());
}

void fake_sink::cork() { _cork.consume(std::max(_cork.available_units(), 0L)); }

ss::future<> fake_source::start() { co_return; }

ss::future<> fake_source::stop() { co_return; }

kafka::offset fake_source::latest_offset() {
    if (_batches.empty()) {
        return {};
    }
    return _batches.rbegin()->first;
}

ss::future<std::optional<kafka::offset>>
fake_source::offset_at_timestamp(model::timestamp ts, ss::abort_source*) {
    // Walk through the batches from most recent and look for the first batch
    // where the timestamp bounds is inclusive of `ts`.
    for (const auto& batch : _batches) {
        const auto& header = batch.second.header();
        if (header.max_timestamp >= ts) {
            auto delta = (header.first_timestamp - ts).value();
            for (const auto& r : batch.second.copy_records()) {
                if (r.timestamp_delta() >= delta) {
                    co_return kafka::offset(
                      header.base_offset() + r.offset_delta());
                }
            }
            co_return model::offset_cast(batch.second.header().last_offset());
        }
    }
    co_return std::nullopt;
}

kafka::offset fake_source::start_offset() const {
    if (_batches.empty()) {
        return {};
    }
    return _batches.begin()->first;
}

ss::future<model::record_batch_reader>
fake_source::read_batch(kafka::offset offset, ss::abort_source* as) {
    auto sub = as->subscribe([this]() noexcept { _cond_var.broadcast(); });
    co_await _cond_var.wait([this, as, offset] {
        if (as->abort_requested()) {
            return true;
        }
        auto it = _batches.lower_bound(offset);
        return it != _batches.end();
    });
    as->check();
    auto it = _batches.lower_bound(offset);
    co_return model::make_memory_record_batch_reader(it->second.copy());
}

ss::future<> fake_source::push_batch(model::record_batch batch) {
    _batches.emplace(model::offset_cast(batch.last_offset()), std::move(batch));
    _cond_var.broadcast();
    co_return;
}

ss::future<> fake_wasm_engine::start() {
    if (_started) {
        throw std::logic_error("starting already started wasm engine");
    }
    _started = true;
    co_return;
}

ss::future<> fake_wasm_engine::stop() {
    if (!_started) {
        throw std::logic_error("stopping already stopped wasm engine");
    }
    _started = false;
    co_return;
}

ss::future<> fake_offset_tracker::stop() { co_return; }

ss::future<> fake_offset_tracker::start() { co_return; }

void fake_wasm_engine::set_output_topics(std::vector<model::topic> topics) {
    _output_topics = std::move(topics);
}

void fake_wasm_engine::set_use_default_output_topic() {
    _output_topics = std::nullopt;
}

ss::future<> fake_wasm_engine::transform(
  model::record_batch batch,
  wasm::transform_probe*,
  wasm::transform_callback cb) {
    auto it = model::record_batch_iterator::create(batch);
    while (it.has_next()) {
        auto transformed = model::transformed_data::from_record(it.next());
        if (!_output_topics.has_value()) {
            auto success = co_await cb(std::nullopt, std::move(transformed));
            if (!success) {
                throw std::runtime_error("transform write failed!");
            }
        } else {
            for (const auto& topic : _output_topics.value()) {
                auto success = co_await cb(topic, transformed.copy());
                if (!success) {
                    throw std::runtime_error("transform write failed!");
                }
            }
        }
    }
}

ss::future<> fake_offset_tracker::commit_offset(
  model::output_topic_index idx, kafka::offset o) {
    _committed[idx] = o;
    _cond_var.broadcast();
    co_return;
}

ss::future<absl::flat_hash_map<model::output_topic_index, kafka::offset>>
fake_offset_tracker::load_committed_offsets() {
    co_return _committed;
}

class commit_wait_timed_out : public ss::condition_variable_timed_out {
    const char* what() const noexcept override {
        return "waiting for committed offset timed out";
    }
};

ss::future<> fake_offset_tracker::wait_for_committed_offset(
  model::output_topic_index index, kafka::offset o) {
    return _cond_var
      .wait(
        1s,
        [this, index, o] {
            auto it = _committed.find(index);
            return it != _committed.end() && it->second >= o;
        })
      .handle_exception([](auto) { throw commit_wait_timed_out(); });
}

ss::future<> fake_offset_tracker::wait_for_previous_flushes(ss::abort_source*) {
    co_return;
}

} // namespace transform::testing
