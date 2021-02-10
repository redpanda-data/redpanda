/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/event_listener.h"

#include "config/configuration.h"
#include "coproc/errc.h"
#include "coproc/logger.h"
#include "coproc/wasm_event.h"
#include "model/namespace.h"
#include "ssx/future-util.h"
#include "storage/parser_utils.h"
#include "utils/unresolved_address.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>

namespace coproc::wasm {

static wasm::event_action query_action(const iobuf& source_code) {
    /// If this came from a remove event, the validator would
    /// have failed if the value() field of the record wasn't
    /// empty. Therefore checking if this iobuf is empty is a
    /// certain way to know if the intended request was to
    /// deploy or remove
    return source_code.empty() ? wasm::event_action::remove
                               : wasm::event_action::deploy;
}

ss::future<> event_listener::stop() {
    _abort_source.request_abort();
    return _gate.close().then([this] { return _client.stop(); });
}

ss::future<>
event_listener::persist_actions(absl::btree_map<script_id, iobuf> wsas) {
    std::vector<enable_copros_request::data> enables;
    std::vector<script_id> disables;
    for (auto& [id, source] : wsas) {
        /// Keeping the active_ids cache up to date solves the issue of issuing
        /// a bunch of remove commands for scripts that aren't deployed (this
        /// could occur during bootstrapping)
        auto found = _active_ids.find(id);
        if (query_action(source) == event_action::remove) {
            if (found != _active_ids.end()) {
                _active_ids.erase(found);
                disables.emplace_back(id);
            }
        } else {
            /// ... or in the case if deploys are performed using the same key.
            /// This would normally be resolved if the events came in the same
            /// record batch by the reconcile events function, but not if they
            /// arrived in separate batches.
            if (found == _active_ids.end()) {
                _active_ids.insert(id);
                enables.emplace_back(enable_copros_request::data{
                  .id = id, .source_code = std::move(source)});
            }
        }
    }
    if (!enables.empty()) {
        enable_copros_request req{.inputs = std::move(enables)};
        co_await _dispatcher.enable_coprocessors(std::move(req));
    }
    if (!disables.empty()) {
        disable_copros_request req{.ids = std::move(disables)};
        co_await _dispatcher.disable_coprocessors(std::move(req));
    }
}

event_listener::event_listener(ss::sharded<pacemaker>& pacemaker)
  : _client({config::shard_local_cfg().kafka_api()[0].address})
  , _dispatcher(pacemaker, _abort_source) {}

ss::future<> event_listener::start() {
    return _client.connect().then([this] {
        (void)ss::with_gate(_gate, [this] {
            return ss::do_until(
              [this] { return _abort_source.abort_requested(); },
              [this] { return do_start(); });
        });
    });
}

static ss::future<std::vector<model::record_batch>>
decompress_wasm_events(model::record_batch_reader::data_t events) {
    return ssx::parallel_transform(
      std::move(events), [](model::record_batch&& rb) {
          /// If batch isn't compressed, returns 'rb'
          return storage::internal::decompress_batch(std::move(rb));
      });
}

ss::future<> event_listener::do_start() {
    /// This method performs the main polling behavior. Within a repeat loop it
    /// will poll from data until it cannot poll anymore. Normally we would be
    /// concerned about keeping all of this data in memory, however the topic is
    /// compacted, we don't expect the size of unique records to be very big.
    return ss::do_with(
      model::record_batch_reader::data_t(),
      [this](model::record_batch_reader::data_t& events) {
          /// The stop condition is met when its been detected that the stored
          /// offset has not moved.
          return ss::repeat([this, &events] {
                     model::offset initial_offset = _offset;
                     return poll_topic(events).then([this, initial_offset] {
                         return initial_offset == _offset
                                  ? ss::stop_iteration::yes
                                  : ss::stop_iteration::no;
                     });
                 })
            .then(
              [&events] { return decompress_wasm_events(std::move(events)); })
            .then([](std::vector<model::record_batch> events) {
                return wasm::reconcile_events(std::move(events));
            })
            .then([this](absl::btree_map<script_id, iobuf> wsas) {
                return persist_actions(std::move(wsas));
            })
            .then([this] { return ss::sleep_abortable(2s, _abort_source); })
            .handle_exception([](std::exception_ptr eptr) {
                vlog(coproclog.debug, "Exited sleep early: {}", eptr);
            });
      });
}

ss::future<>
event_listener::poll_topic(model::record_batch_reader::data_t& events) {
    return _client
      .fetch_partition(model::coprocessor_internal_tp, _offset, 64_KiB, 5s)
      .then([this, &events](kafka::fetch_response response) {
          if (
            (response.error != kafka::error_code::none)
            || _abort_source.abort_requested()) {
              return ss::now();
          }
          vassert(response.partitions.size() == 1, "Unexpected partition size");
          auto& p = response.partitions[0];
          vassert(
            p.name == model::coprocessor_internal_topic,
            "Unexpected topic name");
          vassert(p.responses.size() == 1, "Unexpected responses size");
          auto& pr = p.responses[0];
          if (!pr.has_error()) {
              auto crs = kafka::batch_reader(std::move(*pr.record_set));
              while (!crs.empty()) {
                  auto kba = crs.consume_batch();
                  if (!kba.v2_format || !kba.valid_crc || !kba.batch) {
                      vlog(
                        coproclog.warn,
                        "Invalid batch pushed to internal wasm topic");
                      continue;
                  }
                  events.push_back(std::move(*kba.batch));
                  /// Update so subsequent reads start at the correct offset
                  _offset = events.back().last_offset() + model::offset(1);
              }
          }
          return ss::now();
      });
};

} // namespace coproc::wasm
