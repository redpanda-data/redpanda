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
#include "coproc/api/wasm_list.h"
#include "coproc/errc.h"
#include "coproc/logger.h"
#include "coproc/types.h"
#include "kafka/client/publish_batch_consumer.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/create_topics_request.h"
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
#include <seastar/core/sleep.hh>

#include <exception>
#include <system_error>

namespace {

kafka::client::client make_client() {
    kafka::client::configuration cfg;
    cfg.brokers.set_value(std::vector<unresolved_address>{
      config::shard_local_cfg().kafka_api()[0].address});
    cfg.retries.set_value(size_t(1));
    return kafka::client::client{to_yaml(cfg)};
}

} // namespace

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
    vlog(coproclog.info, "Stopping coproc::wasm::event_listener");
    _abort_source.request_abort();
    return _gate.close().then([this] { return _client.stop(); });
}

ss::future<> event_listener::persist_actions(
  absl::btree_map<script_id, log_event> wsas, model::offset last_offset) {
    std::vector<enable_copros_request::data> enables;
    std::vector<script_id> disables;
    for (auto& [id, event] : wsas) {
        /// Keeping the active_ids cache up to date solves the issue of issuing
        /// a bunch of remove commands for scripts that aren't deployed (this
        /// could occur during bootstrapping)
        auto found = _active_ids.find(id);
        if (query_action(event.source_code) == event_action::remove) {
            if (found != _active_ids.end()) {
                disables.emplace_back(id);
            }
        } else {
            /// ... or in the case if deploys are performed using the same key.
            /// This would normally be resolved if the events came in the same
            /// record batch by the reconcile events function, but not if they
            /// arrived in separate batches.
            if (found == _active_ids.end()) {
                enables.emplace_back(enable_copros_request::data{
                  .id = id, .source_code = std::move(event.source_code)});
            }
        }
    }
    /// TODO: In the future, maybe it would be cleaner to have a add/remove
    /// endpoint, instead of two seperate RPC endpoints
    if (!enables.empty()) {
        enable_copros_request req{.inputs = std::move(enables)};
        auto resp = co_await _dispatcher.enable_coprocessors(std::move(req));
        if (resp.has_error()) {
            vlog(
              coproclog.error,
              "Failed to register coprocessors with the wasm engine: {}",
              resp.error());
            _offset = last_offset;
            co_return;
        } else {
            for (script_id id : resp.value()) {
                vlog(
                  coproclog.info,
                  "Successfully registered script with id: {}",
                  id);
                _active_ids[id] = std::move(wsas[id].attrs);
            }
            co_await advertise_state_update();
        }
    }
    if (!disables.empty()) {
        disable_copros_request req{.ids = std::move(disables)};
        auto resp = co_await _dispatcher.disable_coprocessors(std::move(req));
        if (resp.has_error()) {
            vlog(
              coproclog.error,
              "Failed to make disable request to the wasm engine: {}",
              resp.error());
            /// In this case the code will follow a path that re-enters this
            /// method with the same inputs, and the call to enable_copros above
            /// succeeded but the call to disable_coprocessors failed, double
            /// registrations will be avoided because the ids have entered the
            /// \ref active_ids cache and will not be queued for re-registration
            _offset = last_offset;
        } else {
            for (script_id id : resp.value()) {
                _active_ids.erase(id);
            }
            co_await advertise_state_update();
        }
    }
}

event_listener::event_listener(ss::sharded<pacemaker>& pacemaker)
  : _client(make_client())
  , _pacemaker(pacemaker)
  , _dispatcher(pacemaker, _abort_source) {}

ss::future<> event_listener::start() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _abort_source.abort_requested(); },
          [this] {
              return do_start().then([this] {
                  return ss::sleep_abortable(1s, _abort_source)
                    .handle_exception_type([](const ss::sleep_aborted&) {});
              });
          });
    });
    return ss::now();
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
    bool connected = co_await _client.is_connected();
    if (!connected) {
        try {
            /// Connect may throw if it cannot establish a connection within the
            /// pre-defined global retry policy
            co_await _client.connect();
        } catch (const kafka::client::broker_error& e) {
            vlog(
              coproclog.warn,
              "Failed to connect to a broker within the retry policy: {}",
              e);
            co_return;
        }
    }
    auto heartbeat = co_await _dispatcher.heartbeat();
    if (heartbeat.has_error()) {
        std::error_code err = heartbeat.error();
        if (
          err == rpc::errc::client_request_timeout
          || err == rpc::errc::disconnected_endpoint) {
            vlog(
              coproclog.error,
              "Wasm engine failed to reply to heartbeat within the "
              "expected "
              "interval");
            co_return;
        }
    } else if (heartbeat.value().data != _active_ids.size()) {
        /// There is a discrepency between the number of registered coprocs
        /// according to redpanda and according to the wasm engine.
        /// Reconcile all state from offset 0.
        vlog(coproclog.info, "Replaying coprocessor state...");
        if (co_await _dispatcher.disable_all_coprocessors()) {
            vlog(
              coproclog.error,
              "Failed to reset wasm_engine state, will keep retrying...");
        } else {
            _active_ids.clear();
            _offset = model::offset(0);
            co_await advertise_state_update();
        }
    }

    co_await do_ingest();
}

ss::future<> event_listener::boostrap_status_topic() {
    /// Bootstrap the coproc status topic. At mininum the wasm engine must
    /// be up for this part of code to be reached.
    kafka::creatable_topic copro_status_topic{
      .name = model::coprocessor_status_topic,
      .num_partitions = 1,
      .replication_factor = 1,
      .configs{kafka::createable_topic_config{
        .name = "cleanup.policy", .value = "compact"}}};
    auto responses = co_await _client.create_topic(
      std::move(copro_status_topic));
    vassert(responses.data.topics.size() == 1, "more then expected responses");
    const auto code = responses.data.topics[0].error_code;
    if (
      code != kafka::error_code::none
      && code != kafka::error_code::topic_already_exists) {
        vlog(
          coproclog.error,
          "Error recieved when attempting to create coproc status topic: "
          "{}, will try again",
          code);
    }
}

ss::future<> event_listener::do_ingest() {
    /// This method performs the main polling behavior, looping until theres no
    /// more data to read from the topic. Normally we would be concerned about
    /// keeping all of this data in memory, however the topic is compacted, we
    /// don't expect the size of unique records to be very big.
    model::record_batch_reader::data_t events;
    model::offset last_offset = _offset;
    ss::stop_iteration stop{};
    while (stop == ss::stop_iteration::no) {
        stop = co_await poll_topic(events);
    }
    auto decompressed = co_await decompress_wasm_events(std::move(events));
    auto reconciled = wasm::reconcile_events(std::move(decompressed));
    co_await persist_actions(std::move(reconciled), last_offset);
}

ss::future<ss::stop_iteration>
event_listener::poll_topic(model::record_batch_reader::data_t& events) {
    auto response = co_await _client.fetch_partition(
      model::coprocessor_internal_tp, _offset, 64_KiB, 100ms);
    if (
      response.data.error_code != kafka::error_code::none
      || _abort_source.abort_requested()) {
        co_return ss::stop_iteration::yes;
    }
    vassert(response.data.topics.size() == 1, "Unexpected partition size");
    auto& p = response.data.topics[0];
    vassert(
      p.name == model::coprocessor_internal_topic, "Unexpected topic name");
    vassert(p.partitions.size() == 1, "Unexpected responses size");
    auto& pr = p.partitions[0];
    model::offset initial = _offset;
    if (pr.error_code == kafka::error_code::none) {
        auto crs = kafka::batch_reader(std::move(*pr.records));
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
    co_return initial == _offset ? ss::stop_iteration::yes
                                 : ss::stop_iteration::no;
};

ss::future<> event_listener::advertise_state_update(std::size_t n) {
    if (n <= 0) {
        vlog(
          coproclog.error,
          "Attempted to create and produce to status topic multiple times, "
          "will try again later");
        co_return;
    }
    model::node_id node_id = model::node_id(config::shard_local_cfg().node_id);
    model::record_batch_reader status = co_await current_status(
      node_id, _pacemaker, _active_ids);
    auto results = co_await std::move(status).for_each_ref(
      kafka::client::publish_batch_consumer(
        _client, model::coprocessor_status_tp),
      model::no_timeout);
    vassert(
      results.size() == 1,
      "Only one response was to be expected for this request");
    if (
      results[0].error_code == kafka::error_code::unknown_topic_or_partition) {
        co_await boostrap_status_topic();
        co_await advertise_state_update(n - 1);
    } else if (results[0].error_code != kafka::error_code::none) {
        vlog(coproclog.error, "Error producing onto coproc status topic");
    }
}

} // namespace coproc::wasm
