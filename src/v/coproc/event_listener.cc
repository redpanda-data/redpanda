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
#include "kafka/client/exceptions.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/metadata.h"
#include "model/namespace.h"
#include "rpc/dns.h"
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
    return _gate.close().then([this] {
        if (_client) {
            return _client->stop().finally([c{std::move(_client)}] {});
        }
        return ss::now();
    });
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
  : _dispatcher(pacemaker, _abort_source) {}

ss::future<> event_listener::do_connect() {
    vassert(!_client, "do_connect() called when theres already a valid client");
    /// Invoke metadata request on a new client connected to this broker
    auto addr = co_await rpc::resolve_dns(
      config::shard_local_cfg().kafka_api.value()[0].address);
    auto client = std::make_unique<kafka::client::transport>(
      rpc::base_transport::configuration{.server_addr = addr});
    co_await client->connect();
    /// Parse the response looking for the broker that is the leader for the
    /// model::coprocessor_internal_topic
    kafka::metadata_response r = co_await client->dispatch(
      kafka::metadata_request{.list_all_topics = true});
    auto found = std::find_if(
      r.topics.begin(), r.topics.end(), [](kafka::metadata_response::topic& t) {
          return t.name == model::coprocessor_internal_topic;
      });
    if (found == r.topics.end()) {
        co_await client->stop();
        throw kafka::client::partition_error(
          model::coprocessor_internal_tp,
          kafka::error_code::unknown_topic_or_partition);
    }
    vassert(
      found->partitions.size() == 1, "Copro internal topic misconfigured");
    model::node_id id = found->partitions[0].leader;
    auto broker = std::find_if(
      r.brokers.begin(),
      r.brokers.end(),
      [id](const kafka::metadata_response::broker& b) {
          return b.node_id == id;
      });
    /// If the broker is found set _client and connect (if neccessary)
    vassert(broker != r.brokers.end(), "Erranous kafka metadata response");
    ss::socket_address broker_addr(
      ss::net::ipv4_address(broker->host), broker->port);
    if (broker_addr == client->server_address()) {
        _client = std::move(client);
        co_return;
    }
    co_await client->stop();
    _client = std::make_unique<kafka::client::transport>(
      rpc::base_transport::configuration{.server_addr = broker_addr});
    co_await _client->connect();
}

ss::future<> event_listener::start() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _abort_source.abort_requested(); },
          [this] {
              if (!_client) {
                  return do_connect().handle_exception(
                    [](std::exception_ptr eptr) {
                        vlog(coproclog.trace, "do_connect() failed: {}", eptr);
                        return ss::sleep_abortable(1s).handle_exception(
                          [](std::exception_ptr) {});
                    });
              }
              return do_start();
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
    /// This method performs the main polling behavior. Within a repeat loop it
    /// will poll from data until it cannot poll anymore. Normally we would be
    /// concerned about keeping all of this data in memory, however the topic is
    /// compacted, we don't expect the size of unique records to be very big.
    return ss::do_with(
      model::record_batch_reader::data_t(),
      [this](model::record_batch_reader::data_t& events) {
          /// The stop condition is met when its been detected that the stored
          /// offset has not moved.
          return ss::repeat([this, &events] { return poll_topic(events); })
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

ss::future<ss::stop_iteration>
event_listener::poll_topic(model::record_batch_reader::data_t& events) {
    std::vector<kafka::fetch_request::partition> partitions;
    partitions.push_back(kafka::fetch_request::partition{
      .id = model::partition_id(0), .fetch_offset = _offset});
    std::vector<kafka::fetch_request::topic> topics;
    topics.push_back(kafka::fetch_request::topic{
      .name = model::coprocessor_internal_topic,
      .partitions = std::move(partitions)});
    kafka::fetch_request req{
      .max_wait_time = 5s, .min_bytes = 0, .topics = std::move(topics)};
    vassert(_client, "Handle to kafka::transport must not be null");
    return _client->dispatch(std::move(req))
      .then([this, &events](kafka::fetch_response response) {
          if (response.error != kafka::error_code::none) {
              return _client->stop()
                .then([] { return ss::stop_iteration::yes; })
                .finally([c{std::move(_client)}] {});
          }
          vassert(
            response.partitions.size() == 1, "Unexpected partition size ");
          auto& p = response.partitions[0];
          vassert(
            p.name == model::coprocessor_internal_topic,
            "Unexpected topic name");
          vassert(p.responses.size() == 1, "Unexpected responses size");
          auto& pr = p.responses[0];
          model::offset last_offset = _offset;
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
          auto should_stop = (_offset == last_offset) ? ss::stop_iteration::yes
                                                      : ss::stop_iteration::no;
          return ss::make_ready_future<ss::stop_iteration>(should_stop);
      });
};

} // namespace coproc::wasm
