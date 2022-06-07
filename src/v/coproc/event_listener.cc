/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/event_listener.h"

#include "config/configuration.h"
#include "coproc/errc.h"
#include "coproc/logger.h"
#include "coproc/types.h"
#include "coproc/wasm_event.h"
#include "model/namespace.h"
#include "net/unresolved_address.h"
#include "ssx/future-util.h"
#include "storage/parser_utils.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/flat_hash_map.h>

#include <exception>
#include <system_error>

namespace {

kafka::client::client make_client() {
    kafka::client::configuration cfg;
    cfg.brokers.set_value(std::vector<net::unresolved_address>{
      config::node().kafka_api()[0].address});
    cfg.retries.set_value(size_t(1));
    return kafka::client::client{to_yaml(cfg, config::redact_secrets::no)};
}

} // namespace

namespace coproc::wasm {

ss::future<> event_listener::stop() {
    vlog(coproclog.info, "Stopping coproc::wasm::event_listener");
    _abort_source.request_abort();
    co_await _gate.close().then([this] { return _client.stop(); });
    co_await ss::parallel_for_each(
      _handlers.begin(),
      _handlers.end(),
      [](std::pair<const event_type, wasm::event_handler*>& type_and_handler) {
          return type_and_handler.second->stop();
      });
}

event_listener::event_listener(ss::abort_source& as)
  : _client(make_client())
  , _abort_source(as) {}

ss::future<> event_listener::start() {
    co_await ss::parallel_for_each(
      _handlers.begin(),
      _handlers.end(),
      [](std::pair<const event_type, event_handler*>& type_and_handler) {
          return type_and_handler.second->start();
      });

    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _abort_source.abort_requested(); },
          [this] {
              return do_start()
                .then([this] {
                    return ss::sleep_abortable(1s, _abort_source)
                      .handle_exception_type([](const ss::sleep_aborted&) {});
                })
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      coproclog.warn,
                      "Error listening for coproc events - {}",
                      e);
                });
          });
    });
    co_return;
}

void event_listener::register_handler(event_type type, event_handler* handler) {
    _handlers[type] = handler;
    vlog(
      coproclog.info,
      "Register new handler for wasm_event_type: {}",
      coproc_type_as_string_view(type));
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

    for (auto& [_, handler] : _handlers) {
        auto cron_res = co_await handler->preparation_before_process();
        switch (cron_res) {
        case event_handler::cron_finish_status::skip_pull: {
            co_return;
        }
        case event_handler::replay_topic: {
            _offset = model::offset(0);
            break;
        }
        default:
            break;
        }
    }

    co_await do_ingest();
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
    auto reconciled = wasm::reconcile_events_by_type(std::move(decompressed));
    co_await process_events(std::move(reconciled), last_offset);
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

ss::future<>
event_listener::process_events(event_batch events, model::offset last_offset) {
    for (auto& [type, prepared_events] : events) {
        auto it = _handlers.find(type);
        if (it == _handlers.end()) {
            vlog(
              coproclog.warn,
              "Can not find handler for coproc_type: {}",
              coproc_type_as_string_view(type));
            continue;
        }

        try {
            co_await it->second->process(std::move(prepared_events));
        } catch (const async_event_handler_exception& ex) {
            _offset = last_offset;
            vlog(coproclog.error, "{}", ex.what());
        }
    }
}

} // namespace coproc::wasm
