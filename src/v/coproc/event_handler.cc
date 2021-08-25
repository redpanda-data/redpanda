/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/event_handler.h"

#include "coproc/ntp_context.h"

#include <optional>

namespace coproc::wasm {

async_event_handler::async_event_handler(ss::sharded<pacemaker>& pacemaker)
  : _dispatcher(pacemaker, _abort_source) {}

ss::future<> async_event_handler::start() { co_return; }

ss::future<> async_event_handler::stop() {
    _abort_source.request_abort();
    co_return;
}

ss::future<event_handler::cron_finish_status> async_event_handler::cron() {
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
            co_return cron_finish_status::skip_pull;
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
            co_return cron_finish_status::replay_topic;
        }
    }
    co_return cron_finish_status::none;
}

ss::future<>
async_event_handler::process(absl::btree_map<script_id, parsed_event> wsas) {
    std::vector<enable_copros_request::data> enables;
    std::vector<script_id> disables;
    for (auto& [id, event] : wsas) {
        /// Keeping the active_ids cache up to date solves the issue of
        /// issuing a bunch of remove commands for scripts that aren't
        /// deployed (this could occur during bootstrapping)
        auto found = _active_ids.find(id);
        if (event.header.action == event_action::remove) {
            if (found != _active_ids.end()) {
                disables.emplace_back(id);
            }
        } else {
            /// ... or in the case if deploys are performed using the same
            /// key. This would normally be resolved if the events came in
            /// the same record batch by the reconcile events function, but
            /// not if they arrived in separate batches.
            if (found == _active_ids.end()) {
                enables.emplace_back(enable_copros_request::data{
                  .id = id, .source_code = std::move(event.data)});
            }
        }
    }
    /// TODO: In the future, maybe it would be cleaner to have a add/remove
    /// endpoint, instead of two seperate RPC endpoints
    if (!enables.empty()) {
        enable_copros_request req{.inputs = std::move(enables)};
        auto resp = co_await _dispatcher.enable_coprocessors(std::move(req));
        if (resp.has_error()) {
            throw event_handler_exception(fmt::format(
              "Failed to register coprocessors with the wasm engine: {}",
              resp.error()));
            co_return;
        } else {
            for (script_id id : resp.value()) {
                vlog(
                  coproclog.info,
                  "Successfully registered script with id: {}",
                  id);
                _active_ids.insert(id);
            }
        }
    }
    if (!disables.empty()) {
        disable_copros_request req{.ids = std::move(disables)};
        auto resp = co_await _dispatcher.disable_coprocessors(std::move(req));
        if (resp.has_error()) {
            /// In this case the code will follow a path that re-enters this
            /// method with the same inputs, and the call to enable_copros
            /// above succeeded but the call to disable_coprocessors failed,
            /// double registrations will be avoided because the ids have
            /// entered the \ref active_ids cache and will not be queued for
            /// re-registration
            throw event_handler_exception(fmt::format(
              "Failed to make disable request to the wasm engine: {}",
              resp.error()));
        } else {
            for (script_id id : resp.value()) {
                _active_ids.erase(id);
            }
        }
    }
}

ss::future<> data_policy_event_handler::start() { co_await _scritps.start(); }

ss::future<> data_policy_event_handler::stop() { co_await _scritps.stop(); }

ss::future<> data_policy_event_handler::process(
  absl::btree_map<script_id, parsed_event> wsas) {
    for (auto& [id, event] : wsas) {
        co_await _scritps.invoke_on_all(
          [id = id, &event = event](
            absl::btree_map<script_id, iobuf>& _local_scripts) mutable {
              if (event.header.action == event_action::deploy) {
                  _local_scripts.insert_or_assign(id, event.data.copy());
              } else {
                  _local_scripts.erase(id);
              }
          });
    }
    co_return;
}

std::optional<iobuf>
data_policy_event_handler::get_code(std::string_view name) {
    script_id id(xxhash_64(name.data(), name.size()));
    auto code = _scritps.local().find(id);
    if (code == _scritps.local().end()) {
        return std::nullopt;
    }

    return code->second.copy();
}

} // namespace coproc::wasm
