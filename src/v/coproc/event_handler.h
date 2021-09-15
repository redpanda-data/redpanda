/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "coproc/fwd.h"
#include "coproc/logger.h"
#include "coproc/wasm_event.h"
#include "seastarx.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>

namespace coproc::wasm {

class event_handler_exception : public std::exception {
public:
    explicit event_handler_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

/// This class implements logic for handling events from internal coproc topic.
/// There are 2 methods: for process event for specifix type and cron
/// events(like heartbeats) We need store this handler to application, and pass
/// it to register method of event_listener.
class event_handler {
public:
    virtual ~event_handler() {}

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    enum cron_finish_status { replay_topic, skip_pull, none };

    /// Cron. Run it from loop in do_start event_listener function
    virtual ss::future<event_handler::cron_finish_status>
    preparation_before_process() = 0;

    /// Process parsed event with the same coproc type
    virtual ss::future<>
    process(absl::btree_map<script_id, parsed_event> wsas) = 0;
};

class async_event_handler_exception final : public event_handler_exception {
public:
    explicit async_event_handler_exception(ss::sstring msg) noexcept
      : event_handler_exception(std::move(msg)) {}
};

class async_event_handler final : public event_handler {
public:
    explicit async_event_handler(script_dispatcher&);

    ss::future<> start() override;
    ss::future<> stop() override;

    ss::future<event_handler::cron_finish_status>
    preparation_before_process() override;

    ss::future<>
    process(absl::btree_map<script_id, parsed_event> wsas) override;

private:
    ss::gate _gate;

    /// Used to make requests to the wasm engine
    script_dispatcher& _dispatcher;
};

class data_policy_event_handler final : public event_handler {
public:
    ss::future<> start() override;
    ss::future<> stop() override;

    ss::future<event_handler::cron_finish_status>
    preparation_before_process() override {
        co_return cron_finish_status::none;
    }

    ss::future<>
    process(absl::btree_map<script_id, parsed_event> wsas) override;

    std::optional<iobuf> get_code(std::string_view name);

private:
    /// Map of known script ids to their code
    ss::sharded<absl::btree_map<script_id, iobuf>> _scripts;
};

} // namespace coproc::wasm
