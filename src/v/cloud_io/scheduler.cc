/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/scheduler.h"

#include "cloud_io/scheduler_policy.h"

#include <seastar/core/coroutine.hh>

#include <utility>

namespace cloud_io {

namespace {

/// No-op admission policy.
///
/// When active, concurrency is bounded by the client pool's capacity alone.
class passthrough final : public scheduler_policy {
public:
    using scheduler_policy::scheduler_policy;

    ss::future<> admit(group_id, ss::abort_source& as) override {
        as.check();
        return ss::now();
    }

    bool try_admit(group_id) noexcept override { return true; }

    void release(group_id) noexcept override {}

    size_t in_flight(group_id) const noexcept override { return 0; }
    size_t waiters(group_id) const noexcept override { return 0; }
    size_t available_slots() const noexcept override { return _capacity; }
    size_t total_capacity() const noexcept override { return _capacity; }
};

} // namespace

std::unique_ptr<scheduler_policy>
scheduler::make_policy(size_t capacity, scheduler_config cfg) {
    switch (cfg.policy) {
    case policy_type::passthrough:
        return std::make_unique<passthrough>(capacity);
    }
    std::unreachable();
}

scheduler::scheduler(size_t capacity, scheduler_config cfg)
  : _policy(make_policy(capacity, std::move(cfg))) {}

scheduler::~scheduler() noexcept = default;

ss::future<> scheduler::stop() {
    _draining = true;
    co_await _policy->stop();
}

ss::future<> scheduler::admit(group_id g, ss::abort_source& as) {
    if (_draining) {
        throw ss::abort_requested_exception{};
    }
    co_await _policy->admit(g, as);
}

bool scheduler::try_admit(group_id g) {
    if (_draining) {
        return false;
    }
    return _policy->try_admit(g);
}

void scheduler::release(group_id g) { _policy->release(g); }

size_t scheduler::in_flight(group_id g) const { return _policy->in_flight(g); }
size_t scheduler::waiters(group_id g) const { return _policy->waiters(g); }
size_t scheduler::available_slots() const { return _policy->available_slots(); }
size_t scheduler::total_capacity() const { return _policy->total_capacity(); }
bool scheduler::has_waiters() const {
    for (const auto g : all_group_ids) {
        if (_policy->waiters(g) > 0) {
            return true;
        }
    }
    return false;
}

} // namespace cloud_io
