/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/l1_memory_broker.h"

#include "base/vassert.h"

namespace cloud_topics::prefetch {

l1_memory_broker::l1_memory_broker(config::binding<size_t> budget_bytes)
  : _budget(std::move(budget_bytes)) {}

size_t l1_memory_broker::budget() const { return _budget(); }

size_t l1_memory_broker::reserved() const { return _reserved; }

size_t l1_memory_broker::available() const {
    const auto b = _budget();
    return _reserved >= b ? 0 : b - _reserved;
}

bool l1_memory_broker::over_budget() const { return _reserved > _budget(); }

bool l1_memory_broker::try_reserve(size_t n) {
    if (_reserved + n > _budget()) {
        return false;
    }
    _reserved += n;
    return true;
}

void l1_memory_broker::borrow(size_t n) { _reserved += n; }

void l1_memory_broker::release(size_t n) {
    vassert(
      n <= _reserved,
      "l1_memory_broker::release({}) > reserved({})",
      n,
      _reserved);
    _reserved -= n;
}

} // namespace cloud_topics::prefetch
