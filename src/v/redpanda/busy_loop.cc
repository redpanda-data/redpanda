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

#include "redpanda/busy_loop.h"

#include "random/generators.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>

busy_loop::busy_loop(
  int min_spins_per_scheduling_point,
  int max_spins_per_scheduling_point,
  int num_fibers) {
    for (int i = 0; i < num_fibers; i++) {
        ssx::spawn_with_gate(
          _gate,
          [min_spins_per_scheduling_point,
           max_spins_per_scheduling_point,
           this] {
              return run_loop(
                min_spins_per_scheduling_point, max_spins_per_scheduling_point);
          });
    }
}

ss::future<> busy_loop::run_loop(int min, int max) {
    while (!_as.abort_requested()) {
        int spins_per_scheduling_point
          = min == max ? min : random_generators::get_int(min, max);
        co_await ss::maybe_yield();
        volatile int spins = 0;
        while (spins < spins_per_scheduling_point) {
            spins = spins + 1;
        }
    }
}

ss::future<> busy_loop::stop() {
    _as.request_abort();
    co_await _gate.close();
}

bool busy_loop_manager::start(
  int min_spins_per_scheduling_point,
  int max_spins_per_scheduling_point,
  int num_fibers) {
    if (_loop) {
        return false;
    }
    _loop = busy_loop::start(
      min_spins_per_scheduling_point,
      max_spins_per_scheduling_point,
      num_fibers);
    return true;
}

ss::future<> busy_loop_manager::stop() {
    if (_loop) {
        co_await _loop->stop();
        _loop.reset();
    }
}
