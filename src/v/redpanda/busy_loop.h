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
#pragma once

#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <memory>

class busy_looper;

class busy_loop {
public:
    // Instantiates the given number of fibers, each running `run_loop`.
    static std::unique_ptr<busy_loop> start(
      int min_spins_per_scheduling_point,
      int max_spins_per_scheduling_point,
      int num_fibers) {
        return std::make_unique<busy_loop>(
          min_spins_per_scheduling_point,
          max_spins_per_scheduling_point,
          num_fibers);
    }

    // Runs a single fiber that spins the given number of times per scheduling
    // point.
    ss::future<> run_loop(int min, int max);

    // Stops any existing loop.
    ss::future<> stop();

    busy_loop(
      int spins_per_scheduling_point,
      int max_spins_per_scheduling_po,
      int num_fibers);

private:
    friend class busy_looper;

    ss::gate _gate;
    ss::abort_source _as;
};

// Wrapper around a single busy loop to ensure only one loop is active at a
// time.
class busy_loop_manager {
public:
    explicit busy_loop_manager() = default;

    // Starts a busy loop and returns true, or returns false if a loop is
    // already running.
    bool start(
      int min_spins_per_scheduling_point,
      int max_spins_per_scheduling_point,
      int num_fibers);

    // Stops any existing loops.
    ss::future<> stop();

private:
    std::unique_ptr<busy_loop> _loop;
};
