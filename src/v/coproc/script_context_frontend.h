/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "coproc/script_context_router.h"
#include "coproc/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

namespace coproc {
/// Type of result to expect from 'read_from_inputs', to be immeadiately
/// dispatched to a wasm engine
using input_read_results = std::vector<process_batch_request::data>;

/// Arugments to pass to 'read_from_inputs', trivially copyable
struct input_read_args {
    script_id id;
    ssx::semaphore& read_sem;
    ss::abort_source& abort_src;
    routes_t& inputs;
};

/**
 * Ingests data from a single coprocessors registered input topics.
 *
 * Abortable mechanism, reads from all input topics within the bounds of a
 * semaphaore. Contains a single side effect that will update the last read
 * offset from each corresponding input ntp.
 * @params args
 * @return list of process_batch_requests to be sent to the wasm engine
 */
ss::future<input_read_results> read_from_inputs(input_read_args);
} // namespace coproc
