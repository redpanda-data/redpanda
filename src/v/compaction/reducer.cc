// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/reducer.h"

#include <seastar/core/coroutine.hh>

#include <exception>

namespace compaction {

ss::future<> compaction::sliding_window_reducer::run() && {
    std::exception_ptr eptr;
    try {
        // Step 0: Initialize source
        co_await _src->initialize();

        // Step 1: Perform map building pass.
        co_await ss::repeat(
          [this]() { return _src->map_building_iteration(); });

        // Step 2: Initialize sink
        bool should_deduplicate = co_await _sink->initialize(*_src);

        if (should_deduplicate) {
            // Step 3: Perform de-duplication pass.
            co_await ss::repeat(
              [this]() { return _src->deduplication_iteration(*_sink); });
        }
    } catch (...) {
        eptr = std::current_exception();
    }

    // Done!
    bool success = eptr == nullptr;
    co_await _sink->finalize(success);

    if (eptr) {
        std::rethrow_exception(eptr);
    }
}

} // namespace compaction
