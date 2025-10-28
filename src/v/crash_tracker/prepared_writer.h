/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "crash_tracker/types.h"

namespace crash_tracker {

// prepared_writer is a thread-safe helper for writing a crash_description to a
// file in an async-signal safe way. The state transition diagram for the object
// is below. Note that fill() may race with other calls of fill() or with
// release(), and the class safely breaks this race. Calling fill() returns a
// nullptr if it lost the race (i.e. if the object is already in
// filled/written/released state). Calling release() is a noop if it lost the
// race (i.e. if the object is already in filled/written state).
//
// clang-format off
// +---------------+  initialize()   +-------------+  fill()   +--------+  write()   +---------+
// | uninitialized +---------------->| initialized +---------->| filled +----------->| written |
// +------+--------+                 +------+------+           +--------+            +---------+
//       / \                                |
//        |                                 |
//        |                                 |
//        |                                 |
//      reset()                             |
//                                          |                        release()      +----------+
//                                          +-------------------------------------->| released |
//                                                                                  +----------+
// clang-format on
//
// Please note there is a `reset()` method provided that can be used but _only_
// for testing purposes.  It will reset the prepared_writer back to the
// uninitialized state
class prepared_writer {
public:
    ss::future<> initialize(std::filesystem::path);
    ss::future<> release();

    /// Async-signal safe
    /// May return nullptr if the prepared_writer has already been consumed
    crash_description* fill();

    /// Async-signal safe
    /// Must be called after a fill() that returned a non-null value
    /// Returns true if writing the crash file succeeded
    [[nodiscard]] bool write();

    /// Only to be used under testing situations.  Must be called _after_
    /// release()
    void reset();

    /// Returns the writer's filename. It returns nullopt when the file does not
    /// exist (eg. before initialization, or after the file has been deleted).
    const std::optional<std::filesystem::path>&
    get_crash_report_file_name() const {
        return _crash_report_file_name;
    }

    bool initialized() const { return _state != state::uninitialized; }

private:
    enum class state { uninitialized, initialized, filled, written, released };
    friend std::ostream& operator<<(std::ostream&, state);

    // Returns true on success, false on failure
    bool try_write_crash();

    std::atomic<state> _state{state::uninitialized};

    // We want to avoid taking locks during signal handling. An atomic enum with
    // a few states should be lock-free implementable on the platforms redpanda
    // supports, but if this check ever fails we could change the type of the
    // enum class to an enum or integer.
    static_assert(std::atomic<state>::is_always_lock_free);

    crash_description _prepared_cd;
    iobuf _serde_output;
    std::optional<std::filesystem::path> _crash_report_file_name;
    int _fd{-1};
};

} // namespace crash_tracker
