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

// Helper to allow writing a crash_description out to a file in an async-signal
// safe way. The state transition diagram for the object is shown below:
//
// clang-format off
// +---------------+  initialize()   +-------------+  fill()   +--------+  write()   +---------+      
// | uninitialized +---------------->| initialized +---------->| filled +----------->| written |      
// +-----+---------+                 +------+------+           +---+----+            +-+-------+      
//       |                                  |                      |                   |             
//       |                                  |                      |                   |             
//       |                                  |                      |                   |             
//       |                                  |                      |                   |             
//       |                                  |                      |                   |             
//       |                                  |                      |        release()  |   +--------+
//       +----------------------------------+----------------------+-------------------+-->|released|
//                                                                                         +--------+
// clang-format on
class prepared_writer {
public:
    // TODO: import the naming of the methods and their correspondence to the
    // state
    ss::future<> initialize(std::filesystem::path);
    ss::future<> release();

    // Async-signal safe
    constexpr bool initialized() const { return _state == state::initialized; }

    /// Async-signal safe
    crash_description& fill();

    /// Async-signal safe
    void write();

private:
    enum class state { uninitialized, initialized, filled, written, released };
    friend std::ostream& operator<<(std::ostream&, state);

    bool update_crash_md(const crash_description&);

    state _state{state::uninitialized};
    crash_description _prepared_cd;
    iobuf _serde_output;
    std::filesystem::path _crash_report_file_name;
    int _fd{0};
};

} // namespace crash_tracker
