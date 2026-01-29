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

#include "base/format_to.h"
#include "bytes/iobuf.h"

#include <cstdint>

namespace lsm::block {

// A handle to a block within an SST file.
struct handle {
    uint64_t offset = 0;
    uint64_t size = 0;

    bool operator==(const handle&) const = default;
    fmt::iterator format_to(fmt::iterator) const;

    // Encode this handle to an iobuf.
    iobuf as_iobuf() const;
    // Decode this handle from an iobuf.
    static handle from_iobuf(iobuf);
};

} // namespace lsm::block
