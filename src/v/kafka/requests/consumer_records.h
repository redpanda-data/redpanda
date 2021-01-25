/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "kafka/types.h"

#include <optional>

namespace kafka {

///\brief consumer_records are a concatenation of multiple model::record_batch
/// on the wire; without a size header (c.f. a kafka array<thing>)
class consumer_records {
public:
    consumer_records() = default;

    explicit consumer_records(std::optional<iobuf> buf)
      : _record_set(std::move(buf)) {}

    // Returns true if the record_set is nullopt or empty
    bool empty() const { return !_record_set || _record_set->empty(); }

    // Returns the size of the buffer, or -1 if it's nullopt
    size_t size_bytes() const {
        return _record_set ? _record_set->size_bytes() : -1;
    }

    // Check that the iobuf isn't nullopt
    constexpr explicit operator bool() const noexcept {
        return _record_set.operator bool();
    }

    // Release any remaining iobuf that hasn't been consumed
    std::optional<iobuf> release() && { return std::move(_record_set); }

private:
    std::optional<iobuf> _record_set;
};

} // namespace kafka
