// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "jumbo_log/segment/sparse_index.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "jumbo_log/segment/segment.h"
#include "model/fundamental.h"

#include <absl/types/optional.h>

#include <optional>

namespace jumbo_log::segment {

std::optional<chunk_loc>
sparse_index::find_chunk(const model::ntp& ntp, model::offset offset) const {
    vassert(!_data.entries.empty(), "jls chunk index is empty");

    // TODO(nv): Binary search.

    auto it = _data.entries.begin();

    auto chunk_ix = 0;

    // If the first chunk is already greater than the requested NTP and offset,
    // it means that the requested NTP and offset are not in the index.
    if (unlikely(!within_bounds(ntp, offset))) {
        return std::nullopt;
    } else {
        it++;
    }

    // Iterate until we find the first chunk that is greater than the requested
    // NTP and offset and return the one before it.
    while (it != _data.entries.end()) {
        if (ntp < it->ntp || (it->ntp == ntp && offset < it->offset)) {
            break;
        }

        chunk_ix++;
        it++;
    }

    return _data.entries[chunk_ix].loc;
}

bool sparse_index::within_bounds(
  const model::ntp& ntp, model::offset offset) const {
    if (_data.entries.empty()) {
        return false;
    }

    auto first = _data.entries.front();
    bool gte_first_entry = ntp < first.ntp
                           || (ntp == first.ntp && offset < first.offset);
    if (gte_first_entry) {
        return false;
    }

    bool lte_upper_limit
      = _data.upper_limit.first < ntp
        || (_data.upper_limit.first == ntp && _data.upper_limit.second < offset);
    if (unlikely(lte_upper_limit)) {
        return false;
    }

    return true;
}

} // namespace jumbo_log::segment
