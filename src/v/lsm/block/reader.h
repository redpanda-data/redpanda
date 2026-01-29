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

#include "lsm/block/contents.h"
#include "lsm/core/internal/iterator.h"

#include <seastar/core/shared_ptr.hh>

namespace lsm::block {

// A reader of an SST data block.
class reader {
public:
    explicit reader(ss::lw_shared_ptr<contents>);

    // Create an iterator for an SST data block.
    //
    // The iterator's lifetime is independent from it's reader. The iterator may
    // (or may not) outlive the reader.
    std::unique_ptr<internal::iterator> create_iterator();

    size_t size_bytes() const { return _data->size(); }

private:
    ss::lw_shared_ptr<contents> _data;
    uint32_t _restart_offset; // Offset in data_ of restart array
};

} // namespace lsm::block
