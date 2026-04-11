// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

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
