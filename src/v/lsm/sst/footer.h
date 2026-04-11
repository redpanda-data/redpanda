// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "base/format_to.h"
#include "bytes/iobuf.h"
#include "lsm/block/handle.h"

namespace lsm::sst {

// Footer encapsulates the fixed information stored at the tail end of every
// table file.
// An SST structure is a series of data blocks (with the two more blocks
// being special), then a footer. The footer contains pointers to these last
// two blocks:
// The metaindex block this contains meta information like the bloom
// filter.
// Index block: this block contains an index of each data blocks and the end
// key for each block.
struct footer {
    block::handle metaindex_handle;
    block::handle index_handle;

    // Encoded length of the footer. It consists of two block handles, a crc and
    // a magic number.
    constexpr static size_t encoded_length = 2 * sizeof(block::handle)
                                             + sizeof(uint32_t)
                                             + sizeof(uint64_t);

    bool operator==(const footer&) const = default;
    fmt::iterator format_to(fmt::iterator) const;
    // Encode this footer as an iobuf.
    iobuf as_iobuf() const;
    // Decode this footer as an iobuf.
    static footer from_iobuf(iobuf);
};

} // namespace lsm::sst
