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
