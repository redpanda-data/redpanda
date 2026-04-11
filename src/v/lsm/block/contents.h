// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "base/seastarx.h"
#include "bytes/ioarray.h"
#include "bytes/iobuf.h"
#include "lsm/block/handle.h"
#include "lsm/io/persistence.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

namespace lsm::block {

// Contents is a collection of (aligned) buffers that represent the contents of
// a block.
class contents {
public:
    // Read a block's contents from a file using the given handle.
    static ss::future<ss::lw_shared_ptr<contents>>
    read(io::random_access_file_reader*, handle);

    explicit contents(ioarray data);

    // Create block contents from copying out of an iobuf.
    //
    // NOTE: This intended to be used for testing.
    static ss::lw_shared_ptr<contents> copy_from(const iobuf&);

    // Share a portion of the contents as an iobuf.
    //
    // NOTE: This iobuf can lengthen the lifetime of the data backing contents,
    // so it's recommended to use this only for a short time, or to make a copy.
    iobuf share(size_t pos, size_t length);

    // Get a byte at the given index.
    char operator[](size_t i) const { return _data[i]; }
    // The size of the contents.
    size_t size() const { return _data.size(); }

    // Get a view of the contents as a string-like.
    //
    // REQUIRES: length <= max_buffer_size
    ioarray::string_view read_string(size_t pos, size_t length) const;

    // Read an uint32_t from the contents at the given offset.
    uint32_t read_fixed32(size_t offset) const;

private:
    ioarray _data;
};

} // namespace lsm::block
