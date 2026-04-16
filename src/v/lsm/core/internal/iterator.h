// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "bytes/iobuf.h"
#include "lsm/core/internal/keys.h"

#include <seastar/core/future.hh>

namespace lsm::internal {

// Options that control the behavior of iterators over SST files.
struct iterator_options {
    // The amount of extra data to read ahead on each block read. Buffered
    // data is served to subsequent sequential reads without I/O.
    size_t readahead_size = 0;
};

// A bi-directional iterator over sorted key-value pairs.
class iterator {
public:
    iterator() = default;
    iterator(const iterator&) = delete;
    iterator& operator=(const iterator&) = delete;
    iterator(iterator&&) = delete;
    iterator& operator=(iterator&&) = delete;
    virtual ~iterator() = default;

    // Create an empty iterator. This is used to represent an empty source.
    static std::unique_ptr<iterator> create_empty();

    // An iterator is either positioned at a key/value pair, or
    // not valid. This method returns true iff the iterator is valid.
    virtual bool valid() const = 0;

    // Position at the first key in the source. The iterator is valid()
    // after this call iff the source is not empty.
    virtual ss::future<> seek_to_first() = 0;

    // Position at the last key in the source. The iterator is
    // valid() after this call iff the source is not empty.
    virtual ss::future<> seek_to_last() = 0;

    // Position at the first key in the source that is at or past target.
    // The iterator is valid() after this call iff the source contains
    // an entry that comes at or past target.
    virtual ss::future<> seek(key_view target) = 0;

    // Moves to the next entry in the source. After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: valid()
    virtual ss::future<> next() = 0;

    // Moves to the previous entry in the source. After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: valid()
    virtual ss::future<> prev() = 0;

    // Return the key for the current entry. The returned value is only valid
    // until the iterator is moved (next/prev/seek* is called).
    // REQUIRES: valid()
    virtual key_view key() = 0;

    // Return the value for the current entry.
    // REQUIRES: valid()
    virtual iobuf value() = 0;
};

} // namespace lsm::internal
