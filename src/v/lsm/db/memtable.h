// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "absl/container/btree_map.h"
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/lookup_result.h"

#include <seastar/core/sstring.hh>

namespace lsm::db {

// A memtable is a sorted map that stores key-value pairs in memory.
//
// Additionally, it stores the version of each key, which allows a third
// dimension to the key-value pairs, allowing snapshot isolation for reads
// by limiting to a version of the memtable.
//
// It is used to buffer writes before they are flushed to disk.
class memtable : public ss::enable_lw_shared_from_this<memtable> {
    class iterator;

public:
    using table = absl::btree_map<internal::key, iobuf, std::less<>>;

    memtable() noexcept;
    memtable(const memtable&) = delete;
    memtable& operator=(const memtable&) = delete;
    memtable(memtable&&) = delete;
    memtable& operator=(memtable&&) = delete;
    ~memtable();

    // Add a key-value pair to the database. Overwrites any previous value.
    //
    // REQUIRES: key.value_type is value
    void put(internal::key key, iobuf value);

    // Remove the value for a given key.
    //
    // REQUIRES: key.value_type is tombstone
    void remove(internal::key key);

    // Merge the supplied memtable into this memtable.
    // The resulting memtable should not be used anymore after passed into
    // this function as the data is *taken* from the memtable and applied to
    // this memtable.
    //
    // REQUIRES: the sequence numbers in the supplied memtable are >= to the
    // last applied sequence number in this memtable.
    void merge(ss::lw_shared_ptr<memtable>);

    // Get the value for a given key.
    //
    // REQUIRES: key.value_type is value
    lookup_result get(internal::key_view);

    // Create an iterator for this memtable.
    //
    // This iterator is safe to use in face of concurrent updates, but users can
    // see newer values that are added since the iterator is created.
    std::unique_ptr<internal::iterator> create_iterator();

    // The approximate amount of memory used for this memtable.
    size_t approximate_memory_usage() const { return _memory_usage; }

    // If the memtable is empty or not.
    bool empty() const { return _table.empty(); }

    // What is the minimum key in the memtable?
    //
    // REQUIRES: !empty()
    internal::key_view min_key() const { return _table.begin()->first; }

    // What is the maximum key in the memtable?
    //
    // REQUIRES: !empty()
    internal::key_view max_key() const { return _table.rbegin()->first; }

    std::optional<internal::sequence_number> last_seqno() const {
        return _last_seqno;
    }

private:
    friend class iterator;

    void invalidate_iterators();

    table _table;
    size_t _memory_usage = 0;
    std::optional<internal::sequence_number> _last_seqno;
    // We keep a dummy iterator alive to be able to reference all live
    // iterators.
    //
    // We invalidate all the live iterators when writes take places. This
    // approach is chosen over a data structure with stable iterators because
    // it's expected there are more entries in the memtable than live iterators
    // so overall there will be less time and memory used to track an intrusive
    // list for every entry in the memtable. However there are cases where it'd
    // be better to use a copy on write data structure or something with stable
    // iteration? Benchmarks needed, and for now this approach seemed like a
    // lower lift.
    std::unique_ptr<iterator> _list_holder;
};

} // namespace lsm::db
