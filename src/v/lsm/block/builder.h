// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "lsm/core/internal/keys.h"

namespace lsm::block {

// A builder of a block within an SSTable.
//
// A block is a sequence of key-value pairs. Sequential keys have prefixes
// elided similar to the structure in LevelDB.
//
// This class is mostly responsible for the encoding of the block structure,
// which is as follows:
//
// ```
// sharedkeylen uint32_t
// uniquekeylen uint32_t
// valuelen uint32_t
// key iobuf
// value iobuf
// ```
// Every K entries we reset the shared key as a "reset" point (so
// sharedkeylen=0) and record the offset of said key in a footer, so that we can
// binary search over the keys to find the right one.
//
// At the end of a block we record the reset points and their offsets, like so:
// ```
// repeated restarts uint32_t
// num_restarts uint32_t
// ```
class builder {
public:
    struct options {
        constexpr static uint32_t default_restart_interval = 16;
        // The interval at which we reset the shared key.
        // This is a trade-off between space and speed.
        uint32_t restart_interval = default_restart_interval;
    };

    builder();
    explicit builder(options opts);
    builder(const builder&) = delete;
    builder& operator=(const builder&) = delete;
    builder(builder&&) = default;
    builder& operator=(builder&&) = default;
    ~builder() = default;

    // Adds a key-value pair to the block.
    // REQUIRES: key must be lexicographically greater than the last key
    void add(internal::key key, iobuf&& value);

    // Returns an estimate of the current (uncompressed) size of the block
    // we are building.
    size_t current_size_estimate() const;

    // If the builder is empty or not.
    bool empty() const { return _buf.empty(); }

    // Finish building the block and return the iobuf that is the constructed
    // block.
    //
    // After this method is called, the builder can be used to build a new
    // block.
    iobuf finish();

private:
    uint32_t restart_interval() const;

    internal::key _last_key;
    iobuf _buf;
    chunked_vector<uint32_t> _restarts;
    uint32_t _counter = 0; // number of entires since last restart
    uint32_t _restart_interval = 0;
};

} // namespace lsm::block
