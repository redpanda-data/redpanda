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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "lsm/block/contents.h"
#include "lsm/core/internal/keys.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <cstddef>

namespace lsm::block {

// A filter_builder is used to construct filters for a SST.
// It generates a single value of data that is stored as a special block in the
// table.
class filter_builder {
public:
    struct options {
        // The number of bits per key to use in the bloom filter.
        constexpr static uint8_t default_bits_per_key = 10;
        uint8_t bits_per_key = default_bits_per_key;
        // The frequency at which to create a new bloom filter.
        constexpr static size_t default_filter_period = 2_KiB;
        size_t filter_period = default_filter_period;
    };
    explicit filter_builder(options);

    void start_block(size_t block_offset);
    void add_key(internal::key_view key);
    iobuf finish();

private:
    void generate_filter();

    chunked_vector<ss::sstring> _keys;
    chunked_vector<uint32_t> _filter_offsets;
    iobuf _filter;
    uint8_t _bits_per_key;
    uint8_t _filter_base_lg;
    size_t _filter_base;
};

// A reader for a filter block in an SST.
class filter_reader {
public:
    explicit filter_reader(ss::lw_shared_ptr<contents>);

    // Check if it's possible that the user's key exists in the block at this
    // offset within the SST.
    bool key_may_match(uint64_t block_offset, internal::key_view key);

private:
    ss::lw_shared_ptr<contents> _contents;
    size_t _offset; // The offset at which the data ends
    size_t _num;
    uint8_t _base_lg;
};

} // namespace lsm::block
