// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "lsm/block/contents.h"
#include "lsm/core/internal/keys.h"

#include <seastar/core/future.hh>
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
//
// The filter is read lazily to bound resident memory: a deep-level SST's filter
// is O(GiB), so holding it all (one per open reader) doesn't scale. open()
// retains only the per-region offset array and a per-region CRC array (together
// ~0.4% of the file); each key_may_match reads just the covering ~filter_period
// region from the file on demand.
//
// open() streams the whole block once to validate its CRC, and in that same
// pass computes a CRC over each region. The per-region CRCs are retained and
// checked on every lazy read, so a bit that rots on the (cache) disk after open
// is caught rather than silently turned into a bloom false negative -- the
// integrity of a lazily-read region is verified against a checksum established
// over bytes the whole-block CRC just proved intact.
class filter_reader {
public:
    /// Open a filter over the filter block content at
    /// [content_offset, content_offset + content_size) in `file`, immediately
    /// followed by the block trailer (a compression-type byte and the
    /// whole-block CRC that open() validates). Retains the region offset array
    /// and a per-region CRC array. `file` must outlive the returned reader.
    static ss::future<filter_reader> open(
      io::random_access_file_reader* file,
      uint64_t content_offset,
      uint64_t content_size);

    // Check if it's possible that the user's key exists in the block at this
    // offset within the SST. Reads the covering filter region from the file and
    // verifies it against the region CRC retained at open; throws
    // corruption_exception on a mismatch.
    ss::future<bool>
    key_may_match(uint64_t block_offset, internal::key_view key);

private:
    filter_reader(
      io::random_access_file_reader* file,
      uint64_t content_offset,
      contents tail,
      uint32_t offsets_start,
      size_t num,
      uint8_t base_lg,
      chunked_vector<uint32_t> region_crcs);

    io::random_access_file_reader* _file;
    // File offset of the start of the filter block content.
    uint64_t _content_offset;
    // The retained tail of the filter block: the per-region offset array
    // followed by the (offsets_start, base_lg) trailer. Region byte ranges
    // (positions within the content) are read from here.
    contents _tail;
    uint32_t _offsets_start; // Content position where the offset array begins.
    size_t _num;             // Number of filter regions.
    uint8_t _base_lg;
    // CRC32C of each region's bytes, computed at open over the validated block.
    // Indexed by the same region ordinal as the offset array.
    chunked_vector<uint32_t> _region_crcs;
};

} // namespace lsm::block
