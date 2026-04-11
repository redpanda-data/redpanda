// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "base/seastarx.h"
#include "lsm/block/builder.h"
#include "lsm/block/filter.h"
#include "lsm/block/handle.h"
#include "lsm/core/compression.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/io/persistence.h"

namespace lsm::sst {

// builder provides the interface used to build a Sorted String Table (SST),
// which is an immutable and sorted map from keys to values.
class builder {
public:
    struct options {
        size_t block_size = internal::options::default_sst_block_size;
        size_t filter_period = internal::options::default_sst_filter_period;
        compression_type compression = compression_type::none;
    };

    // Construct a new builder that will write to the given file.
    builder(std::unique_ptr<io::sequential_file_writer>, options);

    // Add key, value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    ss::future<> add(internal::key key, iobuf value);

    // Finish building the table. Stops using the file passed to the
    // constructor after this function returns.
    ss::future<> finish();

    // Close the builder. This menthod must be called after any exceptions or
    // after finish is called in the case of a successful table build.
    ss::future<> close();

    // The number of calls to `add` so far.
    size_t num_entries() const;
    // Size of the file generated so far.
    //
    // If invoked after `finish`, returns the size of the final generated file.
    size_t file_size() const;

private:
    ss::future<block::handle> write_raw_block(iobuf, compression_type);
    ss::future<> flush();

    size_t _added_entries = 0;
    size_t _written_bytes = 0;
    block::builder _data_block;
    block::builder _index_block;
    block::handle _pending_handle;
    internal::key _last_key;
    std::unique_ptr<io::sequential_file_writer> _writer;
    options _opts;
    std::optional<block::filter_builder> _filter;
    bool _pending_index_entry = false;
};

} // namespace lsm::sst
