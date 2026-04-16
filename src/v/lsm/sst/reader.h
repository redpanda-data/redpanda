// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "absl/functional/function_ref.h"
#include "base/seastarx.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/block_cache.h"

#include <seastar/core/future.hh>

namespace lsm::sst {

// A reader for an SST file.
class reader {
    class impl;

public:
    reader(const reader&) = delete;
    reader& operator=(const reader&) = delete;
    reader(reader&&) noexcept;
    reader& operator=(reader&&) noexcept;
    ~reader();

    // Open the table that is stored in bytes [0..file_size) of "file", and read
    // the metadata entries necessary to allow retrieving data from the table.
    static ss::future<reader> open(
      std::unique_ptr<io::random_access_file_reader> file,
      internal::file_id,
      size_t file_size,
      ss::lw_shared_ptr<block_cache> cache);

    // Returns a new iterator over the table contents. The iterator may only be
    // used while the reader is alive, or said another way, the reader must
    // outlive any iterators to it.
    //
    // The result of create_iterator is initially invalid (caller must call one
    // of the seek* methods on the iterator before using it).
    std::unique_ptr<internal::iterator>
    create_iterator(internal::iterator_options opts = {});

    // Calls the function with the key/value pair with the entry found after a
    // call to `create_iterator()->seek(key)`. May not make such a call if the
    // filter policy says that key is not present.
    ss::future<> internal_get(
      internal::key_view key,
      absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn);

    // Closes the reader and the closes the file it's referencing.
    //
    // There must not be any active iterators when this is called.
    //
    // This must be called before destructing.
    ss::future<> close();

private:
    explicit reader(std::unique_ptr<impl>);
    std::unique_ptr<impl> _impl;
};

} // namespace lsm::sst
