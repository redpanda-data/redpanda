// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "bytes/iobuf.h"
#include "lsm/core/internal/iterator.h"

#include <seastar/util/noncopyable_function.hh>

namespace lsm::internal {

// A function to convert an index_iter value into an iterator over the contents
// of the corresponding user data.
using data_iterator_function = ss::noncopyable_function<
  ss::future<std::unique_ptr<internal::iterator>>(iobuf index_value)>;

// Return a new two level iterator. A two-level iterator contains an index
// iterator whose values point to a sequence of entries where each entry is
// itself a sequence of key,value pairs.
//
// The returned iterator yields the concatenation of all key/value pairs in the
// sequence of data iterators.
std::unique_ptr<internal::iterator> create_two_level_iterator(
  std::unique_ptr<internal::iterator> index_iter,
  data_iterator_function data_iter_fn);

} // namespace lsm::internal
