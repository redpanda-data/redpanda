// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "container/chunked_vector.h"
#include "lsm/core/internal/iterator.h"

#include <memory>

namespace lsm::internal {

// Return an iterator that provides the union of data in `children`.
//
// The resulting iterator does not supress duplicates. I.e., if a particular key
// is present in K child iterators, it will be yielded K times.
std::unique_ptr<iterator>
create_merging_iterator(chunked_vector<std::unique_ptr<iterator>> children);

} // namespace lsm::internal
